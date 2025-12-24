from __future__ import annotations

import hashlib
import html as html_lib
import json
import os
import smtplib
import socket
from email.message import EmailMessage
from typing import Iterable, Optional, Tuple

from django.apps import apps
from django.conf import settings

from peds_edu.aws_secrets import get_secret_string

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
except Exception:
    SendGridAPIClient = None  # type: ignore
    Mail = None  # type: ignore


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _truncate(s: str, limit: int = 12000) -> str:
    s = s or ""
    if len(s) <= limit:
        return s
    return s[:limit] + f"\n... (truncated; len={len(s)})"


def _sanitize_secret(s: str) -> str:
    """
    Remove accidental prefixes/quotes/whitespace:
      - 'Bearer <key>'
      - surrounding quotes
      - trailing newlines/spaces
    """
    s = (s or "").strip().strip('"').strip("'")
    if s.lower().startswith("bearer "):
        parts = s.split(None, 1)
        s = parts[1].strip() if len(parts) > 1 else ""
    return s.strip()


def _fingerprint(secret: str) -> str:
    """
    Non-sensitive fingerprint for logs (safe to store).
    """
    secret = secret or ""
    if not secret:
        return "missing"
    h = hashlib.sha256(secret.encode("utf-8")).hexdigest()[:12]
    return f"len={len(secret)} sha256_12={h}"


def _extract_sendgrid_key(raw: str) -> str:
    """
    Supports secrets stored as:
      - Plain string: "SG...."
      - JSON: {"SENDGRID_API_KEY":"SG...."} or {"api_key":"SG...."} etc.
    """
    raw = _sanitize_secret(raw)
    if not raw:
        return ""

    # JSON secret support
    if raw.startswith("{") and raw.endswith("}"):
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                for k in (
                    "SENDGRID_API_KEY",
                    "sendgrid_api_key",
                    "api_key",
                    "apikey",
                    "key",
                    "SENDGRID_API",
                ):
                    v = obj.get(k)
                    if isinstance(v, str) and v.strip():
                        return _sanitize_secret(v)

                # fallback: first non-empty string value
                for v in obj.values():
                    if isinstance(v, str) and v.strip():
                        return _sanitize_secret(v)
        except Exception:
            pass

    return raw


def _aws_region() -> str:
    return (
        os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or getattr(settings, "AWS_REGION", "")
        or getattr(settings, "AWS_DEFAULT_REGION", "")
        or "ap-south-1"
    )


def _aws_sendgrid_api_key() -> str:
    """
    Reads SendGrid key from AWS Secrets Manager.

    Default secret name: SendGrid_API
    Override via:
      - env SENDGRID_SECRET_NAME
      - settings.SENDGRID_SECRET_NAME
    """
    secret_name = (
        os.getenv("SENDGRID_SECRET_NAME")
        or getattr(settings, "SENDGRID_SECRET_NAME", "")
        or "SendGrid_API"
    )

    try:
        raw = get_secret_string(secret_name, region_name=_aws_region()) or ""
    except Exception:
        raw = ""

    return _extract_sendgrid_key(raw)


def _get_backend_mode() -> str:
    """
    Accepts multiple naming conventions to avoid env drift:
      - settings.EMAIL_BACKEND_MODE or env EMAIL_BACKEND_MODE (preferred)
      - settings.EMAIL_BACKEND or env EMAIL_BACKEND (legacy)
    """
    candidates = [
        getattr(settings, "EMAIL_BACKEND_MODE", None),
        os.getenv("EMAIL_BACKEND_MODE"),
        getattr(settings, "EMAIL_BACKEND", None),
        os.getenv("EMAIL_BACKEND"),
    ]

    for cand in candidates:
        if not cand:
            continue
        v = str(cand).strip().lower()

        # Django backend path support
        if "sendgrid" in v:
            return "sendgrid"
        if "smtp" in v:
            return "smtp"
        if "console" in v or "locmem" in v:
            return "console"

        if v in ("sendgrid", "smtp", "console"):
            return v

    return "smtp"


def _resolve_sendgrid_api_key() -> str:
    """
    Best-effort resolution of SendGrid API key.
    """
    # 1) settings
    key = _sanitize_secret(getattr(settings, "SENDGRID_API_KEY", "") or "")
    if key:
        return key

    # 2) env
    key = _sanitize_secret(os.getenv("SENDGRID_API_KEY", "") or "")
    if key:
        return key

    # 3) also allow EMAIL_HOST_PASSWORD to carry the API key
    key = _sanitize_secret(getattr(settings, "EMAIL_HOST_PASSWORD", "") or "")
    if key:
        return key
    key = _sanitize_secret(os.getenv("EMAIL_HOST_PASSWORD", "") or "")
    if key:
        return key

    # 4) AWS Secrets Manager
    return _aws_sendgrid_api_key()


def _resolve_from_email(explicit: Optional[str] = None) -> str:
    return (
        (explicit or "").strip()
        or (getattr(settings, "SENDGRID_FROM_EMAIL", "") or "").strip()
        or (getattr(settings, "DEFAULT_FROM_EMAIL", "") or "").strip()
        or (os.getenv("SENDGRID_FROM_EMAIL", "") or "").strip()
        or "no-reply@example.com"
    )


def _probe_tcp(host: str, port: int, timeout: int = 4) -> str:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return "tcp_ok"
    except Exception as e:
        return f"tcp_fail:{type(e).__name__}:{str(e)[:180]}"


def _log_email_attempt(
    *,
    to_email: str,
    subject: str,
    provider: str,
    success: bool,
    status_code: Optional[int] = None,
    response_body: str = "",
    error: str = "",
) -> None:
    """
    Writes to accounts_emaillog (EmailLog model) if available.
    Never raises (email sending must not crash user flows).
    """
    try:
        EmailLog = apps.get_model("accounts", "EmailLog")
    except Exception:
        EmailLog = None

    if EmailLog is None:
        return

    try:
        EmailLog.objects.create(
            to_email=to_email,
            subject=subject[:255],
            provider=(provider or "")[:50],
            success=bool(success),
            status_code=status_code,
            response_body=_truncate(response_body or ""),
            error=_truncate(error or ""),
        )
    except Exception:
        # Do not raise; logging must never break functional flows.
        return


def _send_via_sendgrid_api(
    *,
    subject: str,
    to_emails: list[str],
    plain_text: str,
    from_email: str,
) -> Tuple[bool, Optional[int], str, str]:
    """
    Returns: (ok, status_code, response_body, error)
    """
    api_key = _resolve_sendgrid_api_key()
    api_fp = _fingerprint(api_key)

    if not api_key:
        return False, None, f"sendgrid_api_key={api_fp}", "SENDGRID_API_KEY missing"

    if SendGridAPIClient is None or Mail is None:
        return (
            False,
            None,
            f"sendgrid_api_key={api_fp}",
            "sendgrid python package not available at runtime",
        )

    # Simple HTML alternative improves readability in many inboxes
    safe_html = "<pre>" + html_lib.escape(plain_text or "") + "</pre>"

    try:
        message = Mail(
            from_email=from_email,
            to_emails=to_emails,
            subject=subject,
            plain_text_content=plain_text or "",
            html_content=safe_html,
        )

        sg = SendGridAPIClient(api_key)
        resp = sg.send(message)

        body = ""
        try:
            if resp.body is None:
                body = ""
            elif isinstance(resp.body, (bytes, bytearray)):
                body = resp.body.decode("utf-8", errors="ignore")
            else:
                body = str(resp.body)
        except Exception:
            body = ""

        ok = 200 <= int(resp.status_code) < 300

        diag = {
            "provider": "sendgrid",
            "status": int(resp.status_code),
            "from_email": from_email,
            "to_count": len(to_emails),
            "sendgrid_api_key_fp": api_fp,
        }

        return ok, int(resp.status_code), json.dumps(diag) + ("\n" + body if body else ""), ""
    except Exception as e:
        status = int(getattr(e, "status_code", 0) or getattr(e, "code", 0) or 0) or None

        err_body = ""
        try:
            raw_body = getattr(e, "body", None)
            if raw_body is not None:
                if isinstance(raw_body, (bytes, bytearray)):
                    err_body = raw_body.decode("utf-8", errors="ignore")
                else:
                    err_body = str(raw_body)
        except Exception:
            err_body = ""

        diag = {
            "provider": "sendgrid",
            "from_email": from_email,
            "to_count": len(to_emails),
            "sendgrid_api_key_fp": api_fp,
        }

        err_text = str(e)
        combined = json.dumps(diag)
        if err_body:
            combined += "\n" + err_body

        return False, status, combined, err_text


def _send_via_smtp(
    *,
    subject: str,
    to_emails: list[str],
    plain_text: str,
    from_email: str,
) -> Tuple[bool, Optional[int], str, str]:
    """
    Returns: (ok, status_code, response_body, error)
    """
    host = (getattr(settings, "EMAIL_HOST", "") or os.getenv("EMAIL_HOST", "") or "").strip()
    host = host or "smtp.sendgrid.net"

    port = int(getattr(settings, "EMAIL_PORT", 587) or 587)
    use_tls = bool(getattr(settings, "EMAIL_USE_TLS", True))
    use_ssl = bool(getattr(settings, "EMAIL_USE_SSL", False))

    user = (getattr(settings, "EMAIL_HOST_USER", "") or os.getenv("EMAIL_HOST_USER", "") or "").strip()
    user = user or "apikey"

    password = _sanitize_secret(getattr(settings, "EMAIL_HOST_PASSWORD", "") or os.getenv("EMAIL_HOST_PASSWORD", "") or "")
    if not password:
        # allow SendGrid API key as SMTP password as well
        password = _resolve_sendgrid_api_key()

    pw_fp = _fingerprint(password)

    if not password:
        diag = {
            "provider": "smtp",
            "host": host,
            "port": port,
            "use_tls": use_tls,
            "use_ssl": use_ssl,
            "smtp_password_fp": pw_fp,
        }
        return False, None, json.dumps(diag), "SendGrid SMTP password/API key missing"

    probe = _probe_tcp(host, port)

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.set_content(plain_text or "")

    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host=host, port=port, timeout=20)
        else:
            server = smtplib.SMTP(host=host, port=port, timeout=20)

        try:
            server.ehlo()
            if use_tls and not use_ssl:
                server.starttls()
                server.ehlo()

            server.login(user, password)
            refused = server.send_message(msg) or {}
        finally:
            try:
                server.quit()
            except Exception:
                try:
                    server.close()
                except Exception:
                    pass

        ok = len(refused) == 0

        diag = {
            "provider": "smtp",
            "host": host,
            "port": port,
            "use_tls": use_tls,
            "use_ssl": use_ssl,
            "probe": probe,
            "smtp_user": user,
            "smtp_password_fp": pw_fp,
            "refused_recipients": list(refused.keys()) if refused else [],
        }

        # SMTP success has no meaningful HTTP-like status code; we use 250 conventionally.
        return ok, (250 if ok else None), json.dumps(diag), ("" if ok else "SMTP refused some recipients")
    except Exception as e:
        diag = {
            "provider": "smtp",
            "host": host,
            "port": port,
            "use_tls": use_tls,
            "use_ssl": use_ssl,
            "probe": probe,
            "smtp_user": user,
            "smtp_password_fp": pw_fp,
        }
        return False, None, json.dumps(diag), str(e)


# ---------------------------------------------------------------------
# Public API used by accounts/views.py
# ---------------------------------------------------------------------


def send_email_via_sendgrid(
    subject: str,
    to_emails: Iterable[str],
    plain_text_content: str,
    from_email: Optional[str] = None,
) -> bool:
    """
    Sends email using the configured backend mode, but with fallback:
      - If mode is smtp => try SMTP first, then SendGrid Web API
      - If mode is sendgrid => try SendGrid Web API first, then SMTP
      - If mode is console => print and log as success

    ALWAYS logs each attempt into accounts_emaillog (EmailLog model).
    """
    subject = (subject or "").strip()
    recipients = [e.strip() for e in (to_emails or []) if e and str(e).strip()]
    recipients = list(dict.fromkeys(recipients))  # de-dupe, preserve order

    if not subject or not recipients:
        # Log minimal failure for traceability
        for r in recipients or [""]:
            _log_email_attempt(
                to_email=r or "(missing)",
                subject=subject or "(missing)",
                provider="internal",
                success=False,
                status_code=None,
                response_body="",
                error="Missing subject and/or recipients",
            )
        return False

    mode = _get_backend_mode()
    from_addr = _resolve_from_email(from_email)

    if mode == "console":
        print("=== EMAIL (console mode) ===")
        print("To:", recipients)
        print("Subject:", subject)
        print("Body:\n", plain_text_content or "")
        for r in recipients:
            _log_email_attempt(
                to_email=r,
                subject=subject,
                provider="console",
                success=True,
                status_code=200,
                response_body="Printed to console (EMAIL_BACKEND_MODE=console).",
                error="",
            )
        return True

    # Preferred order based on mode, but ALWAYS fallback to the other provider.
    providers = ["smtp", "sendgrid"] if mode == "smtp" else ["sendgrid", "smtp"]

    last_ok = False

    for provider in providers:
        if provider == "sendgrid":
            ok, status, resp_body, err = _send_via_sendgrid_api(
                subject=subject,
                to_emails=recipients,
                plain_text=plain_text_content or "",
                from_email=from_addr,
            )
        else:
            ok, status, resp_body, err = _send_via_smtp(
                subject=subject,
                to_emails=recipients,
                plain_text=plain_text_content or "",
                from_email=from_addr,
            )

        # Log for each recipient (helps trace specific addresses)
        for r in recipients:
            _log_email_attempt(
                to_email=r,
                subject=subject,
                provider=provider,
                success=ok,
                status_code=status,
                response_body=resp_body,
                error=err,
            )

        if ok:
            last_ok = True
            break

    return last_ok
