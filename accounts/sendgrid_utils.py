from __future__ import annotations

import hashlib
import html as html_lib
import json
import os
import smtplib
import socket
import ssl
from dataclasses import dataclass
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
                    "SENDGRID_KEY",
                    "sendgrid_key",
                ):
                    v = obj.get(k)
                    if isinstance(v, str) and v.strip():
                        return _sanitize_secret(v)
        except Exception:
            pass

    return raw


def _fingerprint(secret: str) -> str:
    secret = secret or ""
    if not secret:
        return "missing"
    h = hashlib.sha256(secret.encode("utf-8")).hexdigest()[:12]
    return f"len={len(secret)} sha256_12={h}"


def _aws_region() -> str:
    return (
        os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or getattr(settings, "AWS_REGION", None)
        or "ap-south-1"
    )


def _aws_secret_name() -> str:
    return (
        os.getenv("SENDGRID_SECRET_NAME")
        or getattr(settings, "SENDGRID_SECRET_NAME", None)
        or "SendGrid_API"
    )


def _get_secret_string_uncached(secret_name: str, region_name: str) -> str:
    """
    Fetch secret from AWS Secrets Manager without using functools.lru_cache.

    peds_edu.aws_secrets.get_secret_string is lru_cached in this codebase. That is fine
    for most settings, but for debugging and key-rotation we want the *current* value.
    """
    try:
        wrapped = getattr(get_secret_string, "__wrapped__", None)
        if wrapped is not None:
            val = wrapped(secret_name, region_name=region_name)  # type: ignore[misc]
            return (val or "").strip()
    except Exception:
        pass

    # Fallback: cached function (best effort)
    return (get_secret_string(secret_name, region_name=region_name) or "").strip()


@dataclass(frozen=True)
class _KeyCandidate:
    source: str
    key: str

    @property
    def fp(self) -> str:
        return _fingerprint(self.key)


def _iter_sendgrid_api_key_candidates() -> list[_KeyCandidate]:
    """
    Return de-duplicated candidate SendGrid API keys from multiple sources.

    IMPORTANT: We try AWS Secrets Manager *first*, because your production fix relies on
    Secrets Manager and because env/.env values can easily be stale or incorrect.
    """
    region = _aws_region()
    secret_name = _aws_secret_name()

    candidates_raw: list[tuple[str, str]] = []

    # 0) AWS Secrets Manager first (fresh read)
    secret_raw = _get_secret_string_uncached(secret_name, region)
    secret_key = _extract_sendgrid_key(secret_raw)
    if secret_key:
        candidates_raw.append((f"aws_secrets:{secret_name}@{region}", secret_key))

    # 1) Django settings
    candidates_raw.append(("settings.SENDGRID_API_KEY", str(getattr(settings, "SENDGRID_API_KEY", "") or "")))
    candidates_raw.append(("settings.EMAIL_HOST_PASSWORD", str(getattr(settings, "EMAIL_HOST_PASSWORD", "") or "")))

    # 2) Process env (including .env loaded by settings.py)
    candidates_raw.append(("env:SENDGRID_API_KEY", os.getenv("SENDGRID_API_KEY", "") or ""))
    candidates_raw.append(("env:EMAIL_HOST_PASSWORD", os.getenv("EMAIL_HOST_PASSWORD", "") or ""))

    # Normalize, extract JSON-wrapped secrets, sanitize, dedupe by fingerprint
    out: list[_KeyCandidate] = []
    seen_fp: set[str] = set()

    for src, raw in candidates_raw:
        key = _extract_sendgrid_key(raw)
        if not key:
            continue
        fp = _fingerprint(key)
        if fp in seen_fp:
            continue
        seen_fp.add(fp)
        out.append(_KeyCandidate(source=src, key=key))

    return out


def _resolve_from_email(from_email: Optional[str] = None) -> str:
    # Prefer explicit parameter
    if from_email and str(from_email).strip():
        return str(from_email).strip()

    # Settings fallbacks
    v = getattr(settings, "SENDGRID_FROM_EMAIL", None) or getattr(settings, "DEFAULT_FROM_EMAIL", None)
    if v and str(v).strip():
        return str(v).strip()

    # Sensible default
    return "no-reply@example.com"


def _get_backend_mode() -> str:
    """
    Determines provider ordering:
      - 'sendgrid' => SendGrid Web API first, then SMTP fallback
      - 'smtp'     => SMTP first, then SendGrid Web API fallback
      - 'console'  => prints to stdout only (useful for local)
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
        if "console" in v:
            return "console"
        if "sendgrid" in v:
            return "sendgrid"
        if "smtp" in v:
            return "smtp"

    # Default: prefer SendGrid Web API if we can resolve a key and the library is present.
    if SendGridAPIClient is not None and Mail is not None and _iter_sendgrid_api_key_candidates():
        return "sendgrid"

    return "smtp"


def _probe_tcp(host: str, port: int, timeout: float = 3.0) -> str:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return "tcp_ok"
    except Exception as e:
        return f"tcp_fail:{type(e).__name__}"


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
            subject=subject,
            provider=provider,
            success=bool(success),
            status_code=status_code,
            response_body=_truncate(response_body or ""),
            error=_truncate(error or "", limit=8000),
        )
    except Exception:
        # Never crash due to logging failures
        return


# ---------------------------------------------------------------------
# Provider implementations
# ---------------------------------------------------------------------


def _send_via_sendgrid_api(
    *,
    subject: str,
    to_emails: list[str],
    plain_text: str,
    from_email: str,
) -> Tuple[bool, Optional[int], str, str]:
    """
    Returns: (ok, status_code, response_body, error)

    Behavior:
      - Tries AWS Secrets Manager key first (fresh read), then settings/env fallbacks.
      - If a key returns 401/403, it automatically retries with the next candidate.
    """
    key_candidates = _iter_sendgrid_api_key_candidates()

    diag_base = {
        "provider": "sendgrid",
        "from_email": from_email,
        "to_count": len(to_emails),
        "secret_name": _aws_secret_name(),
        "region": _aws_region(),
        "candidates": [{"source": c.source, "fp": c.fp} for c in key_candidates],
    }

    if not key_candidates:
        return False, None, json.dumps(diag_base), "SENDGRID_API_KEY missing (no candidates found)"

    if SendGridAPIClient is None or Mail is None:
        return False, None, json.dumps(diag_base), "sendgrid python package not available at runtime"

    # Simple HTML alternative improves readability in many inboxes
    safe_html = "<pre>" + html_lib.escape(plain_text or "") + "</pre>"

    last_status: Optional[int] = None
    last_err_text: str = ""
    last_err_body: str = ""

    for cand in key_candidates:
        api_key = cand.key
        api_fp = cand.fp

        try:
            message = Mail(
                from_email=from_email,
                to_emails=to_emails,
                subject=subject,
                html_content=safe_html,
            )
            sg = SendGridAPIClient(api_key)
            resp = sg.send(message)

            status = getattr(resp, "status_code", None)
            diag = dict(diag_base)
            diag.update(
                {
                    "selected_source": cand.source,
                    "sendgrid_api_key_fp": api_fp,
                    "status_code": status,
                }
            )
            return (200 <= int(status) < 300), int(status), json.dumps(diag), ""

        except Exception as e:
            # SendGrid python client usually raises urllib.error.HTTPError, but keep generic.
            status = getattr(e, "status_code", None) or getattr(e, "code", None)
            last_status = int(status) if isinstance(status, int) else None

            # Best-effort body extraction
            err_body = ""
            try:
                body = getattr(e, "body", None)
                if body:
                    if isinstance(body, (bytes, bytearray)):
                        err_body = body.decode("utf-8", errors="ignore")
                    else:
                        err_body = str(body)
                elif hasattr(e, "read"):
                    raw = e.read()  # type: ignore[attr-defined]
                    if isinstance(raw, (bytes, bytearray)):
                        err_body = raw.decode("utf-8", errors="ignore")
                    else:
                        err_body = str(raw)
            except Exception:
                err_body = ""

            last_err_text = str(e)
            last_err_body = err_body

            # If unauthorized/forbidden, try next candidate (common when .env is stale).
            if last_status in (401, 403):
                continue

            # Other errors: still allow retry with other candidate (network flake), but keep going.
            continue

    diag = dict(diag_base)
    diag.update(
        {
            "selected_source": None,
            "last_status": last_status,
            "last_error": _truncate(last_err_text, 2000),
        }
    )
    combined = json.dumps(diag)
    if last_err_body:
        combined += "\n" + _truncate(last_err_body, 12000)

    return False, last_status, combined, last_err_text


def _send_via_smtp(
    *,
    subject: str,
    to_emails: list[str],
    plain_text: str,
    from_email: str,
) -> Tuple[bool, Optional[int], str, str]:
    """
    Returns: (ok, status_code, response_body, error)

    SMTP is kept as a fallback. In many AWS environments, outbound SMTP can be flaky or blocked.
    """
    host = str(getattr(settings, "EMAIL_HOST", "") or os.getenv("EMAIL_HOST", "") or "smtp.sendgrid.net").strip()
    port = int(getattr(settings, "EMAIL_PORT", None) or os.getenv("EMAIL_PORT", 587))
    use_tls = bool(getattr(settings, "EMAIL_USE_TLS", None) if hasattr(settings, "EMAIL_USE_TLS") else False)
    use_ssl = bool(getattr(settings, "EMAIL_USE_SSL", None) if hasattr(settings, "EMAIL_USE_SSL") else False)
    if os.getenv("EMAIL_USE_TLS") is not None:
        use_tls = os.getenv("EMAIL_USE_TLS", "1") == "1"
    if os.getenv("EMAIL_USE_SSL") is not None:
        use_ssl = os.getenv("EMAIL_USE_SSL", "0") == "1"

    user = str(getattr(settings, "EMAIL_HOST_USER", "") or os.getenv("EMAIL_HOST_USER", "") or "apikey").strip()

    # Build password candidates: explicit EMAIL_HOST_PASSWORD first, then SendGrid API key candidates.
    pw_candidates_raw: list[tuple[str, str]] = [
        ("settings.EMAIL_HOST_PASSWORD", str(getattr(settings, "EMAIL_HOST_PASSWORD", "") or "")),
        ("env:EMAIL_HOST_PASSWORD", os.getenv("EMAIL_HOST_PASSWORD", "") or ""),
    ]
    for c in _iter_sendgrid_api_key_candidates():
        pw_candidates_raw.append((f"derived_from:{c.source}", c.key))

    pw_candidates: list[_KeyCandidate] = []
    seen_fp: set[str] = set()
    for src, raw in pw_candidates_raw:
        pw = _extract_sendgrid_key(raw)
        if not pw:
            continue
        fp = _fingerprint(pw)
        if fp in seen_fp:
            continue
        seen_fp.add(fp)
        pw_candidates.append(_KeyCandidate(source=src, key=pw))

    probe = _probe_tcp(host, port)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    msg.set_content(plain_text or "")

    if not pw_candidates and user:
        diag = {
            "provider": "smtp",
            "host": host,
            "port": port,
            "use_tls": use_tls,
            "use_ssl": use_ssl,
            "probe": probe,
            "smtp_user": user,
            "smtp_password_fp": "missing",
        }
        return False, None, json.dumps(diag), "EMAIL_HOST_PASSWORD missing (no candidates found)"

    last_err: str = ""
    last_diag: dict = {}

    for pw_cand in (pw_candidates or [_KeyCandidate(source="none", key="")]):
        pw = pw_cand.key
        pw_fp = pw_cand.fp

        try:
            if use_ssl:
                server: smtplib.SMTP = smtplib.SMTP_SSL(host=host, port=port, timeout=20)
            else:
                server = smtplib.SMTP(host=host, port=port, timeout=20)

            try:
                server.ehlo()
            except Exception:
                pass

            if use_tls and not use_ssl:
                ctx = ssl.create_default_context()
                server.starttls(context=ctx)
                try:
                    server.ehlo()
                except Exception:
                    pass

            if user and pw:
                server.login(user, pw)

            refused = server.send_message(msg)

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
                "smtp_password_source": pw_cand.source,
                "refused_recipients": list(refused.keys()) if refused else [],
            }

            # SMTP success has no meaningful HTTP-like status code; we use 250 conventionally.
            return ok, (250 if ok else None), json.dumps(diag), ("" if ok else "SMTP refused some recipients")

        except smtplib.SMTPAuthenticationError as e:
            # Try next password candidate
            last_err = str(e)
            last_diag = {
                "provider": "smtp",
                "host": host,
                "port": port,
                "use_tls": use_tls,
                "use_ssl": use_ssl,
                "probe": probe,
                "smtp_user": user,
                "smtp_password_fp": pw_fp,
                "smtp_password_source": pw_cand.source,
            }
            continue
        except Exception as e:
            # Connection/TLS errors are unlikely to be fixed by trying a different password.
            last_err = str(e)
            last_diag = {
                "provider": "smtp",
                "host": host,
                "port": port,
                "use_tls": use_tls,
                "use_ssl": use_ssl,
                "probe": probe,
                "smtp_user": user,
                "smtp_password_fp": pw_fp,
                "smtp_password_source": pw_cand.source,
            }
            break

    return False, None, json.dumps(last_diag or {"provider": "smtp"}), last_err or "SMTP send failed"


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


def send_email_via_sendgrid(
    *,
    subject: str,
    to_emails: Iterable[str],
    plain_text_content: str,
    from_email: Optional[str] = None,
) -> bool:
    """
    Unified email sending entrypoint used by the app.

    - Logs to accounts_emaillog for every attempted provider.
    - Uses SendGrid Web API and/or SMTP based on EMAIL_BACKEND_MODE.
    - Always falls back to the other provider if the first fails.
    """
    recipients = [str(e).strip() for e in (to_emails or []) if str(e).strip()]
    if not recipients:
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
