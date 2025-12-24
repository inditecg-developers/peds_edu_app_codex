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
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from django.apps import apps
from django.conf import settings

from peds_edu.aws_secrets import get_last_error, get_secret_string

SENDGRID_API_URL = "https://api.sendgrid.com/v3/mail/send"


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
    s = (s or "").strip()
    if not s:
        return ""
    if s.lower().startswith("bearer "):
        s = s[7:].strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s.strip()


def _extract_sendgrid_key(raw: str) -> str:
    """
    If your AWS secret is JSON, allow formats like:
      {"SENDGRID_API_KEY": "SG..."}
      {"api_key": "SG..."}
    Otherwise, treat the whole secret as the key.
    """
    raw = (raw or "").strip()
    if not raw:
        return ""

    # Try JSON first
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

    return _sanitize_secret(raw)


def _fingerprint(secret: str) -> str:
    secret = secret or ""
    if not secret:
        return "missing"
    h = hashlib.sha256(secret.encode("utf-8")).hexdigest()[:12]
    return f"len={len(secret)} sha256_12={h}"


def _redacted_tail(secret: str, n: int = 4) -> str:
    """
    Return only the last N characters (default 4), never the full secret.
    For very short strings, returns '<short>'.
    """
    secret = secret or ""
    if len(secret) < max(1, n):
        return "<short>"
    return secret[-n:]


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


def _get_secret_string_uncached(secret_name: str, region_name: str) -> Tuple[str, Optional[str]]:
    """
    Fetch secret from AWS Secrets Manager without relying on functools.lru_cache.

    IMPORTANT: This MUST NOT raise. If AWS credentials are not available, it returns ("", "<error>").

    Note:
      - peds_edu.aws_secrets.get_secret_string is lru_cached.
      - We attempt to call the undecorated implementation via __wrapped__ to bypass caching,
        and then read peds_edu.aws_secrets.get_last_error() for a diagnostic string.
    """
    try:
        wrapped = getattr(get_secret_string, "__wrapped__", None)
        if wrapped is not None:
            val = wrapped(secret_name, region_name=region_name)  # type: ignore[misc]
        else:
            val = get_secret_string(secret_name, region_name=region_name)

        err = (get_last_error() or "").strip()
        return (val or "").strip(), (err or None)
    except Exception as e:
        return "", f"{type(e).__name__}: {e}"


@dataclass(frozen=True)
class _KeyCandidate:
    source: str
    key: str

    @property
    def fp(self) -> str:
        return _fingerprint(self.key)

    @property
    def tail(self) -> str:
        # Tail length can be controlled via env (default 4) for debugging.
        try:
            n = int(os.getenv("SENDGRID_KEY_TAIL_CHARS", "4"))
        except Exception:
            n = 4
        n = max(2, min(12, n))  # keep this bounded
        return _redacted_tail(self.key, n=n)


def _get_sendgrid_api_key_candidates() -> Tuple[list[_KeyCandidate], dict]:
    """
    Return de-duplicated candidate SendGrid API keys from multiple sources, plus diagnostics.

    Ordering:
      0) AWS Secrets Manager (fresh read, best-effort)
      1) Django settings
      2) Process env (including .env loaded by settings.py)

    Never raises.
    """
    region = _aws_region()
    secret_name = _aws_secret_name()

    diag = {
        "secret_name": secret_name,
        "region": region,
        "aws_secret_attempted": True,
        "aws_secret_error": "",
        "aws_secret_value_present": False,  # whether *something* was returned (not necessarily a valid key)
    }

    candidates_raw: list[tuple[str, str]] = []

    # 0) AWS Secrets Manager first (fresh read)
    secret_raw, secret_err = _get_secret_string_uncached(secret_name, region)
    if secret_err:
        diag["aws_secret_error"] = secret_err
    if secret_raw:
        diag["aws_secret_value_present"] = True
    secret_key = _extract_sendgrid_key(secret_raw)
    if secret_key:
        candidates_raw.append((f"aws_secrets:{secret_name}@{region}", secret_key))

    # 1) Django settings
    candidates_raw.append(("settings.SENDGRID_API_KEY", str(getattr(settings, "SENDGRID_API_KEY", "") or "")))
    candidates_raw.append(("settings.EMAIL_HOST_PASSWORD", str(getattr(settings, "EMAIL_HOST_PASSWORD", "") or "")))

    # 2) Process env (including .env loaded by settings.py)
    candidates_raw.append(("env:SENDGRID_API_KEY", os.getenv("SENDGRID_API_KEY", "") or ""))
    candidates_raw.append(("env:EMAIL_HOST_PASSWORD", os.getenv("EMAIL_HOST_PASSWORD", "") or ""))

    # Normalize + de-dupe by fingerprint
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

    return out, diag


def _resolve_from_email(from_email: Optional[str]) -> str:
    if from_email and str(from_email).strip():
        return str(from_email).strip()

    # Settings fallbacks
    v = getattr(settings, "SENDGRID_FROM_EMAIL", None) or getattr(settings, "DEFAULT_FROM_EMAIL", None)
    if v and str(v).strip():
        return str(v).strip()

    # Env fallbacks
    v = os.getenv("SENDGRID_FROM_EMAIL") or os.getenv("DEFAULT_FROM_EMAIL")
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
        if "console" in v:
            return "console"
        if "sendgrid" in v:
            return "sendgrid"
        if "smtp" in v:
            return "smtp"

    # Default: prefer SendGrid Web API if we can resolve a key
    key_candidates, _ = _get_sendgrid_api_key_candidates()
    if key_candidates:
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
      - ALWAYS sets Authorization header: "Bearer <API_KEY>" (explicit, via urllib).
    """
    key_candidates, key_diag = _get_sendgrid_api_key_candidates()

    diag_base = {
        "provider": "sendgrid",
        "from_email": from_email,
        "to_count": len(to_emails),
        "aws_secrets": key_diag,
        "candidates": [{"source": c.source, "fp": c.fp} for c in key_candidates],
        "sendgrid_api_url": SENDGRID_API_URL,
        "authorization_header_set": True,
    }

    if not key_candidates:
        return False, None, json.dumps(diag_base), "SENDGRID_API_KEY missing (no candidates found)"

    safe_html = "<pre>" + html_lib.escape(plain_text or "") + "</pre>"

    last_status: Optional[int] = None
    last_err_text: str = ""
    last_err_body: str = ""

    # Shared body (only API key differs)
    payload = {
        "personalizations": [{"to": [{"email": e} for e in to_emails]}],
        "from": {"email": from_email},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": plain_text or ""},
            {"type": "text/html", "value": safe_html},
        ],
    }
    payload_bytes = json.dumps(payload).encode("utf-8")

    for cand in key_candidates:
        api_key = cand.key
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        try:
            req = Request(SENDGRID_API_URL, data=payload_bytes, headers=headers, method="POST")
            with urlopen(req, timeout=25) as resp:
                status = getattr(resp, "status", None) or resp.getcode()
                try:
                    body = resp.read().decode("utf-8", errors="ignore")
                except Exception:
                    body = ""

            ok = isinstance(status, int) and 200 <= status < 300

            diag = dict(diag_base)
            diag.update(
                {
                    "selected_source": cand.source,
                    "sendgrid_api_key_fp": cand.fp,
                    "sendgrid_api_key_tail": cand.tail,
                    "status_code": status,
                }
            )

            combined = json.dumps(diag)
            if body:
                combined += "\n" + _truncate(body, 12000)

            if ok:
                return True, int(status), combined, ""

            last_status = int(status) if isinstance(status, int) else None
            last_err_text = f"HTTP {status}"
            last_err_body = body

            # Wrong key / revoked key -> try next candidate
            if status in (401, 403):
                continue

            # Other errors are unlikely to be fixed by switching keys, but we still allow fallback.
            break

        except HTTPError as e:
            status = getattr(e, "code", None)
            try:
                body = e.read().decode("utf-8", errors="ignore")  # type: ignore[attr-defined]
            except Exception:
                body = ""

            last_status = int(status) if isinstance(status, int) else None
            last_err_text = f"HTTPError {status}: {getattr(e, 'reason', '')}".strip()
            last_err_body = body

            # Wrong key / revoked key -> try next candidate
            if status in (401, 403):
                continue

            break

        except URLError as e:
            last_status = None
            last_err_text = f"URLError: {e}"
            last_err_body = ""
            break

        except Exception as e:
            last_status = None
            last_err_text = f"{type(e).__name__}: {e}"
            last_err_body = ""
            break

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

    return False, last_status, combined, last_err_text or "SendGrid API send failed"


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

    # Pull SendGrid API key candidates (these can also be used as SMTP password)
    try:
        key_candidates, key_diag = _get_sendgrid_api_key_candidates()
        for c in key_candidates:
            pw_candidates_raw.append((f"derived_from:{c.source}", c.key))
        aws_diag = key_diag
    except Exception as e:
        key_candidates = []
        aws_diag = {
            "secret_name": _aws_secret_name(),
            "region": _aws_region(),
            "aws_secret_attempted": True,
            "aws_secret_error": f"{type(e).__name__}: {e}",
            "aws_secret_value_present": False,
        }

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
            "aws_secrets": aws_diag,
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
                if use_tls and not use_ssl:
                    ctx = ssl.create_default_context()
                    server.starttls(context=ctx)
                    server.ehlo()

                if user and pw:
                    server.login(user, pw)

                server.send_message(msg)
                try:
                    server.quit()
                except Exception:
                    pass

                diag = {
                    "provider": "smtp",
                    "host": host,
                    "port": port,
                    "use_tls": use_tls,
                    "use_ssl": use_ssl,
                    "probe": probe,
                    "smtp_user": user,
                    "smtp_password_fp": pw_fp,
                    "smtp_password_tail": pw_cand.tail,
                    "smtp_password_source": pw_cand.source,
                    "aws_secrets": aws_diag,
                }
                return True, 250, json.dumps(diag), ""
            finally:
                try:
                    server.close()
                except Exception:
                    pass

        except smtplib.SMTPAuthenticationError as e:
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
                "smtp_password_tail": pw_cand.tail,
                "smtp_password_source": pw_cand.source,
                "aws_secrets": aws_diag,
                "error_class": "SMTPAuthenticationError",
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
                "smtp_password_tail": pw_cand.tail,
                "smtp_password_source": pw_cand.source,
                "aws_secrets": aws_diag,
                "error_class": type(e).__name__,
            }
            break

    return False, None, json.dumps(last_diag or {"provider": "smtp"}), last_err or "SMTP send failed"


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
    Never raises.
    """
    subject = (subject or "").strip()
    recipients = [str(e).strip() for e in (to_emails or []) if e and str(e).strip()]
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
        try:
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
        except Exception as e:
            ok = False
            status = None
            resp_body = ""
            err = f"provider_crash:{provider}:{type(e).__name__}: {e}"

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
