from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from typing import Iterable, Optional

from django.conf import settings
from peds_edu.aws_secrets import get_secret_string

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
except Exception:
    SendGridAPIClient = None
    Mail = None


def _sanitize_secret(value: str) -> str:
    """Remove accidental surrounding quotes/spaces from secrets."""
    if value is None:
        return ""
    v = str(value).strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1].strip()
    return v


def _aws_sendgrid_api_key() -> str:
    """Fetch SendGrid API key from AWS Secrets Manager (secret: SendGrid_API)."""
    return (get_secret_string("SendGrid_API", region_name="ap-south-1") or "").strip()


def _read_env_var(key: str) -> str:
    """Fallback env lookup (supports local .env if present)."""
    val = os.environ.get(key, "")
    if val:
        return val

    # Local fallback: read BASE_DIR/.env if present
    base_dir = getattr(settings, "BASE_DIR", None)
    if base_dir:
        env_path = os.path.join(str(base_dir), ".env")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        if k.strip() == key:
                            return v.strip()
            except Exception:
                pass
    return ""


def _smtp_enabled() -> bool:
    mode = _sanitize_secret(getattr(settings, "EMAIL_BACKEND_MODE", "") or "")
    if mode:
        return mode.lower() == "smtp"
    return True


def _smtp_send_raw(
    subject: str,
    to_emails: Iterable[str],
    plain_text_content: str,
    from_email: Optional[str] = None,
) -> bool:
    host = _sanitize_secret(getattr(settings, "EMAIL_HOST", "") or "")
    port = int(getattr(settings, "EMAIL_PORT", 587) or 587)
    use_tls = bool(getattr(settings, "EMAIL_USE_TLS", True))
    user = _sanitize_secret(getattr(settings, "EMAIL_HOST_USER", "") or "")

    # Priority:
    # 1) Django EMAIL_HOST_PASSWORD
    # 2) Django SENDGRID_API_KEY
    # 3) ENV EMAIL_HOST_PASSWORD
    # 4) ENV SENDGRID_API_KEY
    # 5) AWS Secrets Manager SendGrid_API
    sg_key = _sanitize_secret(getattr(settings, "EMAIL_HOST_PASSWORD", "") or "")
    if not sg_key:
        sg_key = _sanitize_secret(getattr(settings, "SENDGRID_API_KEY", "") or "")
    if not sg_key:
        sg_key = _sanitize_secret(_read_env_var("EMAIL_HOST_PASSWORD"))
    if not sg_key:
        sg_key = _sanitize_secret(_read_env_var("SENDGRID_API_KEY"))
    if not sg_key:
        sg_key = _sanitize_secret(_aws_sendgrid_api_key())
    if not sg_key:
        raise RuntimeError("SendGrid SMTP password/API key missing.")

    from_addr = _sanitize_secret(from_email or getattr(settings, "DEFAULT_FROM_EMAIL", "") or "")
    if not from_addr:
        from_addr = "products@inditech.co.in"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join([e for e in to_emails if e])
    msg.set_content(plain_text_content or "")

    try:
        with smtplib.SMTP(host, port, timeout=20) as server:
            if use_tls:
                server.starttls()
            server.login(user or "apikey", sg_key)
            server.send_message(msg)
        return True
    except Exception:
        return False


def _sendgrid_send_raw(
    subject: str,
    to_emails: Iterable[str],
    plain_text_content: str,
    from_email: Optional[str] = None,
) -> bool:
    if SendGridAPIClient is None or Mail is None:
        return False

    api_key = _sanitize_secret(getattr(settings, "SENDGRID_API_KEY", "") or "")
    if not api_key:
        api_key = _sanitize_secret(_read_env_var("SENDGRID_API_KEY"))
    if not api_key:
        api_key = _sanitize_secret(_aws_sendgrid_api_key())
    if not api_key:
        raise RuntimeError("SENDGRID_API_KEY missing.")

    from_addr = _sanitize_secret(from_email or getattr(settings, "SENDGRID_FROM_EMAIL", "") or "")
    if not from_addr:
        from_addr = _sanitize_secret(getattr(settings, "DEFAULT_FROM_EMAIL", "") or "") or "products@inditech.co.in"

    try:
        sg = SendGridAPIClient(api_key)
        message = Mail(
            from_email=from_addr,
            to_emails=list(to_emails),
            subject=subject,
            plain_text_content=plain_text_content or "",
        )
        response = sg.send(message)
        return 200 <= getattr(response, "status_code", 0) < 300
    except Exception:
        return False


def send_email_via_sendgrid(
    subject: str,
    to_emails: Iterable[str],
    plain_text_content: str,
    from_email: Optional[str] = None,
) -> bool:
    """
    Unified email sender:
    - If EMAIL_BACKEND_MODE=smtp: send via SMTP
    - If EMAIL_BACKEND_MODE=sendgrid: send via SendGrid Web API
    """
    mode = _sanitize_secret(getattr(settings, "EMAIL_BACKEND_MODE", "") or "").lower()
    if mode == "sendgrid":
        return _sendgrid_send_raw(subject, to_emails, plain_text_content, from_email=from_email)

    # Default to SMTP
    return _smtp_send_raw(subject, to_emails, plain_text_content, from_email=from_email)
