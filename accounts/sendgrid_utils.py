import logging
import os
from pathlib import Path

from django.conf import settings
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from .email_log import EmailLog

logger = logging.getLogger(__name__)

ENV_PATH = Path("/home/ubuntu/peds_edu_app/.env")


def _read_env_var(name: str, default: str = "") -> str:
    """
    Read from process env first; if missing, fallback to parsing /home/ubuntu/peds_edu_app/.env.
    This bypasses systemd EnvironmentFile confusion.
    """
    val = (os.getenv(name) or "").strip()
    if val:
        return val

    if not ENV_PATH.exists():
        return default

    try:
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            if k != name:
                continue
            v = v.strip().strip('"').strip("'")
            return v
    except Exception:
        logger.exception("Failed reading .env at %s", ENV_PATH)

    return default


def send_email_via_sendgrid(to_email: str, subject: str, text: str) -> bool:
    # Prefer Django settings, but fall back to reading .env explicitly
    api_key = (getattr(settings, "SENDGRID_API_KEY", "") or "").strip()
    from_email = (getattr(settings, "SENDGRID_FROM_EMAIL", "") or "").strip()

    if not api_key:
        api_key = _read_env_var("SENDGRID_API_KEY", "")
    if not from_email:
        from_email = _read_env_var("SENDGRID_FROM_EMAIL", "")

    api_key = (api_key or "").strip()
    from_email = (from_email or "").strip()

    key_fingerprint = f"len={len(api_key)} tail={api_key[-6:] if api_key else 'EMPTY'}"

    if not api_key or not from_email:
        EmailLog.objects.create(
            to_email=to_email,
            subject=subject,
            provider="sendgrid",
            success=False,
            status_code=None,
            response_body="",
            error=f"Missing SENDGRID_API_KEY or SENDGRID_FROM_EMAIL | {key_fingerprint} from={from_email}",
        )
        return False

    try:
        message = Mail(
            from_email=from_email,
            to_emails=to_email,
            subject=subject,
            plain_text_content=text,
        )
        sg = SendGridAPIClient(api_key)
        resp = sg.send(message)

        body = ""
        try:
            body = (resp.body or b"").decode("utf-8", errors="ignore")
        except Exception:
            body = str(resp.body)

        ok = (resp.status_code == 202)

        EmailLog.objects.create(
            to_email=to_email,
            subject=subject,
            provider="sendgrid",
            success=ok,
            status_code=resp.status_code,
            response_body=body,
            error="" if ok else f"SendGrid non-202 | {key_fingerprint} from={from_email}",
        )
        return ok

    except Exception as e:
        EmailLog.objects.create(
            to_email=to_email,
            subject=subject,
            provider="sendgrid",
            success=False,
            status_code=None,
            response_body="",
            error=f"{str(e)} | {key_fingerprint} from={from_email}",
        )
        logger.exception("SendGrid send failed")
        return False
