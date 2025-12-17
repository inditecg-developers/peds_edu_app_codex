import logging
import os
from pathlib import Path

from django.conf import settings
from django.core.mail import send_mail
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from .email_log import EmailLog

logger = logging.getLogger(__name__)

ENV_PATH = Path("/home/ubuntu/peds_edu_app/.env")


def _read_env_var(name: str, default: str = "") -> str:
    """
    Read from process env first; if missing, fallback to parsing /home/ubuntu/peds_edu_app/.env.
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


def _smtp_enabled() -> bool:
    mode = (os.getenv("EMAIL_BACKEND") or "").strip().lower()
    if mode:
        return mode == "smtp"
    backend = getattr(settings, "EMAIL_BACKEND", "")
    return "smtp" in (backend or "").lower()


def send_email_via_sendgrid(to_email: str, subject: str, text: str) -> bool:
    """
    Sends email:
    - Prefer SMTP if EMAIL_BACKEND=smtp
    - Otherwise attempt SendGrid Web API

    Always logs to EmailLog.
    """
    # ---------- SMTP path ----------
    if _smtp_enabled():
        try:
            send_mail(
                subject=subject,
                message=text,
                from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "") or getattr(settings, "SENDGRID_FROM_EMAIL", ""),
                recipient_list=[to_email],
                fail_silently=False,
            )
            EmailLog.objects.create(
                to_email=to_email,
                subject=subject,
                provider="smtp",
                success=True,
                status_code=202,
                response_body="Sent via SMTP backend",
                error="",
            )
            return True
        except Exception as e:
            EmailLog.objects.create(
                to_email=to_email,
                subject=subject,
                provider="smtp",
                success=False,
                status_code=None,
                response_body="",
                error=(
                    f"{type(e).__name__}: {str(e)} | "
                    f"host={getattr(settings,'EMAIL_HOST','')} "
                    f"port={getattr(settings,'EMAIL_PORT','')} "
                    f"tls={getattr(settings,'EMAIL_USE_TLS','')} "
                    f"ssl={getattr(settings,'EMAIL_USE_SSL','')} "
                    f"user={getattr(settings,'EMAIL_HOST_USER','')}"
                ),
            )
            logger.exception("SMTP send failed")
            return False

    # ---------- SendGrid Web API path ----------
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

        try:
            body = (resp.body or b"").decode("utf-8", errors="ignore")
        except Exception:
            body = str(resp.body)

        ok = resp.status_code == 202

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
        status_code = getattr(e, "status_code", None)

        body = ""
        try:
            raw_body = getattr(e, "body", None)
            if raw_body is not None:
                if isinstance(raw_body, (bytes, bytearray)):
                    body = raw_body.decode("utf-8", errors="ignore")
                else:
                    body = str(raw_body)
        except Exception:
            body = ""

        if not body:
            try:
                resp = getattr(e, "response", None)
                if resp is not None:
                    rb = getattr(resp, "body", None)
                    if isinstance(rb, (bytes, bytearray)):
                        body = rb.decode("utf-8", errors="ignore")
                    elif rb is not None:
                        body = str(rb)
            except Exception:
                pass

        EmailLog.objects.create(
            to_email=to_email,
            subject=subject,
            provider="sendgrid",
            success=False,
            status_code=status_code,
            response_body=body,
            error=f"{str(e)} | {key_fingerprint} from={from_email}",
        )

        logger.exception("SendGrid send failed")
        return False
