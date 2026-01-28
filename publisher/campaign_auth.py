# publisher/campaign_auth.py

from __future__ import annotations

from functools import wraps
from typing import Any, Dict, Optional, Sequence
from urllib.parse import urlencode

from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect

import time
import json
import uuid

from accounts import master_db

# Part-C session keys (Project2)
SESSION_KEY = getattr(settings, "SSO_SESSION_KEY_IDENTITY", "sso_identity")
SESSION_CAMPAIGN_KEY = getattr(settings, "SSO_SESSION_KEY_CAMPAIGN", "campaign_id")

# Legacy compatibility (if any old sessions exist)
LEGACY_SESSION_KEY = "publisher_jwt_claims"
LEGACY_CAMPAIGN_KEY = "publisher_current_campaign_id"

SESSION_PUBLISHER_MASTER_VALIDATION = "publisher_master_validation"
PUBLISHER_MASTER_VALIDATION_TTL_SECONDS = getattr(settings, "PUBLISHER_MASTER_VALIDATION_TTL_SECONDS", 300)


def unauthorized_response() -> HttpResponse:
    return HttpResponse("unauthorised access", status=401, content_type="text/plain")


def _debug_enabled(request: HttpRequest) -> bool:
    return (request.GET.get("debug_sso") or "").strip() == "1"


def _normalize_roles(value: Any) -> Sequence[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    return [str(value)]


def _extract_token(request: HttpRequest) -> Optional[str]:
    token = (
        request.GET.get("token")
        or request.GET.get("sso_token")
        or request.GET.get("jwt")
        or request.GET.get("access_token")
        or request.GET.get("jwt_token")     # NEW
        or request.GET.get("id_token")      # NEW (if Project1 uses this)
    )
    if token:
        return token.strip()

    auth = (request.META.get("HTTP_AUTHORIZATION") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def _extract_email_from_claims(ident: Dict[str, Any]) -> str:
    email = (ident.get("email") or "").strip().lower()
    if email and "@" in email:
        return email

    username = (ident.get("username") or "").strip().lower()
    if username and "@" in username:
        return username

    other = (ident.get("publisher_email") or "").strip().lower()
    if other and "@" in other:
        return other

    return ""


def _is_publisher_authorized_in_master(request: HttpRequest, email: str) -> bool:
    if not email:
        return False

    cache = request.session.get(SESSION_PUBLISHER_MASTER_VALIDATION) or {}
    now = int(time.time())

    if (
        isinstance(cache, dict)
        and cache.get("email") == email
        and cache.get("ok") is True
        and isinstance(cache.get("ts"), int)
        and now - cache["ts"] <= int(PUBLISHER_MASTER_VALIDATION_TTL_SECONDS)
    ):
        return True

    ok = master_db.authorized_publisher_exists(email)
    request.session[SESSION_PUBLISHER_MASTER_VALIDATION] = {"email": email, "ok": bool(ok), "ts": now}
    request.session.modified = True
    return bool(ok)


def get_publisher_claims(request: HttpRequest) -> Optional[Dict[str, Any]]:
    ident = request.session.get(SESSION_KEY)
    if isinstance(ident, dict):
        roles = _normalize_roles(ident.get("roles"))
        if "publisher" in [r.lower() for r in roles]:
            email = _extract_email_from_claims(ident)
            if _is_publisher_authorized_in_master(request, email):
                return ident

            # Not authorized anymore -> wipe session identity
            request.session.pop(SESSION_KEY, None)
            request.session.pop(SESSION_CAMPAIGN_KEY, None)
            request.session.pop(SESSION_PUBLISHER_MASTER_VALIDATION, None)
            request.session.modified = True
            return None

    legacy = request.session.get(LEGACY_SESSION_KEY)
    if isinstance(legacy, dict):
        roles = _normalize_roles(legacy.get("roles"))
        if "publisher" in [r.lower() for r in roles]:
            email = _extract_email_from_claims(legacy)
            if _is_publisher_authorized_in_master(request, email):
                return legacy

            request.session.pop(LEGACY_SESSION_KEY, None)
            request.session.pop(LEGACY_CAMPAIGN_KEY, None)
            request.session.pop(SESSION_PUBLISHER_MASTER_VALIDATION, None)
            request.session.modified = True
            return None

    return None


def _redirect_to_sso_consume(request: HttpRequest, token: str) -> HttpResponse:
    campaign_id = (
        request.GET.get("campaign_id")
        or request.GET.get("campaign-id")
        or request.session.get(SESSION_CAMPAIGN_KEY)
        or request.session.get(LEGACY_CAMPAIGN_KEY)
        or ""
    )
    if not campaign_id:
        return unauthorized_response()

    # next URL without token params
    params = request.GET.copy()
    for k in ("token", "sso_token", "jwt", "access_token"):
        params.pop(k, None)

    next_url = request.path
    if params:
        next_url = f"{next_url}?{params.urlencode()}"

    consume_url = "/sso/consume/?" + urlencode(
        {"token": token, "campaign_id": campaign_id, "next": next_url}
    )
    return redirect(consume_url)


def publisher_required(view_func):
    """
    Destination behavior:
    - If valid SSO session exists AND master allow-list passes -> allow
    - Else if token present -> route via /sso/consume/
    - Else -> 401

    Debug:
      Add ?debug_sso=1 to any protected URL to see plaintext diagnostics.
    """

    @wraps(view_func)
    def _wrapped(request: HttpRequest, *args, **kwargs):
        req_id = request.META.get("HTTP_X_REQUEST_ID") or uuid.uuid4().hex[:12]

        def _plog(event: str, **data) -> None:
            payload = {
                "ts": int(time.time()),
                "req_id": req_id,
                "event": event,
                "path": request.path,
                "method": request.method,
                "view": getattr(view_func, "__name__", "unknown"),
            }
            payload.update(data)
            print(json.dumps(payload, default=str))

        debug = _debug_enabled(request)
        debug_lines = []

        def _dbg(line: str):
            if debug:
                debug_lines.append(line)

        _plog("publisher_required.start", query_keys=list(request.GET.keys()))
        _dbg("publisher_required.start")
        _dbg(f"path={request.path}")
        _dbg(f"query_keys={list(request.GET.keys())}")

        try:
            claims = get_publisher_claims(request)
        except Exception as e:
            _plog("publisher_required.claims_error", error=str(e))
            _dbg(f"claims_error={type(e).__name__}: {e}")
            return HttpResponse("\n".join(debug_lines), status=401, content_type="text/plain") if debug else unauthorized_response()

        if claims:
            _plog("publisher_required.authorized", roles=claims.get("roles"))
            _dbg("authorized=True")
            return view_func(request, *args, **kwargs)

        # Not authorized via session. Explain why (debug), then try token.
        ident = request.session.get(SESSION_KEY)
        _dbg(f"has_session_ident={isinstance(ident, dict)}")
        if isinstance(ident, dict):
            _dbg(f"session_ident_keys={list(ident.keys())}")
            _dbg(f"session_roles={ident.get('roles')}")
            extracted_email = _extract_email_from_claims(ident)
            _dbg(f"extracted_email={'<missing>' if not extracted_email else extracted_email}")
            try:
                ok = master_db.authorized_publisher_exists(extracted_email) if extracted_email else False
                _dbg(f"master_allowlist_ok={ok}")
            except Exception as e:
                _dbg(f"master_allowlist_error={type(e).__name__}: {e}")

        token = _extract_token(request)
        _dbg(f"token_present={bool(token)}")
        if token:
            _plog("publisher_required.token_present.redirect_to_consume", token_len=len(token))
            _dbg("redirecting_to=/sso/consume/")
            resp = _redirect_to_sso_consume(request, token)
            return HttpResponse("\n".join(debug_lines), content_type="text/plain") if debug else resp

        _plog("publisher_required.unauthorized.no_claims_no_token")
        _dbg("unauthorized_reason=no_claims_no_token_or_session_invalidated")
        return HttpResponse("\n".join(debug_lines), status=401, content_type="text/plain") if debug else unauthorized_response()

    return _wrapped
