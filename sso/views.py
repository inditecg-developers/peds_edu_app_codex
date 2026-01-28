from __future__ import annotations

import json
import time
import uuid

from django.conf import settings
from django.contrib import messages
from django.http import HttpResponse
from django.shortcuts import redirect
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_http_methods

from .jwt import decode_and_verify_hs256_jwt, JWTError


@require_http_methods(["GET"])
def consume(request):
    """
    SSO endpoint:
      GET /sso/consume/?token=...&campaign_id=...&next=/some/path

    Validates token and then creates Project2 session.

    Debug mode:
      Append ?debug_sso=1 to see plaintext progress output in browser.
      No sensitive values are exposed.
    """

    # ------------------------------------------------------------------
    # Debug helpers (NO-OP unless ?debug_sso=1)
    # ------------------------------------------------------------------
    debug_enabled = request.GET.get("debug_sso") == "1"
    debug_lines: list[str] = []

    def _debug(msg: str):
        if debug_enabled:
            debug_lines.append(msg)

    def _debug_response(final: bool = False):
        if debug_enabled and final:
            return HttpResponse(
                "\n".join(debug_lines),
                content_type="text/plain",
            )
        return None

    # ------------------------------------------------------------------
    # Structured log (still safe even without server access)
    # ------------------------------------------------------------------
    req_id = request.META.get("HTTP_X_REQUEST_ID") or uuid.uuid4().hex[:12]

    def _log(event: str, **data):
        payload = {
            "ts": int(time.time()),
            "req_id": req_id,
            "event": event,
        }
        payload.update(data)
        print(json.dumps(payload, default=str))

    # ------------------------------------------------------------------
    # Token + params (support alternate token names)
    # ------------------------------------------------------------------
    token = (
        request.GET.get("token")
        or request.GET.get("sso_token")
        or request.GET.get("jwt")
        or request.GET.get("access_token")
        or ""
    ).strip()

    campaign_id_raw = (
        request.GET.get("campaign_id")
        or request.GET.get("campaign-id")
        or ""
    ).strip()

    next_url = (request.GET.get("next") or "/").strip() or "/"

    _debug("Reached consume()")
    _debug(f"token present: {bool(token)}")
    _debug(f"campaign_id present: {bool(campaign_id_raw)}")

    _log(
        "sso.consume.start",
        token_len=len(token),
        has_campaign_id=bool(campaign_id_raw),
        next_path=next_url[:200],
    )

    # ------------------------------------------------------------------
    # Basic validation
    # ------------------------------------------------------------------
    if not token or not campaign_id_raw:
        _debug("FAIL: missing token or campaign_id")
        _log("sso.consume.missing_params")
        messages.error(request, "Missing token or campaign_id.")
        return _debug_response(final=True) or redirect("/")

    if not getattr(settings, "SSO_SHARED_SECRET", ""):
        _debug("FAIL: SSO_SHARED_SECRET not configured")
        _log("sso.consume.misconfigured")
        messages.error(request, "SSO not configured.")
        return _debug_response(final=True) or redirect("/")

    # ------------------------------------------------------------------
    # Decode + verify JWT
    # ------------------------------------------------------------------
    try:
        payload = decode_and_verify_hs256_jwt(
            token,
            secret=settings.SSO_SHARED_SECRET,
            issuer=settings.SSO_EXPECTED_ISSUER,
            audience=settings.SSO_EXPECTED_AUDIENCE,
        )
        _debug("JWT verified successfully")
    except JWTError as e:
        _debug(f"FAIL: JWT error ({e.__class__.__name__})")
        _log("sso.consume.jwt_error", error=str(e))
        messages.error(
            request,
            "SSO link is invalid or expired. Please reopen it from the publisher portal."
        )
        return _debug_response(final=True) or redirect("/")

    # ------------------------------------------------------------------
    # Required claims
    # ------------------------------------------------------------------
    sub = (payload.get("sub") or "").strip()
    username = (payload.get("username") or "").strip()
    roles = payload.get("roles") or []

    _debug(f"sub present: {bool(sub)}")
    _debug(f"username present: {bool(username)}")
    _debug(f"roles valid list: {isinstance(roles, list)}")

    _log(
        "sso.consume.jwt_ok",
        sub=(sub[:6] + "***") if sub else "",
        username=(
            (username.split("@")[0][:2] + "***@" + username.split("@")[1])
            if "@" in username
            else (username[:3] + "***")
        ),
        roles=roles,
    )

    if not sub or not username or not isinstance(roles, list):
        _debug("FAIL: missing or invalid JWT claims")
        _log("sso.consume.claims_invalid")
        messages.error(request, "SSO token missing required claims.")
        return _debug_response(final=True) or redirect("/")

    # ------------------------------------------------------------------
    # Optional hardening: campaign_id inside JWT
    # ------------------------------------------------------------------
    token_campaign = (payload.get("campaign_id") or "").strip()
    if token_campaign and token_campaign != campaign_id_raw:
        _debug("FAIL: campaign_id mismatch (token vs query)")
        _log("sso.consume.campaign_mismatch")
        messages.error(request, "Invalid campaign_id.")
        return _debug_response(final=True) or redirect("/")

    # ------------------------------------------------------------------
    # Normalize campaign_id
    # ------------------------------------------------------------------
    campaign_id_value = campaign_id_raw
    try:
        campaign_id_value = str(uuid.UUID(campaign_id_raw))
        _debug("campaign_id normalized to UUID")
    except Exception:
        _debug("campaign_id treated as string")

    # ------------------------------------------------------------------
    # Create Project2 session
    # ------------------------------------------------------------------
    request.session[getattr(settings, "SSO_SESSION_KEY_IDENTITY", "sso_identity")] = {
        "sub": sub,
        "username": username,
        "roles": roles,
        "iss": payload.get("iss"),
        "aud": payload.get("aud"),
    }
    request.session[getattr(settings, "SSO_SESSION_KEY_CAMPAIGN", "campaign_id")] = str(
        campaign_id_value
    )

    request.session.set_expiry(
        getattr(settings, "SSO_SESSION_AGE_SECONDS", 3600)
    )
    request.session.modified = True

    _debug("Session created successfully")
    _log(
        "sso.consume.session_set",
        session_age_seconds=getattr(settings, "SSO_SESSION_AGE_SECONDS", 3600),
    )

    # ------------------------------------------------------------------
    # Safe redirect
    # ------------------------------------------------------------------
    if not url_has_allowed_host_and_scheme(
        next_url,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        _debug("Unsafe next URL — redirecting to /")
        _log("sso.consume.unsafe_next_url")
        next_url = "/"

    _debug(f"Redirecting to: {next_url}")
    _log("sso.consume.redirect", next_url=next_url)

    return _debug_response(final=True) or redirect(next_url)
