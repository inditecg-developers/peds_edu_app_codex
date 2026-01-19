from __future__ import annotations

from functools import wraps
from typing import Any, Dict, Optional, Sequence
from urllib.parse import urlencode

from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect

# Part-C session keys (Project2)
SESSION_KEY = getattr(settings, "SSO_SESSION_KEY_IDENTITY", "sso_identity")
SESSION_CAMPAIGN_KEY = getattr(settings, "SSO_SESSION_KEY_CAMPAIGN", "campaign_id")

# Legacy compatibility (if any old sessions exist)
LEGACY_SESSION_KEY = "publisher_jwt_claims"
LEGACY_CAMPAIGN_KEY = "publisher_current_campaign_id"


def unauthorized_response() -> HttpResponse:
    return HttpResponse("unauthorised access", status=401, content_type="text/plain")


def _normalize_roles(value: Any) -> Sequence[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    return [str(value)]


def _extract_token(request: HttpRequest) -> Optional[str]:
    token = (
        request.GET.get("token")
        or request.GET.get("jwt")
        or request.GET.get("access_token")
    )
    if token:
        return token.strip()

    auth = (request.META.get("HTTP_AUTHORIZATION") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()

    return None


def get_publisher_claims(request: HttpRequest) -> Optional[Dict[str, Any]]:
    ident = request.session.get(SESSION_KEY)
    if isinstance(ident, dict):
        roles = _normalize_roles(ident.get("roles"))
        if "publisher" in [r.lower() for r in roles]:
            return ident

    legacy = request.session.get(LEGACY_SESSION_KEY)
    if isinstance(legacy, dict):
        roles = _normalize_roles(legacy.get("roles"))
        if "publisher" in [r.lower() for r in roles]:
            return legacy

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
    for k in ("token", "jwt", "access_token"):
        if k in params:
            params.pop(k)

    next_url = request.path
    if params:
        next_url = f"{next_url}?{params.urlencode()}"

    consume_url = "/sso/consume/?" + urlencode(
        {"token": token, "campaign_id": campaign_id, "next": next_url}
    )
    return redirect(consume_url)


def publisher_required(view_func):
    """
    Part-C destination behavior:
    - If valid SSO session exists -> allow
    - Else if token present -> route via /sso/consume/
    - Else -> 401 unauthorised access
    """
    @wraps(view_func)
    def _wrapped(request: HttpRequest, *args, **kwargs):
        if get_publisher_claims(request):
            return view_func(request, *args, **kwargs)

        token = _extract_token(request)
        if token:
            return _redirect_to_sso_consume(request, token)

        return unauthorized_response()

    return _wrapped
