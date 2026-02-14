
from __future__ import annotations
import json
from typing import Any, Dict, List, Set
from urllib.parse import urlencode
from django import forms
from django.contrib import messages
from django.db import models, transaction
from django.db.models import Q
from django.http import (
    HttpRequest,
    HttpResponse,
    HttpResponseBadRequest,
    JsonResponse,
)
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.text import slugify
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from django import forms
from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.http import urlencode
from django.views.decorators.http import require_http_methods

from accounts import master_db

from catalog.models import (
    TherapyArea,
    TriggerCluster,
    Trigger,
    Video,
    VideoLanguage,
    VideoCluster,
    VideoClusterLanguage,
    VideoClusterVideo,
)

from .campaign_auth import SESSION_CAMPAIGN_KEY, get_publisher_claims, publisher_required
from .campaign_forms import CampaignCreateForm, CampaignEditForm
from .models import Campaign


# -----------------------------
# Helpers
# -----------------------------
SESSION_CAMPAIGN_META_BY_CAMPAIGN_KEY = "publisher_campaign_meta_by_campaign"

def _capture_campaign_meta(request: HttpRequest, campaign_id: str | None) -> dict[str, Any]:
    """
    Capture extra values coming from Project1 and persist them in session.
    Stored per-campaign to avoid collisions.
    """
    meta_by_campaign = request.session.get(SESSION_CAMPAIGN_META_BY_CAMPAIGN_KEY) or {}
    meta = meta_by_campaign.get(campaign_id, {}) if campaign_id else {}

    param_names = [
        "num_doctors_supported",
        "name",
        "company_name",
        "contact_person_name",
        "contact_person_phone",
        "contact_person_email",
    ]

    for key in param_names:
        v = request.GET.get(key)
        if v is None:
            v = request.GET.get(key.replace("_", "-"))  # tolerate hyphenated keys
        if v is not None:
            meta[key] = str(v).strip()

    # normalize int
    try:
        meta["num_doctors_supported"] = int(meta.get("num_doctors_supported")) if meta.get("num_doctors_supported") not in (None, "") else None
    except Exception:
        meta["num_doctors_supported"] = None

    if campaign_id:
        meta_by_campaign[campaign_id] = meta
        request.session[SESSION_CAMPAIGN_META_BY_CAMPAIGN_KEY] = meta_by_campaign
        request.session.modified = True

    return meta

class FieldRepWhatsAppForm(forms.Form):
    whatsapp_number = forms.CharField(
        label="Enter doctor’s WhatsApp number",
        max_length=20,
        required=True,
        widget=forms.TextInput(attrs={"placeholder": "e.g. 9876543210", "inputmode": "numeric"}),
    )


def _jwt_b64url_decode(seg: str) -> bytes:
    import base64
    s = seg.encode("utf-8")
    s += b"=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode(s)

def _decode_and_verify_hs256(token: str, secret: str) -> dict:
    """
    Minimal HS256 JWT verifier (no external deps).
    Returns payload dict on success; raises ValueError on failure.
    """
    import hmac, hashlib, json as _json
    parts = (token or "").split(".")
    if len(parts) != 3:
        raise ValueError("token_not_3_parts")

    header_b64, payload_b64, sig_b64 = parts
    signing_input = (header_b64 + "." + payload_b64).encode("utf-8")
    sig = _jwt_b64url_decode(sig_b64)

    mac = hmac.new((secret or "").encode("utf-8"), signing_input, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, sig):
        raise ValueError("signature_mismatch")

    payload_raw = _jwt_b64url_decode(payload_b64)
    obj = _json.loads(payload_raw.decode("utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("payload_not_object")
    return obj


def _render_campaign_text_template(
    template: str,
    *,
    doctor_name: str,
    clinic_link: str,
    setup_link: str = "",
) -> str:
    """
    Replace all placeholders unconditionally.
    Empty values should REMOVE placeholders, not preserve them.
    """
    text = template or ""

    replacements = {
        "<doctor.user.full_name>": doctor_name or "",
        "<doctor_name>": doctor_name or "",
        "{{doctor_name}}": doctor_name or "",

        "<clinic_link>": clinic_link or "",
        "{{clinic_link}}": clinic_link or "",

        "<setup_link>": setup_link or "",
        "{{setup_link}}": setup_link or "",
    }

    for k, v in replacements.items():
        text = text.replace(k, v)

    return text.strip()



@require_http_methods(["GET", "POST"])
def field_rep_landing_page(request: HttpRequest) -> HttpResponse:
    """
    Field rep landing page.

    On-screen debug:
      - Append `&debug=1` to the URL to show a debug panel (safe-masked).
      - Debug output is only intended for internal troubleshooting.
    """
    import json
    import time
    import uuid
    import traceback
    import re
    from urllib.parse import urlencode as _urlencode
    from django.db import connections

    # ------------------------------------------------------------------
    # Debug controls (on-screen)
    # ------------------------------------------------------------------
    debug_mode = str(request.GET.get("debug") or "").lower() in ("1", "true", "yes", "y")
    if not debug_mode:
        # Allow debug panel in Django DEBUG mode as well.
        try:
            debug_mode = bool(getattr(settings, "DEBUG", False))
        except Exception:
            debug_mode = False

    # -------------------------
    # lightweight JSON logger (stdout) + on-screen debug info
    # -------------------------
    req_id = request.META.get("HTTP_X_REQUEST_ID") or uuid.uuid4().hex[:12]
    start_ts = time.time()

    def _mask_phone(value: str) -> str:
        s = "".join(ch for ch in str(value or "") if ch.isdigit())
        if not s:
            return ""
        if len(s) <= 4:
            return "*" * len(s)
        return ("*" * (len(s) - 4)) + s[-4:]

    def _mask_email(value: str) -> str:
        v = (value or "").strip()
        if "@" not in v:
            return v[:2] + "***" if v else ""
        local, domain = v.split("@", 1)
        if len(local) <= 2:
            local_masked = local[0] + "*"
        else:
            local_masked = local[:2] + "***"
        return f"{local_masked}@{domain}"

    def _plog(event: str, **data) -> None:
        payload = {
            "ts": int(time.time()),
            "req_id": req_id,
            "event": event,
            "path": request.path,
            "method": request.method,
        }
        payload.update(data)
        try:
            print(json.dumps(payload, default=str))
        except Exception:
            print(f"[req_id={req_id}] {event} {data}")

    # Safe on-screen debug payload (masked)
    debug_info: Dict[str, Any] = {
        "req_id": req_id,
        "debug_mode": debug_mode,
        "path": request.path,
        "method": request.method,
        "get_params": {},
    }

    try:
        safe_params = {}
        for k in request.GET.keys():
            v = request.GET.get(k, "")
            if k.lower() in ("token", "jwt", "access_token"):
                safe_params[k] = f"<masked len={len(v or '')}>"
            else:
                safe_params[k] = v
        debug_info["get_params"] = safe_params
    except Exception:
        debug_info["get_params"] = {"_error": "failed_to_read_get_params"}

    def _render_with_debug(status: int, **context):
        if debug_mode:
            try:
                context["debug_json"] = json.dumps(debug_info, indent=2, sort_keys=True, default=str)
            except Exception:
                context["debug_json"] = repr(debug_info)
        context["debug_mode"] = debug_mode
        return render(request, "publisher/field_rep_landing_page.html", context, status=status)

    def _normalize_campaign_id_for_master(raw: str) -> str:
        # Many master tables store campaign_id without hyphens (32 hex).
        return (raw or "").strip().replace("-", "")

    _plog("field_rep_landing.start")

    # -------------------------
    # Parse inputs
    # -------------------------
    campaign_id = (request.GET.get("campaign-id") or request.GET.get("campaign_id") or "").strip()
    field_rep_id_raw = (request.GET.get("field_rep_id") or request.GET.get("field-rep-id") or "").strip()

    campaign_id_db = _normalize_campaign_id_for_master(campaign_id)

    debug_info.update(
        {
            "campaign_id": campaign_id,
            "campaign_id_db": campaign_id_db,
            "field_rep_id_raw": field_rep_id_raw,
            "query_keys": list(request.GET.keys()),
        }
    )

    _plog(
        "field_rep_landing.params",
        campaign_id=campaign_id,
        campaign_id_db=campaign_id_db,
        field_rep_id=field_rep_id_raw,
        query_keys=list(request.GET.keys()),
    )

    if not campaign_id or not field_rep_id_raw:
        _plog("field_rep_landing.bad_request.missing_params")
        return _render_with_debug(
            400,
            form=FieldRepWhatsAppForm(),
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            limit_reached=True,
            limit_message="Missing campaign-id or field_rep_id in URL.",
        )

    # -------------------------
    # MASTER DB: resolve Field Rep robustly
    # -------------------------
    master_alias = getattr(settings, "MASTER_DB_ALIAS", "master")
    master_conn = connections[master_alias]

    join_table = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_TABLE", "campaign_campaignfieldrep")
    join_pk_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_PK_COLUMN", "id")
    join_campaign_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_CAMPAIGN_COLUMN", "campaign_id")
    join_fieldrep_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_FIELD_REP_COLUMN", "field_rep_id")

    debug_info["master"] = {
        "alias": master_alias,
        "join_table": join_table,
        "join_pk_col": join_pk_col,
        "join_campaign_col": join_campaign_col,
        "join_fieldrep_col": join_fieldrep_col,
    }

    # Candidates to try (URL param, plus SSO sub if present)
    lookup_candidates: List[str] = [field_rep_id_raw]
    # Also try to resolve field rep from JWT token if present (important when session is empty)
    token = (request.GET.get("token") or "").strip()
    if token:
        try:
            secret = getattr(settings, "SSO_SHARED_SECRET", "") or getattr(settings, "PUBLISHER_SSO_SHARED_SECRET", "")
            claims = _decode_and_verify_hs256(token, secret)
            sub = str(claims.get("sub") or "").strip()   # e.g. "fieldrep_16"
            if sub and sub not in lookup_candidates:
                lookup_candidates.append(sub)
    
            m = re.search(r"(\d+)$", sub)
            if m and m.group(1) not in lookup_candidates:
                lookup_candidates.append(m.group(1))
        except Exception as e:
            # keep working; debug output will show this
            _plog("field_rep_landing.token_decode_error", error=str(e))


    session_key = getattr(settings, "SSO_SESSION_KEY_IDENTITY", "sso_identity")
    ident = request.session.get(session_key)
    sub = ""
    if isinstance(ident, dict):
        sub = (ident.get("sub") or "").strip()
        if sub and sub not in lookup_candidates:
            lookup_candidates.append(sub)
        m = re.search(r"(\d+)$", sub)
        if m and m.group(1) not in lookup_candidates:
            lookup_candidates.append(m.group(1))

    debug_info["field_rep_lookup_candidates"] = list(lookup_candidates)

    # Helper: resolve campaign_campaignfieldrep join-pk -> actual field_rep_id
    def _resolve_fieldrep_id_from_join_pk(join_pk: int) -> str | None:
        try:
            sql = (
                f"SELECT {join_fieldrep_col} "
                f"FROM {join_table} "
                f"WHERE {join_pk_col} = %s "
                f"  AND ({join_campaign_col} = %s OR {join_campaign_col} = %s) "
                f"LIMIT 1"
            )
            with master_conn.cursor() as cur:
                cur.execute(sql, [int(join_pk), campaign_id_db, campaign_id])
                row = cur.fetchone()
            if row and row[0] is not None:
                return str(row[0]).strip()
            return None
        except Exception as e:
            debug_info.setdefault("errors", []).append(
                {
                    "stage": "join_pk_lookup",
                    "join_pk": str(join_pk),
                    "error": f"{type(e).__name__}: {e}",
                }
            )
            return None

    # Helper: verify field rep is linked to campaign in join table
    def _is_fieldrep_linked_to_campaign(field_rep_pk: int) -> bool:
        try:
            sql = (
                f"SELECT 1 FROM {join_table} "
                f"WHERE ({join_campaign_col} = %s OR {join_campaign_col} = %s) "
                f"  AND {join_fieldrep_col} = %s "
                f"LIMIT 1"
            )
            with master_conn.cursor() as cur:
                cur.execute(sql, [campaign_id_db, campaign_id, int(field_rep_pk)])
                return cur.fetchone() is not None
        except Exception as e:
            debug_info.setdefault("errors", []).append(
                {
                    "stage": "join_link_check",
                    "field_rep_pk": str(field_rep_pk),
                    "error": f"{type(e).__name__}: {e}",
                }
            )
            return False

    # Attempt resolution paths:
    #  - Direct: interpret the URL value as a FieldRep identifier (pk, external id, or token style)
    #  - Join-PK: if the URL value is numeric, interpret it as campaign_campaignfieldrep.id
    direct_hits: List[Dict[str, Any]] = []
    join_pk_hit: Dict[str, Any] = {}

    resolved_options: List[Dict[str, Any]] = []

    # ---- Direct lookups (do NOT early-return; keep options for later selection)
    for cand in lookup_candidates:
        if not cand:
            continue
        try:
            tmp = master_db.get_field_rep(cand)
        except Exception as e:
            _plog(
                "field_rep_landing.field_rep.direct_lookup_error",
                candidate=cand,
                error=str(e),
                traceback=traceback.format_exc()[-2000:],
            )
            debug_info.setdefault("errors", []).append(
                {"stage": "field_rep_direct_lookup", "candidate": cand, "error": f"{type(e).__name__}: {e}"}
            )
            tmp = None

        if not tmp:
            continue

        hit = {
            "candidate": cand,
            "id": int(tmp.id),
            "brand_supplied_field_rep_id": str(tmp.brand_supplied_field_rep_id or ""),
            "is_active": bool(tmp.is_active),
        }
        direct_hits.append(hit)

        linked = _is_fieldrep_linked_to_campaign(int(tmp.id))
        resolved_options.append(
            {
                "source": "direct",
                "candidate": cand,
                "field_rep_id": int(tmp.id),
                "brand_supplied_field_rep_id": str(tmp.brand_supplied_field_rep_id or ""),
                "is_active": bool(tmp.is_active),
                "linked_to_campaign": bool(linked),
            }
        )

    debug_info["field_rep_direct_hits"] = direct_hits

    # ---- Join-PK lookup for the RAW URL value (numeric only)
    join_resolved_fieldrep_id = None
    fr_from_join_pk = None
    if field_rep_id_raw.isdigit():
        try:
            join_pk = int(field_rep_id_raw)
        except Exception:
            join_pk = None

        if join_pk is not None:
            join_resolved_fieldrep_id = _resolve_fieldrep_id_from_join_pk(join_pk)
            join_pk_hit = {
                "join_pk": join_pk,
                "resolved_fieldrep_id": join_resolved_fieldrep_id,
            }

            if join_resolved_fieldrep_id:
                try:
                    fr_from_join_pk = master_db.get_field_rep(str(join_resolved_fieldrep_id))
                except Exception as e:
                    debug_info.setdefault("errors", []).append(
                        {
                            "stage": "field_rep_join_pk_get_field_rep",
                            "resolved_fieldrep_id": str(join_resolved_fieldrep_id),
                            "error": f"{type(e).__name__}: {e}",
                        }
                    )
                    fr_from_join_pk = None

                if fr_from_join_pk:
                    linked = _is_fieldrep_linked_to_campaign(int(fr_from_join_pk.id))
                    resolved_options.append(
                        {
                            "source": "join_pk",
                            "candidate": field_rep_id_raw,
                            "join_pk": join_pk,
                            "field_rep_id": int(fr_from_join_pk.id),
                            "brand_supplied_field_rep_id": str(fr_from_join_pk.brand_supplied_field_rep_id or ""),
                            "is_active": bool(fr_from_join_pk.is_active),
                            "linked_to_campaign": bool(linked),
                        }
                    )

    debug_info["field_rep_join_pk_result"] = join_pk_hit
    debug_info["field_rep_resolution_options"] = resolved_options

    # Select the best option:
    #  - Must be linked_to_campaign
    #  - Must be active
    selected = None
    for opt in resolved_options:
        if opt.get("linked_to_campaign") and opt.get("is_active"):
            selected = opt
            break

    # If none matched, keep a fallback candidate for better messaging/debug:
    if selected is None and resolved_options:
        # Prefer "linked" over "not linked", even if inactive
        for opt in resolved_options:
            if opt.get("linked_to_campaign"):
                selected = opt
                break
        if selected is None:
            selected = resolved_options[0]

    debug_info["field_rep_selected"] = selected

    # Hydrate selected MasterFieldRep object if possible
    fr = None
    if selected:
        try:
            fr = master_db.get_field_rep(str(selected.get("field_rep_id")))
        except Exception as e:
            debug_info.setdefault("errors", []).append(
                {"stage": "field_rep_selected_hydrate", "field_rep_id": selected.get("field_rep_id"), "error": f"{type(e).__name__}: {e}"}
            )
            fr = None

    # Hard failure: not found OR inactive OR not linked
    if not fr:
        debug_info["field_rep_fail_reason"] = "field_rep_not_found"
        _plog("field_rep_landing.unauthorized.field_rep_not_found")
        return _render_with_debug(
            401,
            form=FieldRepWhatsAppForm(),
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            limit_reached=True,
            limit_message="Invalid field rep id. (Add &debug=1 to view debug details.)" if not debug_mode else "Invalid field rep id.",
        )

    if not bool(getattr(fr, "is_active", False)):
        debug_info["field_rep_fail_reason"] = "field_rep_inactive"
        _plog("field_rep_landing.unauthorized.field_rep_inactive", field_rep_id=str(fr.id))
        return _render_with_debug(
            401,
            form=FieldRepWhatsAppForm(),
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            limit_reached=True,
            limit_message="Inactive field rep id. (Add &debug=1 to view debug details.)" if not debug_mode else "Inactive field rep id.",
        )

    # Enforce link to campaign
    linked = _is_fieldrep_linked_to_campaign(int(fr.id))
    debug_info["field_rep_linked_to_campaign"] = bool(linked)

    if not linked:
        debug_info["field_rep_fail_reason"] = "field_rep_not_linked_to_campaign"
        _plog("field_rep_landing.unauthorized.field_rep_not_linked_to_campaign", field_rep_id=str(fr.id))
        return _render_with_debug(
            401,
            form=FieldRepWhatsAppForm(),
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            limit_reached=True,
            limit_message="This field rep is not authorized for this campaign. (Add &debug=1 to view debug details.)"
            if not debug_mode
            else "This field rep is not authorized for this campaign.",
        )

    _plog(
        "field_rep_landing.field_rep.ok",
        resolution_selected=selected,
        master_field_rep_id=fr.id,
        brand_supplied_field_rep_id=fr.brand_supplied_field_rep_id,
    )

    # Stable downstream field rep identifier for enrollment + register redirect
    downstream_field_rep_id = (fr.brand_supplied_field_rep_id or "").strip() or str(fr.id)

    # -------------------------
    # Fetch campaign (MASTER DB)
    # -------------------------
    try:
        campaign = master_db.get_campaign(campaign_id_db) or master_db.get_campaign(campaign_id)
        debug_info["campaign_lookup"] = {
            "requested": campaign_id,
            "normalized": campaign_id_db,
            "found": bool(campaign),
            "doctors_supported": (campaign.doctors_supported if campaign else None),
        }
        _plog(
            "field_rep_landing.campaign.lookup",
            found=bool(campaign),
            doctors_supported=(campaign.doctors_supported if campaign else None),
        )
    except Exception as e:
        debug_info.setdefault("errors", []).append({"stage": "campaign_lookup", "error": f"{type(e).__name__}: {e}"})
        _plog(
            "field_rep_landing.campaign.lookup_error",
            error=str(e),
            traceback=traceback.format_exc()[-2000:],
        )
        return _render_with_debug(
            500,
            form=FieldRepWhatsAppForm(),
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            limit_reached=True,
            limit_message="Master DB error while fetching campaign.",
        )

    if not campaign:
        _plog("field_rep_landing.bad_request.unknown_campaign")
        return _render_with_debug(
            400,
            form=FieldRepWhatsAppForm(),
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            limit_reached=True,
            limit_message="Unknown campaign-id (not found in master campaign table).",
        )

    # -------------------------
    # Enrollment count + license enforcement
    # -------------------------
    doctors_supported = int(campaign.doctors_supported or 0)

    try:
        enrolled_count_norm = master_db.count_campaign_enrollments(campaign_id_db)
        enrolled_count_raw = 0
        if campaign_id_db != campaign_id:
            enrolled_count_raw = master_db.count_campaign_enrollments(campaign_id)
        enrolled_count = max(enrolled_count_norm, enrolled_count_raw)

        debug_info["enrollment_count"] = {
            "enrolled_count_norm": enrolled_count_norm,
            "enrolled_count_raw": enrolled_count_raw,
            "enrolled_count": enrolled_count,
        }

        _plog(
            "field_rep_landing.enrollment_count",
            campaign_id_db=campaign_id_db,
            enrolled_count_norm=enrolled_count_norm,
            enrolled_count_raw=enrolled_count_raw,
            enrolled_count=enrolled_count,
        )
    except Exception as e:
        debug_info.setdefault("errors", []).append({"stage": "enrollment_count", "error": f"{type(e).__name__}: {e}"})
        _plog(
            "field_rep_landing.enrollment_count_error",
            error=str(e),
            traceback=traceback.format_exc()[-2000:],
        )
        return _render_with_debug(
            500,
            form=FieldRepWhatsAppForm(),
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            campaign=campaign,
            enrolled_count=0,
            doctors_supported=doctors_supported,
            limit_reached=True,
            limit_message="Master DB error while counting enrollments.",
        )

    limit_reached = bool(doctors_supported and enrolled_count >= doctors_supported)
    limit_message = (
        "This campaign already has the maximum allowed doctors registered. "
        "If you wish to register more doctors, please speak to your brand manager who can answer your queries "
        "and obtain more licenses."
    )

    debug_info["limit_check"] = {
        "doctors_supported": doctors_supported,
        "enrolled_count": enrolled_count,
        "limit_reached": limit_reached,
    }

    _plog(
        "field_rep_landing.limit_check",
        doctors_supported=doctors_supported,
        enrolled_count=enrolled_count,
        limit_reached=limit_reached,
    )

    if request.method == "GET":
        _plog("field_rep_landing.render_get", elapsed_ms=int((time.time() - start_ts) * 1000))
        return _render_with_debug(
            200,
            form=FieldRepWhatsAppForm(),
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            campaign=campaign,
            enrolled_count=enrolled_count,
            doctors_supported=doctors_supported,
            limit_reached=limit_reached,
            limit_message=limit_message,
        )

    # -------------------------
    # POST: validate WhatsApp input
    # -------------------------
    form = FieldRepWhatsAppForm(request.POST)
    if not form.is_valid():
        debug_info["post_form_errors"] = form.errors.get_json_data()
        _plog(
            "field_rep_landing.post.invalid_form",
            errors=form.errors.get_json_data(),
            elapsed_ms=int((time.time() - start_ts) * 1000),
        )
        return _render_with_debug(
            200,
            form=form,
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            campaign=campaign,
            enrolled_count=enrolled_count,
            doctors_supported=doctors_supported,
            limit_reached=limit_reached,
            limit_message=limit_message,
        )

    if limit_reached:
        _plog("field_rep_landing.post.limit_reached_block", elapsed_ms=int((time.time() - start_ts) * 1000))
        return _render_with_debug(
            200,
            form=form,
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            campaign=campaign,
            enrolled_count=enrolled_count,
            doctors_supported=doctors_supported,
            limit_reached=True,
            limit_message=limit_message,
        )

    wa_number = form.cleaned_data["whatsapp_number"]
    debug_info["post_whatsapp_number_masked"] = _mask_phone(wa_number)
    _plog("field_rep_landing.post.whatsapp_received", whatsapp_masked=_mask_phone(wa_number))

    # -------------------------
    # Check doctor exists in MASTER DB by WhatsApp
    # -------------------------
    try:
        doctor = master_db.get_doctor_by_whatsapp(wa_number)
        debug_info["doctor_lookup"] = {
            "found": bool(doctor),
            "doctor_id": (doctor.doctor_id if doctor else None),
            "doctor_email_masked": (_mask_email(doctor.email) if doctor and doctor.email else None),
        }
        _plog(
            "field_rep_landing.doctor.lookup",
            found=bool(doctor),
            doctor_id=(doctor.doctor_id if doctor else None),
            doctor_email_masked=(_mask_email(doctor.email) if doctor and doctor.email else None),
        )
    except Exception as e:
        debug_info.setdefault("errors", []).append({"stage": "doctor_lookup", "error": f"{type(e).__name__}: {e}"})
        _plog(
            "field_rep_landing.doctor.lookup_error",
            error=str(e),
            traceback=traceback.format_exc()[-2000:],
        )
        return _render_with_debug(
            500,
            form=form,
            campaign_id=campaign_id,
            field_rep_id=field_rep_id_raw,
            campaign=campaign,
            enrolled_count=enrolled_count,
            doctors_supported=doctors_supported,
            limit_reached=True,
            limit_message="Master DB error while searching doctor.",
        )

    base_url = getattr(settings, "PUBLIC_BASE_URL", "https://portal.cpdinclinic.co.in").rstrip("/")

    if doctor:
        # Ensure enrollment exists in master DB
        registered_by = downstream_field_rep_id
        try:
            master_db.ensure_enrollment(
                doctor_id=doctor.doctor_id,
                campaign_id=campaign_id_db,
                registered_by=registered_by,
            )
            _plog(
                "field_rep_landing.enrollment.ensure",
                doctor_id=doctor.doctor_id,
                campaign_id=campaign_id_db,
                registered_by=registered_by,
                status="ok",
            )
        except Exception as e:
            debug_info.setdefault("errors", []).append({"stage": "ensure_enrollment", "error": f"{type(e).__name__}: {e}"})
            _plog(
                "field_rep_landing.enrollment.ensure_error",
                doctor_id=doctor.doctor_id,
                campaign_id=campaign_id_db,
                registered_by=registered_by,
                error=str(e),
                traceback=traceback.format_exc()[-2000:],
            )
            return _render_with_debug(
                500,
                form=form,
                campaign_id=campaign_id,
                field_rep_id=field_rep_id_raw,
                campaign=campaign,
                enrolled_count=enrolled_count,
                doctors_supported=doctors_supported,
                limit_reached=True,
                limit_message="Master DB error while enrolling doctor into campaign.",
            )

        clinic_link = f"{base_url}/clinic/{doctor.doctor_id}/share/"

        # Prefer local (Project2) campaign message template if present; fall back to master.
        local_campaign = Campaign.objects.filter(campaign_id=campaign_id).first()
        msg_template = ""
        if local_campaign and getattr(local_campaign, "wa_addition", None):
            msg_template = str(local_campaign.wa_addition or "")
        else:
            msg_template = str(getattr(campaign, "wa_addition", "") or "")

        wa_message = _render_campaign_text_template(
            msg_template,
            doctor_name=(doctor.full_name or "Doctor").strip(),
            clinic_link=clinic_link,
            setup_link=f"{base_url}/accounts/login/",
        ).strip()

        # Fallback message if template is empty (should rarely happen)
        if not wa_message:
            wa_message = (
                f"Hi {(doctor.full_name or 'Doctor').strip()},\n"
                f"You have been added to your clinic’s patient education system.\n\n"
                f"Open your clinic dashboard:\n{clinic_link}\n\n"
                f"If you need to set/reset your password:\n{base_url}/accounts/login/\n"
            )

        whatsapp_url = master_db.build_whatsapp_deeplink(wa_number, wa_message)

        _plog(
            "field_rep_landing.redirect.whatsapp",
            whatsapp_masked=_mask_phone(wa_number),
            doctor_id=doctor.doctor_id,
            clinic_link=clinic_link,
            elapsed_ms=int((time.time() - start_ts) * 1000),
        )
        debug_info["redirect"] = {"type": "whatsapp", "whatsapp_masked": _mask_phone(wa_number)}
        return redirect(whatsapp_url)

    # Not found -> redirect to portal registration with params
    register_url = f"{base_url}/accounts/register/"
    q = {"campaign-id": campaign_id_db, "field_rep_id": downstream_field_rep_id}
    if wa_number:
        q["doctor_whatsapp_number"] = wa_number
    query = _urlencode(q)
    dest = f"{register_url}?{query}"

    _plog(
        "field_rep_landing.redirect.register",
        whatsapp_masked=_mask_phone(wa_number),
        destination=dest,
        elapsed_ms=int((time.time() - start_ts) * 1000),
    )
    debug_info["redirect"] = {"type": "register", "destination": dest}
    return redirect(dest)

def _video_title_en(video: Video) -> str:
    # best-effort English title fallback
    vlang = (
        VideoLanguage.objects.filter(video=video, language_code="en").first()
        or VideoLanguage.objects.filter(video=video).first()
    )
    return (vlang.title if vlang and vlang.title else video.code).strip()


def _cluster_name_en(cluster: VideoCluster) -> str:
    clang = (
        VideoClusterLanguage.objects.filter(video_cluster=cluster, language_code="en").first()
        or VideoClusterLanguage.objects.filter(video_cluster=cluster).first()
    )
    if clang and clang.name:
        return clang.name.strip()
    return (cluster.display_name or cluster.code or "").strip()


def _expand_selected_items_to_video_ids(items: List[Dict[str, Any]]) -> List[int]:
    video_ids: Set[int] = set()
    cluster_ids: Set[int] = set()

    for item in items:
        t = str(item.get("type") or "").lower().strip()
        try:
            _id = int(item.get("id"))
        except Exception:
            continue

        if t == "video":
            video_ids.add(_id)
        elif t == "cluster":
            cluster_ids.add(_id)

    if cluster_ids:
        cluster_video_ids = VideoClusterVideo.objects.filter(
            video_cluster_id__in=list(cluster_ids)
        ).values_list("video_id", flat=True)
        video_ids.update(list(cluster_video_ids))

    return sorted(video_ids)


def _get_or_create_brand_trigger() -> Trigger:
    """
    VideoCluster requires a Trigger. Campaign spec does not provide trigger selection,
    so we create/use a dedicated "BRAND_CAMPAIGNS" therapy + trigger cluster + trigger.
    """

    therapy, _ = TherapyArea.objects.get_or_create(
        code="BRAND_CAMPAIGNS",
        defaults={
            "display_name": "Brand Campaigns",
            "description": "Auto-created therapy area for brand campaigns",
            "sort_order": 999,
            "is_active": True,
        },
    )

    tcluster, _ = TriggerCluster.objects.get_or_create(
        code="BRAND_CAMPAIGNS",
        defaults={
            "display_name": "Brand Campaigns",
            "description": "Auto-created trigger cluster for brand campaigns",
            "language_code": "en",
            "sort_order": 999,
            "is_active": True,
        },
    )

    trigger, _ = Trigger.objects.get_or_create(
        code="BRAND_CAMPAIGN",
        defaults={
            "display_name": "Brand Campaign",
            "doctor_trigger_label": "Brand Campaign",
            "subtopic_title": "Brand Campaign",
            "navigation_pathways": "",
            "search_keywords": "brand,campaign",
            "cluster": tcluster,
            "primary_therapy": therapy,
            "sort_order": 999,
            "is_active": True,
        },
    )

    return trigger


def _generate_unique_cluster_code(name: str) -> str:
    base = slugify(name, allow_unicode=False).replace("-", "_").upper().strip("_")
    if not base:
        base = "CAMPAIGN_CLUSTER"

    base = base[:70]  # leave room for suffix
    code = base
    i = 1
    while VideoCluster.objects.filter(code=code).exists():
        suffix = f"_{i}"
        code = f"{base[: (80 - len(suffix))]}{suffix}"
        i += 1

    return code


# -----------------------------
# Pages
# -----------------------------

@publisher_required
@require_GET
def publisher_landing_page(request: HttpRequest) -> HttpResponse:
    claims = get_publisher_claims(request) or {}

    campaign_id = (
        request.GET.get("campaign-id")
        or request.GET.get("campaign_id")
        or request.session.get(SESSION_CAMPAIGN_KEY)
        or ""
    ).strip() or None

    # Capture extra values from Project1 into session (per campaign)
    campaign_meta = _capture_campaign_meta(request, campaign_id)

    # (extra safety) Remove token from URL if present
    if any(k in request.GET for k in ("token", "jwt", "access_token")):
        q = request.GET.copy()
        for k in ("token", "jwt", "access_token"):
            q.pop(k, None)
        return redirect(f"{request.path}?{q.urlencode()}") if q else redirect(request.path)

    return render(
        request,
        "publisher/publisher_landing_page.html",
        {
            "publisher": claims,
            "campaign_id": campaign_id,
            "campaign_meta": campaign_meta,
            "show_auth_links": False,
        },
    )



@publisher_required
@require_http_methods(["GET", "POST"])
def add_campaign_details(request: HttpRequest) -> HttpResponse:
    claims = get_publisher_claims(request) or {}

    campaign_id = (
        request.GET.get("campaign-id")
        or request.GET.get("campaign_id")
        or request.POST.get("campaign_id")
        or request.session.get(SESSION_CAMPAIGN_KEY)
    )
    if not campaign_id:
        return HttpResponseBadRequest("campaign-id missing")

    campaign_id = str(campaign_id).strip()
    request.session[SESSION_CAMPAIGN_KEY] = campaign_id

    # Capture Project1 meta (safe no-op if params absent)
    meta = _capture_campaign_meta(request, campaign_id)

    existing = Campaign.objects.filter(campaign_id=campaign_id).first()
    if existing and request.method == "GET":
        messages.info(request, "Campaign already has details. Redirected to edit screen.")
        return redirect(
            reverse(
                "campaign_publisher:edit_campaign_details",
                kwargs={"campaign_id": campaign_id},
            )
        )

    # MASTER values (read-only in Project2)
    try:
        master_campaign = master_db.get_campaign(campaign_id)
    except Exception:
        master_campaign = None

    doctors_supported_ro = (
        int(getattr(master_campaign, "doctors_supported", 0) or 0)
        if master_campaign is not None
        else int(meta.get("num_doctors_supported") or 0)
    )

    readonly = {
        "doctors_supported": doctors_supported_ro,
        "banner_small_url": str(getattr(master_campaign, "banner_small_url", "") or "")
        if master_campaign
        else "",
        "banner_large_url": str(getattr(master_campaign, "banner_large_url", "") or "")
        if master_campaign
        else "",
        "banner_target_url": str(getattr(master_campaign, "banner_target_url", "") or "")
        if master_campaign
        else "",
    }

    if request.method == "POST":
        form = CampaignCreateForm(request.POST)
        if form.is_valid():
            new_cluster_name = form.cleaned_data["new_video_cluster_name"]

            if VideoClusterLanguage.objects.filter(
                name__iexact=new_cluster_name
            ).exists():
                messages.error(
                    request,
                    "video cluster name already exists.  Write a different name",
                )
                return render(
                    request,
                    "publisher/add_campaign_details.html",
                    {
                        "form": form,
                        "campaign_id": campaign_id,
                        "publisher": claims,
                        "show_auth_links": False,
                        "readonly": readonly,
                    },
                )

            selected_items = json.loads(form.cleaned_data["selected_items_json"])
            video_ids = _expand_selected_items_to_video_ids(selected_items)
            if not video_ids:
                form.add_error(
                    None,
                    "Please select at least one valid video or video-cluster.",
                )
                return render(
                    request,
                    "publisher/add_campaign_details.html",
                    {
                        "form": form,
                        "campaign_id": campaign_id,
                        "publisher": claims,
                        "show_auth_links": False,
                        "readonly": readonly,
                    },
                )

            if Campaign.objects.filter(campaign_id=campaign_id).exists():
                messages.error(
                    request, "Campaign already exists. Use edit instead."
                )
                return redirect(
                    reverse(
                        "campaign_publisher:edit_campaign_details",
                        kwargs={"campaign_id": campaign_id},
                    )
                )

            publisher_sub = str(claims.get("sub") or "")
            publisher_username = str(claims.get("username") or "")
            publisher_roles = ",".join(
                [str(r) for r in (claims.get("roles") or [])]
            )

            with transaction.atomic():
                trigger = _get_or_create_brand_trigger()
                cluster_code = _generate_unique_cluster_code(new_cluster_name)

                cluster = VideoCluster.objects.create(
                    code=cluster_code,
                    display_name=new_cluster_name,
                    description="",
                    trigger=trigger,
                    sort_order=0,
                    is_published=True,
                    search_keywords=new_cluster_name,
                    is_active=True,
                )

                VideoClusterLanguage.objects.create(
                    video_cluster=cluster,
                    language_code="en",
                    name=new_cluster_name,
                )

                videos = list(
                    Video.objects.filter(id__in=video_ids).order_by("code")
                )
                for idx, v in enumerate(videos, start=1):
                    VideoClusterVideo.objects.create(
                        video_cluster=cluster, video=v, sort_order=idx
                    )

                ds_value = int(readonly.get("doctors_supported") or 0)

                # ✅ UPDATED bt_value LOGIC
                bt_value = (
                    str(readonly.get("banner_target_url") or "").strip()
                    or str(form.cleaned_data.get("banner_target_url") or "").strip()
                )

                Campaign.objects.create(
                    campaign_id=campaign_id,
                    new_video_cluster_name=new_cluster_name,
                    selection_json=form.cleaned_data["selected_items_json"],
                    doctors_supported=ds_value,
                    banner_small="",
                    banner_large="",
                    banner_target_url=bt_value,
                    start_date=form.cleaned_data["start_date"],
                    end_date=form.cleaned_data["end_date"],
                    video_cluster=cluster,
                    publisher_sub=publisher_sub,
                    publisher_username=publisher_username,
                    publisher_roles=publisher_roles,
                    email_registration=form.cleaned_data["email_registration"],
                    wa_addition=form.cleaned_data["wa_addition"],
                )

            messages.success(
                request,
                "Campaign saved. Video cluster created successfully.",
            )
            return redirect(
                f"{reverse('campaign_publisher:publisher_landing_page')}?"
                f"{urlencode({'campaign-id': campaign_id})}"
            )

    # GET (and POST invalid)
    initial = {
        "campaign_id": campaign_id,
        "selected_items_json": "[]",
        "email_registration": "",
        "wa_addition": "",
        "banner_target_url": "",  # ✅ ADDED
    }

    # Prefill from MASTER when present
    if master_campaign:
        if getattr(master_campaign, "new_video_cluster_name", ""):
            initial["new_video_cluster_name"] = (
                master_campaign.new_video_cluster_name
            )
        if getattr(master_campaign, "email_registration", ""):
            initial["email_registration"] = (
                master_campaign.email_registration
            )
        if getattr(master_campaign, "wa_addition", ""):
            initial["wa_addition"] = master_campaign.wa_addition
        if getattr(master_campaign, "banner_target_url", ""):
            initial["banner_target_url"] = (
                master_campaign.banner_target_url
            )

    form = CampaignCreateForm(initial=initial)

    return render(
        request,
        "publisher/add_campaign_details.html",
        {
            "form": form,
            "campaign_id": campaign_id,
            "publisher": claims,
            "show_auth_links": False,
            "readonly": readonly,
        },
    )

@publisher_required
@require_GET
def campaign_list(request: HttpRequest) -> HttpResponse:
    claims = get_publisher_claims(request) or {}

    q = (request.GET.get("q") or "").strip()
    rows = Campaign.objects.all().order_by("-created_at")

    if q:
        rows = rows.filter(
            Q(campaign_id__icontains=q) | Q(new_video_cluster_name__icontains=q)
        )

    return render(
        request,
        "publisher/campaign_list.html",
        {
            "publisher": claims,
            "rows": rows,
            "q": q,
            "show_auth_links": False,
        },
    )


@publisher_required
@require_http_methods(["GET", "POST"])
def edit_campaign_details(request: HttpRequest, campaign_id: str) -> HttpResponse:
    claims = get_publisher_claims(request) or {}
    campaign = get_object_or_404(Campaign, campaign_id=campaign_id)

    def _safe_file_url(fieldfile) -> str:
        try:
            if fieldfile and getattr(fieldfile, "url", None):
                return fieldfile.url
        except Exception:
            pass
        return ""

    try:
        master_campaign = master_db.get_campaign(campaign.campaign_id)
    except Exception:
        master_campaign = None

    readonly = {
        "doctors_supported": int(
            getattr(master_campaign, "doctors_supported", campaign.doctors_supported) or 0
        ),
        "banner_small_url": (
            str(getattr(master_campaign, "banner_small_url", "") or "")
            if master_campaign and getattr(master_campaign, "banner_small_url", "")
            else _safe_file_url(campaign.banner_small)
        ),
        "banner_large_url": (
            str(getattr(master_campaign, "banner_large_url", "") or "")
            if master_campaign and getattr(master_campaign, "banner_large_url", "")
            else _safe_file_url(campaign.banner_large)
        ),
        "banner_target_url": (
            str(getattr(master_campaign, "banner_target_url", "") or "")
            if master_campaign and getattr(master_campaign, "banner_target_url", "")
            else (campaign.banner_target_url or "")
        ),
    }

    if request.method == "POST":
        form = CampaignEditForm(request.POST)
        if form.is_valid():
            new_cluster_name = form.cleaned_data["new_video_cluster_name"].strip()
            selected_items = json.loads(form.cleaned_data["selected_items_json"])
            video_ids = _expand_selected_items_to_video_ids(selected_items)

            if not video_ids:
                form.add_error(
                    None,
                    "Please select at least one valid video or video-cluster.",
                )
                return render(
                    request,
                    "publisher/edit_campaign_details.html",
                    {
                        "form": form,
                        "campaign": campaign,
                        "publisher": claims,
                        "show_auth_links": False,
                        "readonly": readonly,
                    },
                )

            if campaign.video_cluster_id:
                if VideoClusterLanguage.objects.filter(
                    name__iexact=new_cluster_name
                ).exclude(video_cluster_id=campaign.video_cluster_id).exists():
                    messages.error(
                        request,
                        "video cluster name already exists.  Write a different name",
                    )
                    return render(
                        request,
                        "publisher/edit_campaign_details.html",
                        {
                            "form": form,
                            "campaign": campaign,
                            "publisher": claims,
                            "show_auth_links": False,
                            "readonly": readonly,
                        },
                    )

            with transaction.atomic():
                cluster = campaign.video_cluster

                if cluster and new_cluster_name:
                    cluster.display_name = new_cluster_name
                    cluster.search_keywords = new_cluster_name
                    cluster.save()

                    cl_en = VideoClusterLanguage.objects.filter(
                        video_cluster=cluster, language_code="en"
                    ).first()
                    if cl_en:
                        cl_en.name = new_cluster_name
                        cl_en.save()
                    else:
                        VideoClusterLanguage.objects.create(
                            video_cluster=cluster,
                            language_code="en",
                            name=new_cluster_name,
                        )

                if cluster:
                    VideoClusterVideo.objects.filter(video_cluster=cluster).delete()
                    videos = list(
                        Video.objects.filter(id__in=video_ids).order_by("code")
                    )
                    for idx, v in enumerate(videos, start=1):
                        VideoClusterVideo.objects.create(
                            video_cluster=cluster, video=v, sort_order=idx
                        )

                campaign.new_video_cluster_name = new_cluster_name
                campaign.selection_json = form.cleaned_data["selected_items_json"]
                campaign.start_date = form.cleaned_data["start_date"]
                campaign.end_date = form.cleaned_data["end_date"]
                campaign.email_registration = form.cleaned_data["email_registration"]
                campaign.wa_addition = form.cleaned_data["wa_addition"]

                # ENFORCE MASTER read-only fields (with fallback)
                campaign.doctors_supported = int(
                    readonly.get("doctors_supported") or 0
                )
                campaign.banner_target_url = (
                    str(readonly.get("banner_target_url") or "").strip()
                    or str(form.cleaned_data.get("banner_target_url") or "").strip()
                )

                campaign.save()

            messages.success(request, "Campaign updated successfully.")
            return redirect(reverse("campaign_publisher:campaign_list"))

    # GET
    form = CampaignEditForm(
        initial={
            "campaign_id": campaign.campaign_id,
            "new_video_cluster_name": campaign.new_video_cluster_name,
            "selected_items_json": campaign.selection_json or "[]",
            "start_date": campaign.start_date,
            "end_date": campaign.end_date,
            "email_registration": campaign.email_registration or "",
            "wa_addition": campaign.wa_addition or "",
            "banner_target_url": campaign.banner_target_url or "",  # ✅ ADDED
        }
    )

    return render(
        request,
        "publisher/edit_campaign_details.html",
        {
            "form": form,
            "campaign": campaign,
            "publisher": claims,
            "show_auth_links": False,
            "readonly": readonly,
        },
    )


# -----------------------------
# APIs for the add/edit screen UI
# -----------------------------

@publisher_required
@require_GET
def api_search_catalog(request: HttpRequest) -> JsonResponse:
    q = (request.GET.get("q") or "").strip()
    if not q or len(q) < 2:
        return JsonResponse({"results": []})

    videos = (
        Video.objects.filter(
            Q(code__icontains=q)
            | Q(search_keywords__icontains=q)
            | Q(languages__title__icontains=q)
        )
        .distinct()
        .order_by("code")[:20]
    )

    clusters = (
        VideoCluster.objects.filter(
            Q(code__icontains=q)
            | Q(display_name__icontains=q)
            | Q(search_keywords__icontains=q)
            | Q(languages__name__icontains=q)
        )
        .distinct()
        .order_by("code")[:20]
    )

    results: List[Dict[str, Any]] = []

    for v in videos:
        results.append(
            {
                "type": "video",
                "id": v.id,
                "code": v.code,
                "title": _video_title_en(v),
            }
        )

    for c in clusters:
        results.append(
            {
                "type": "cluster",
                "id": c.id,
                "code": c.code,
                "title": _cluster_name_en(c),
            }
        )

    return JsonResponse({"results": results})


@publisher_required
@require_POST
def api_expand_selection(request: HttpRequest) -> JsonResponse:
    try:
        payload = json.loads((request.body or b"").decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "invalid json"}, status=400)

    items = payload.get("items")
    if not isinstance(items, list):
        return JsonResponse({"error": "items must be a list"}, status=400)

    # Normalize items
    normalized: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        t = str(it.get("type") or "").lower().strip()
        if t not in ("video", "cluster"):
            continue
        try:
            _id = int(it.get("id"))
        except Exception:
            continue
        normalized.append({"type": t, "id": _id})

    video_ids = _expand_selected_items_to_video_ids(normalized)
    videos = list(Video.objects.filter(id__in=video_ids).order_by("code"))

    out = [{"id": v.id, "code": v.code, "title": _video_title_en(v)} for v in videos]
    return JsonResponse({"videos": out})


