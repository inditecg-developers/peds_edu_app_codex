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


def _render_campaign_text_template(template: str, *, doctor_name: str, clinic_link: str, setup_link: str = "") -> str:
    text = template or ""
    replacements = {
        "<doctor.user.full_name>": doctor_name,
        "<doctor_name>": doctor_name,
        "{{doctor_name}}": doctor_name,

        "<clinic_link>": clinic_link,
        "{{clinic_link}}": clinic_link,

        "<setup_link>": setup_link,
        "{{setup_link}}": setup_link,
    }
    for k, v in replacements.items():
        if v:
            text = text.replace(k, v)
    return text



@require_http_methods(["GET", "POST"])
def field_rep_landing_page(request: HttpRequest) -> HttpResponse:
    """
    Field rep landing page with verbose print logs.

    Fixes:
      - field_rep_id may be a JOIN TABLE PK (campaign_campaignfieldrep.id), not FieldRep.id
      - campaign_id in join table is stored without hyphens; normalize before queries
      - enforce that field rep is linked to campaign via campaign_campaignfieldrep

    Logs:
      - request id, method, path
      - parsed campaign_id / field_rep_id
      - field rep resolution path (direct vs join-table)
      - campaign fetch result
      - enrollment counts and limit checks
      - POST validation, doctor lookup result
      - redirect decisions (WhatsApp vs register)
    """
    import json
    import time
    import uuid
    import traceback
    import re
    from urllib.parse import urlencode as _urlencode
    from django.db import connections

    # -------------------------
    # lightweight JSON logger
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

    def _normalize_campaign_id_for_master(raw: str) -> str:
        # Join table stores campaign_id without hyphens (32 hex)
        return (raw or "").strip().replace("-", "")

    _plog("field_rep_landing.start")

    # -------------------------
    # Parse inputs
    # -------------------------
    campaign_id = (request.GET.get("campaign-id") or request.GET.get("campaign_id") or "").strip()
    field_rep_id = (request.GET.get("field_rep_id") or request.GET.get("field-rep-id") or "").strip()

    campaign_id_db = _normalize_campaign_id_for_master(campaign_id)

    _plog(
        "field_rep_landing.params",
        campaign_id=campaign_id,
        campaign_id_db=campaign_id_db,
        field_rep_id=field_rep_id,
        query_keys=list(request.GET.keys()),
    )

    if not campaign_id or not field_rep_id:
        _plog("field_rep_landing.bad_request.missing_params")
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": FieldRepWhatsAppForm(),
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "limit_reached": True,
                "limit_message": "Missing campaign-id or field_rep_id in URL.",
            },
            status=400,
        )

    # -------------------------
    # Resolve Field Rep (MASTER DB)
    # -------------------------
    master_alias = getattr(settings, "MASTER_DB_ALIAS", "master")
    master_conn = connections[master_alias]

    # Master table names (hardcoded in settings per your latest DB)
    join_table = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_TABLE", "campaign_campaignfieldrep")
    join_pk_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_PK_COLUMN", "id")
    join_campaign_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_CAMPAIGN_COLUMN", "campaign_id")
    join_fieldrep_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_FIELD_REP_COLUMN", "field_rep_id")

    # Try multiple candidates (direct), then fallback to join-table id resolution
    lookup_candidates = [field_rep_id]

    # If SSO identity exists in session, try token sub too (optional)
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

    fr = None
    fr_resolution = {"path": None}

    # ---- (A) Direct lookup in FieldRep table via master_db.get_field_rep()
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
            tmp = None

        if tmp:
            fr = tmp
            fr_resolution = {"path": "direct", "candidate": cand}
            break

    # ---- (B) If not found, treat URL field_rep_id as JOIN TABLE PK
    join_resolved_fieldrep_id = None
    if fr is None and field_rep_id.isdigit():
        try:
            sql = (
                f"SELECT {join_fieldrep_col} "
                f"FROM {join_table} "
                f"WHERE {join_pk_col} = %s AND {join_campaign_col} = %s "
                f"LIMIT 1"
            )
            with master_conn.cursor() as cur:
                cur.execute(sql, [int(field_rep_id), campaign_id_db])
                row = cur.fetchone()

            if row:
                join_resolved_fieldrep_id = str(row[0])
                fr = master_db.get_field_rep(join_resolved_fieldrep_id)
                fr_resolution = {
                    "path": "join_pk",
                    "join_pk": field_rep_id,
                    "resolved_fieldrep_id": join_resolved_fieldrep_id,
                }
        except Exception as e:
            _plog(
                "field_rep_landing.field_rep.join_lookup_error",
                join_pk=field_rep_id,
                campaign_id_db=campaign_id_db,
                error=str(e),
                traceback=traceback.format_exc()[-2000:],
            )

    _plog(
        "field_rep_landing.field_rep.resolution",
        candidates=lookup_candidates,
        found=bool(fr),
        resolution=fr_resolution,
        is_active=(bool(fr.is_active) if fr else None),
        master_field_rep_id=(fr.id if fr else None),
        master_brand_supplied_field_rep_id=(fr.brand_supplied_field_rep_id if fr else None),
    )

    if not fr or not fr.is_active:
        _plog("field_rep_landing.unauthorized.invalid_or_inactive_field_rep")
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": FieldRepWhatsAppForm(),
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "limit_reached": True,
                "limit_message": "Invalid or inactive field rep id.",
            },
            status=401,
        )

    # ---- Enforce: FieldRep must be linked to Campaign via join table
    # (campaign_campaignfieldrep has campaign_id + field_rep_id)
    try:
        fr_id_int = int(fr.id)
        sql = (
            f"SELECT 1 FROM {join_table} "
            f"WHERE {join_campaign_col} = %s AND {join_fieldrep_col} = %s "
            f"LIMIT 1"
        )
        with master_conn.cursor() as cur:
            cur.execute(sql, [campaign_id_db, fr_id_int])
            linked = cur.fetchone() is not None
    except Exception as e:
        _plog(
            "field_rep_landing.field_rep.link_check_error",
            error=str(e),
            traceback=traceback.format_exc()[-2000:],
        )
        linked = False

    _plog(
        "field_rep_landing.field_rep.link_check",
        campaign_id_db=campaign_id_db,
        field_rep_id_resolved=(fr.id if fr else None),
        linked=linked,
    )

    if not linked:
        _plog("field_rep_landing.unauthorized.field_rep_not_linked_to_campaign")
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": FieldRepWhatsAppForm(),
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "limit_reached": True,
                "limit_message": "This field rep is not authorized for this campaign.",
            },
            status=401,
        )

    # Stable downstream field rep identifier for enrollment + register redirect
    downstream_field_rep_id = (fr.brand_supplied_field_rep_id or "").strip() or str(fr.id)

    # -------------------------
    # Fetch campaign (MASTER DB)
    # -------------------------
    try:
        # Prefer normalized campaign id (no hyphens)
        campaign = master_db.get_campaign(campaign_id_db) or master_db.get_campaign(campaign_id)
        _plog(
            "field_rep_landing.campaign.lookup",
            found=bool(campaign),
            doctors_supported=(campaign.doctors_supported if campaign else None),
            used_campaign_id=("normalized" if campaign and campaign.campaign_id == campaign_id_db else "raw_or_unknown"),
        )
    except Exception as e:
        _plog(
            "field_rep_landing.campaign.lookup_error",
            error=str(e),
            traceback=traceback.format_exc()[-2000:],
        )
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": FieldRepWhatsAppForm(),
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "limit_reached": True,
                "limit_message": "Master DB error while fetching campaign.",
            },
            status=500,
        )

    if not campaign:
        _plog("field_rep_landing.bad_request.unknown_campaign")
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": FieldRepWhatsAppForm(),
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "limit_reached": True,
                "limit_message": "Unknown campaign-id (not found in master campaign table).",
            },
            status=400,
        )

    # -------------------------
    # Enrollment count + license enforcement
    # -------------------------
    doctors_supported = int(campaign.doctors_supported or 0)

    try:
        # Prefer normalized id (matches your join table storage)
        enrolled_count_norm = master_db.count_campaign_enrollments(campaign_id_db)
        enrolled_count_raw = 0
        if campaign_id_db != campaign_id:
            enrolled_count_raw = master_db.count_campaign_enrollments(campaign_id)
        enrolled_count = max(enrolled_count_norm, enrolled_count_raw)

        _plog(
            "field_rep_landing.enrollment_count",
            campaign_id_db=campaign_id_db,
            enrolled_count_norm=enrolled_count_norm,
            enrolled_count_raw=enrolled_count_raw,
            enrolled_count=enrolled_count,
        )
    except Exception as e:
        _plog(
            "field_rep_landing.enrollment_count_error",
            error=str(e),
            traceback=traceback.format_exc()[-2000:],
        )
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": FieldRepWhatsAppForm(),
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "campaign": campaign,
                "enrolled_count": 0,
                "doctors_supported": doctors_supported,
                "limit_reached": True,
                "limit_message": "Master DB error while counting enrollments.",
            },
            status=500,
        )

    limit_reached = bool(doctors_supported and enrolled_count >= doctors_supported)
    limit_message = (
        "This campaign already has the maximum allowed doctors registered. "
        "If you wish to register more doctors, please speak to your brand manager who can answer your queries "
        "and obtain more licenses."
    )

    _plog(
        "field_rep_landing.limit_check",
        doctors_supported=doctors_supported,
        enrolled_count=enrolled_count,
        limit_reached=limit_reached,
    )

    if request.method == "GET":
        _plog("field_rep_landing.render_get", elapsed_ms=int((time.time() - start_ts) * 1000))
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": FieldRepWhatsAppForm(),
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "campaign": campaign,
                "enrolled_count": enrolled_count,
                "doctors_supported": doctors_supported,
                "limit_reached": limit_reached,
                "limit_message": limit_message,
            },
        )

    # -------------------------
    # POST: validate WhatsApp input
    # -------------------------
    form = FieldRepWhatsAppForm(request.POST)
    if not form.is_valid():
        _plog(
            "field_rep_landing.post.invalid_form",
            errors=form.errors.get_json_data(),
            elapsed_ms=int((time.time() - start_ts) * 1000),
        )
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": form,
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "campaign": campaign,
                "enrolled_count": enrolled_count,
                "doctors_supported": doctors_supported,
                "limit_reached": limit_reached,
                "limit_message": limit_message,
            },
        )

    if limit_reached:
        _plog("field_rep_landing.post.limit_reached_block", elapsed_ms=int((time.time() - start_ts) * 1000))
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": form,
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "campaign": campaign,
                "enrolled_count": enrolled_count,
                "doctors_supported": doctors_supported,
                "limit_reached": True,
                "limit_message": limit_message,
            },
        )

    wa_number = form.cleaned_data["whatsapp_number"]
    _plog("field_rep_landing.post.whatsapp_received", whatsapp_masked=_mask_phone(wa_number))

    # -------------------------
    # Check doctor exists in MASTER DB by WhatsApp
    # -------------------------
    try:
        doctor = master_db.get_doctor_by_whatsapp(wa_number)
        _plog(
            "field_rep_landing.doctor.lookup",
            found=bool(doctor),
            doctor_id=(doctor.doctor_id if doctor else None),
            doctor_email_masked=(_mask_email(doctor.email) if doctor and doctor.email else None),
        )
    except Exception as e:
        _plog(
            "field_rep_landing.doctor.lookup_error",
            error=str(e),
            traceback=traceback.format_exc()[-2000:],
        )
        return render(
            request,
            "publisher/field_rep_landing_page.html",
            {
                "form": form,
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "campaign": campaign,
                "enrolled_count": enrolled_count,
                "doctors_supported": doctors_supported,
                "limit_reached": True,
                "limit_message": "Master DB error while searching doctor.",
            },
            status=500,
        )

    base_url = getattr(settings, "PUBLIC_BASE_URL", "https://portal.cpdinclinic.co.in").rstrip("/")

    if doctor:
        # Ensure enrollment exists in master DB
        # IMPORTANT: use normalized campaign id + stable registered_by
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
            _plog(
                "field_rep_landing.enrollment.ensure_error",
                doctor_id=doctor.doctor_id,
                campaign_id=campaign_id_db,
                registered_by=registered_by,
                error=str(e),
                traceback=traceback.format_exc()[-2000:],
            )
            return render(
                request,
                "publisher/field_rep_landing_page.html",
                {
                    "form": form,
                    "campaign_id": campaign_id,
                    "field_rep_id": field_rep_id,
                    "campaign": campaign,
                    "enrolled_count": enrolled_count,
                    "doctors_supported": doctors_supported,
                    "limit_reached": True,
                    "limit_message": "Master DB error while enrolling doctor into campaign.",
                },
                status=500,
            )

        clinic_link = f"{base_url}/clinic/{doctor.doctor_id}/share/"

        wa_addition_text = _render_campaign_text_template(
            campaign.wa_addition or "",
            doctor_name=(doctor.full_name or "Doctor").strip(),
            clinic_link=clinic_link,
            setup_link="",
        ).strip()

        lines = []
        if wa_addition_text:
            lines.append(wa_addition_text)
        if campaign.new_video_cluster_name:
            lines.append(str(campaign.new_video_cluster_name).strip())
        lines.append("Link to your clinic’s patient education system")
        lines.append(clinic_link)

        whatsapp_url = master_db.build_whatsapp_deeplink(wa_number, "\n".join(lines))

        _plog(
            "field_rep_landing.redirect.whatsapp",
            whatsapp_masked=_mask_phone(wa_number),
            doctor_id=doctor.doctor_id,
            clinic_link=clinic_link,
            elapsed_ms=int((time.time() - start_ts) * 1000),
        )
        return redirect(whatsapp_url)

    # Not found -> redirect to portal registration with params
    # IMPORTANT: pass normalized campaign-id and stable field_rep_id downstream
    register_url = f"{base_url}/accounts/register/"
    query = _urlencode({"campaign-id": campaign_id_db, "field_rep_id": downstream_field_rep_id})
    dest = f"{register_url}?{query}"

    _plog(
        "field_rep_landing.redirect.register",
        whatsapp_masked=_mask_phone(wa_number),
        destination=dest,
        elapsed_ms=int((time.time() - start_ts) * 1000),
    )
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
        or request.POST.get("campaign-id")
        or request.session.get(SESSION_CAMPAIGN_KEY)
    )
    if not campaign_id:
        return HttpResponseBadRequest("Missing campaign-id")

    request.session[SESSION_CAMPAIGN_KEY] = campaign_id

    existing = Campaign.objects.filter(campaign_id=campaign_id).first()
    if existing and request.method == "GET":
        return redirect("campaign_publisher:edit_campaign_details", campaign_id=existing.id)

    # Read-only / Master-managed fields (banners + doctor-support limit)
    meta_by_campaign = request.session.get(SESSION_CAMPAIGN_META_BY_CAMPAIGN_KEY) or {}
    meta = meta_by_campaign.get(campaign_id, {}) if campaign_id else {}

    try:
        master_campaign = master_db.get_campaign(campaign_id)
    except Exception:
        master_campaign = None

    readonly = {
        "doctors_supported": (
            master_campaign.doctors_supported
            if master_campaign is not None
            else meta.get("num_doctors_supported")
        ),
        "banner_small_url": getattr(master_campaign, "banner_small_url", "") if master_campaign else "",
        "banner_large_url": getattr(master_campaign, "banner_large_url", "") if master_campaign else "",
        "banner_target_url": getattr(master_campaign, "banner_target_url", "") if master_campaign else "",
    }

    if request.method == "POST":
        form = CampaignCreateForm(request.POST)
        if form.is_valid():
            new_cluster_name = form.cleaned_data["new_video_cluster_name"]

            if VideoCluster.objects.filter(name=new_cluster_name).exists():
                form.add_error("new_video_cluster_name", "A cluster with that name already exists.")
            else:
                selected_items = json.loads(form.cleaned_data["selected_items_json"])
                video_ids = _expand_selected_items_to_video_ids(selected_items)

                if not video_ids:
                    form.add_error("selected_items_json", "Selection expanded to no videos.")
                else:
                    duplicates = (
                        ClusterSelection.objects.filter(video_id__in=video_ids)
                        .values_list("video_id", flat=True)
                        .distinct()
                    )
                    if duplicates:
                        form.add_error(
                            "selected_items_json",
                            f"Some selected videos already exist in another cluster: {sorted(list(duplicates))[:10]} ...",
                        )
                    else:
                        # Non-editable values always come from MASTER (or safe fallback if missing)
                        try:
                            ds_value = int(readonly.get("doctors_supported") or 0)
                        except Exception:
                            ds_value = 0
                        bt_value = str(readonly.get("banner_target_url") or "").strip()

                        with transaction.atomic():
                            cluster = VideoCluster.objects.create(name=new_cluster_name)
                            ClusterSelection.objects.bulk_create(
                                [ClusterSelection(cluster=cluster, video_id=v_id) for v_id in video_ids]
                            )

                            Campaign.objects.create(
                                campaign_id=campaign_id,
                                name=meta.get("name", ""),
                                company_name=meta.get("company_name", ""),
                                contact_person_name=meta.get("contact_person_name", ""),
                                contact_person_phone=meta.get("contact_person_phone", ""),
                                contact_person_email=meta.get("contact_person_email", ""),
                                doctors_supported=ds_value,
                                # Banners are stored in MASTER DB; keep local FileFields empty/non-null.
                                banner_small="",
                                banner_large="",
                                banner_target_url=bt_value,
                                start_date=form.cleaned_data["start_date"],
                                end_date=form.cleaned_data["end_date"],
                                email_registration=form.cleaned_data.get("email_registration", ""),
                                wa_addition=form.cleaned_data.get("wa_addition", ""),
                                selection_json=form.cleaned_data["selected_items_json"],
                            )

                        messages.success(request, "Campaign details saved.")
                        return redirect("campaign_publisher:campaign_list")
    else:
        initial = {
            "campaign_id": campaign_id,
            "selected_items_json": "[]",
            "email_registration": "",
            "wa_addition": "",
        }

        # Helpful defaults from MASTER, if available (still editable here)
        if master_campaign:
            if getattr(master_campaign, "new_video_cluster_name", ""):
                initial["new_video_cluster_name"] = master_campaign.new_video_cluster_name
            if getattr(master_campaign, "email_registration", ""):
                initial["email_registration"] = master_campaign.email_registration
            if getattr(master_campaign, "wa_addition", ""):
                initial["wa_addition"] = master_campaign.wa_addition

        form = CampaignCreateForm(initial=initial)

    return render(
        request,
        "publisher/add_campaign_details.html",
        {
            "campaign_id": campaign_id,
            "publisher": claims,
            "form": form,
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
def edit_campaign_details(request: HttpRequest, campaign_id: int) -> HttpResponse:
    claims = get_publisher_claims(request) or {}
    campaign = get_object_or_404(Campaign, pk=campaign_id)

    # Master-managed read-only fields (with safe fallback to local values)
    def _safe_file_url(fieldfile) -> str:
        try:
            return fieldfile.url
        except Exception:
            return ""

    try:
        master_campaign = master_db.get_campaign(campaign.campaign_id)
    except Exception:
        master_campaign = None

    readonly = {
        "doctors_supported": (
            master_campaign.doctors_supported
            if master_campaign is not None
            else campaign.doctors_supported
        ),
        "banner_small_url": (
            getattr(master_campaign, "banner_small_url", "")
            if (master_campaign and getattr(master_campaign, "banner_small_url", ""))
            else _safe_file_url(campaign.banner_small)
        ),
        "banner_large_url": (
            getattr(master_campaign, "banner_large_url", "")
            if (master_campaign and getattr(master_campaign, "banner_large_url", ""))
            else _safe_file_url(campaign.banner_large)
        ),
        "banner_target_url": (
            getattr(master_campaign, "banner_target_url", "")
            if (master_campaign and getattr(master_campaign, "banner_target_url", ""))
            else (campaign.banner_target_url or "")
        ),
    }

    if request.method == "POST":
        form = CampaignEditForm(request.POST)
        if form.is_valid():
            new_cluster_name = form.cleaned_data["new_video_cluster_name"]

            cluster = VideoCluster.objects.filter(name=campaign.new_video_cluster_name).first()
            if not cluster:
                cluster = VideoCluster.objects.create(name=new_cluster_name)
            else:
                if new_cluster_name != campaign.new_video_cluster_name and VideoCluster.objects.filter(
                    name=new_cluster_name
                ).exclude(id=cluster.id).exists():
                    form.add_error("new_video_cluster_name", "A cluster with that name already exists.")
                    return render(
                        request,
                        "publisher/edit_campaign_details.html",
                        {"campaign": campaign, "publisher": claims, "form": form, "readonly": readonly},
                    )
                cluster.name = new_cluster_name
                cluster.save()

            selected_items = json.loads(form.cleaned_data["selected_items_json"])
            video_ids = _expand_selected_items_to_video_ids(selected_items)

            if not video_ids:
                form.add_error("selected_items_json", "Selection expanded to no videos.")
            else:
                duplicates = (
                    ClusterSelection.objects.filter(video_id__in=video_ids)
                    .exclude(cluster=cluster)
                    .values_list("video_id", flat=True)
                    .distinct()
                )
                if duplicates:
                    form.add_error(
                        "selected_items_json",
                        f"Some selected videos already exist in another cluster: {sorted(list(duplicates))[:10]} ...",
                    )
                else:
                    with transaction.atomic():
                        ClusterSelection.objects.filter(cluster=cluster).delete()
                        ClusterSelection.objects.bulk_create(
                            [ClusterSelection(cluster=cluster, video_id=v_id) for v_id in video_ids]
                        )

                        campaign.new_video_cluster_name = new_cluster_name

                        # Always enforce MASTER values for non-editable fields
                        if master_campaign is not None:
                            campaign.doctors_supported = master_campaign.doctors_supported
                            if getattr(master_campaign, "banner_target_url", ""):
                                campaign.banner_target_url = master_campaign.banner_target_url

                        campaign.start_date = form.cleaned_data["start_date"]
                        campaign.end_date = form.cleaned_data["end_date"]
                        campaign.email_registration = form.cleaned_data.get("email_registration", "")
                        campaign.wa_addition = form.cleaned_data.get("wa_addition", "")
                        campaign.selection_json = form.cleaned_data["selected_items_json"]
                        campaign.save()

                    messages.success(request, "Campaign updated.")
                    return redirect("campaign_publisher:campaign_list")
    else:
        form = CampaignEditForm(
            initial={
                "campaign_id": campaign.campaign_id,
                "new_video_cluster_name": campaign.new_video_cluster_name,
                "start_date": campaign.start_date,
                "end_date": campaign.end_date,
                "email_registration": campaign.email_registration,
                "wa_addition": campaign.wa_addition,
                "selected_items_json": campaign.selection_json or "[]",
            }
        )

    return render(
        request,
        "publisher/edit_campaign_details.html",
        {
            "campaign": campaign,
            "publisher": claims,
            "form": form,
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
