from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render

from publisher.models import Campaign as PublisherCampaign

from accounts.models import DoctorProfile
from catalog.constants import LANGUAGE_CODES, LANGUAGES
from catalog.models import (
    Video,
    VideoLanguage,
    VideoCluster,
    VideoClusterLanguage,
)

from peds_edu.master_db import (
    fetch_master_doctor_row_by_id,
    master_row_to_template_context,
    build_patient_link_payload,
    sign_patient_payload,
    unsign_patient_payload,
    fetch_pe_campaign_support_for_doctor_email,
)


from .services import build_whatsapp_message_prefixes, get_catalog_json_cached


def _norm_campaign_id(raw: str) -> str:
    """Normalize UUID to 32-hex without hyphens for cross-DB comparisons."""
    return (raw or "").strip().replace("-", "").lower()


def _filter_catalog_for_campaign_bundles(*, catalog: dict, allowed_campaign_ids: set[str]) -> dict:
    """
    Hide campaign-specific bundles (publisher_campaign.video_cluster) unless the doctor is enrolled
    in that campaign.

    Keeps:
      - all default bundles (not linked to any publisher campaign)
      - campaign bundles whose campaign_id is in allowed_campaign_ids

    Also rewrites each video's bundle_codes/trigger_codes/therapy_codes so filters remain consistent.
    """
    if not isinstance(catalog, dict) or not catalog:
        return catalog

    # Load campaign -> cluster_code mapping from local DB (Project2)
    try:
        qs = (
            PublisherCampaign.objects.select_related("video_cluster")
            .only("campaign_id", "video_cluster__code")
        )
    except Exception:
        return catalog

    campaign_cluster_by_cid: dict[str, str] = {}
    all_campaign_cluster_codes: set[str] = set()

    for c in qs:
        cid = _norm_campaign_id(getattr(c, "campaign_id", ""))
        vc = getattr(c, "video_cluster", None)
        code = getattr(vc, "code", None)
        if cid and code:
            campaign_cluster_by_cid[cid] = str(code)
            all_campaign_cluster_codes.add(str(code))

    # If there are no campaign bundles, nothing to filter
    if not all_campaign_cluster_codes:
        return catalog

    allowed_campaign_ids = {(_norm_campaign_id(x) or "") for x in (allowed_campaign_ids or set()) if x}
    allowed_cluster_codes = {campaign_cluster_by_cid[cid] for cid in allowed_campaign_ids if cid in campaign_cluster_by_cid}

    bundles_in = catalog.get("bundles") or []
    if not isinstance(bundles_in, list):
        return catalog

    # Filter bundles list (do NOT mutate cached list)
    bundles_out = [
        b
        for b in bundles_in
        if isinstance(b, dict)
        and (
            (b.get("code") not in all_campaign_cluster_codes)
            or (b.get("code") in allowed_cluster_codes)
        )
    ]

    allowed_bundle_codes: set[str] = {str(b.get("code")) for b in bundles_out if b.get("code")}

    # Build meta map for recomputing trigger_codes/therapy_codes
    bundle_meta = {
        str(b.get("code")): {
            "trigger_code": b.get("trigger_code"),
            "therapy_code": b.get("therapy_code"),
        }
        for b in bundles_out
        if isinstance(b, dict) and b.get("code")
    }

    videos_in = catalog.get("videos") or []
    if not isinstance(videos_in, list):
        videos_in = []

    videos_out = []
    for v in videos_in:
        if not isinstance(v, dict):
            continue
        nv = dict(v)

        bundle_codes = [str(c) for c in (v.get("bundle_codes") or []) if str(c) in allowed_bundle_codes]
        nv["bundle_codes"] = bundle_codes

        # Recompute derived filters based on remaining bundles
        trig_codes: list[str] = []
        therapy_codes: list[str] = []
        for bc in bundle_codes:
            meta = bundle_meta.get(bc) or {}
            t = meta.get("trigger_code")
            th = meta.get("therapy_code")
            if t and t not in trig_codes:
                trig_codes.append(str(t))
            if th and th not in therapy_codes:
                therapy_codes.append(str(th))

        nv["trigger_codes"] = trig_codes
        nv["therapy_codes"] = therapy_codes

        videos_out.append(nv)

    out = dict(catalog)
    out["bundles"] = bundles_out
    out["videos"] = videos_out
    return out


def home(request: HttpRequest) -> HttpResponse:
    # Keep a simple redirect to login
    return redirect("accounts:login")


@login_required
def doctor_share(request: HttpRequest, doctor_id: str) -> HttpResponse:
    """
    Doctor/clinic landing page:
      - Authorizes based on session["master_doctor_id"] (set at login via master DB).
      - Loads display fields from master DB redflags_doctor.
      - Injects a signed doctor/clinic payload into catalog_json so JS can append it to patient links.
      - Adds PE campaign acknowledgements + banners (from MASTER DB campaign_campaign) at bottom of page.
      - Filters campaign-specific clusters (Project2-created bundles) so doctors only see bundles for their campaign(s).
    """
    session_doctor_id = request.session.get("master_doctor_id")
    if not session_doctor_id or session_doctor_id != doctor_id:
        return HttpResponseForbidden("Not allowed")

    try:
        row = fetch_master_doctor_row_by_id(doctor_id)
    except Exception:
        row = None

    if not row:
        return HttpResponseForbidden("Doctor not found")

    # Build template-friendly dicts (also normalizes state from PIN)
    doctor_ctx, clinic_ctx = master_row_to_template_context(row)
    doctor_name = ((doctor_ctx.get("user") or {}).get("full_name") or "").strip()

    # Campaign acknowledgements + banners (system_pe campaigns only)
    login_email = (getattr(request.user, "email", "") or "").strip()
    doctor_email = ((doctor_ctx.get("user") or {}).get("email") or "").strip()

    extra_emails = [
        login_email,
        doctor_email,
        str(row.get("clinic_user1_email") or "").strip(),
        str(row.get("clinic_user2_email") or "").strip(),
        str(row.get("clinic_user3_email") or "").strip(),
    ]
    phones = [
        str(doctor_ctx.get("whatsapp_number") or "").strip(),
        str(clinic_ctx.get("clinic_phone") or "").strip(),
        str(clinic_ctx.get("clinic_whatsapp_number") or "").strip(),
    ]

    try:
        pe_campaign_support = fetch_pe_campaign_support_for_doctor_email(
            doctor_email or login_email,
            extra_emails=extra_emails,
            phones=phones,
        )
    except Exception:
        pe_campaign_support = []

    # Force refresh once to avoid stale cache during development.
    catalog_json = get_catalog_json_cached(force_refresh=True)

    # Ensure we are working with a mutable dict
    if isinstance(catalog_json, str):
        try:
            catalog_json = json.loads(catalog_json)
        except Exception:
            catalog_json = {}

    catalog_json = dict(catalog_json or {})

    # Restrict campaign-specific bundles (clusters created per campaign in Project2) to doctors
    # enrolled in those campaigns. Default bundles remain visible to everyone.
    allowed_campaign_ids = {
        _norm_campaign_id(str(item.get("campaign_id") or ""))
        for item in (pe_campaign_support or [])
        if isinstance(item, dict) and item.get("campaign_id")
    }
    try:
        catalog_json = _filter_catalog_for_campaign_bundles(
            catalog=catalog_json, allowed_campaign_ids=allowed_campaign_ids
        )
    except Exception:
        # Never block the doctor landing page if filtering fails
        pass

    # Inject doctor-specific, non-cached fields
    catalog_json["doctor_id"] = doctor_id
    catalog_json["message_prefixes"] = build_whatsapp_message_prefixes(doctor_name)

    # Signed payload with all doctor/clinic display values needed by patient pages
    patient_payload = build_patient_link_payload(doctor_ctx, clinic_ctx)
    catalog_json["doctor_payload"] = sign_patient_payload(patient_payload)

    return render(
        request,
        "sharing/share.html",
        {
            "doctor": doctor_ctx,
            "clinic": clinic_ctx,
            "catalog_json": catalog_json,
            "languages": LANGUAGES,
            # Hide "Modify Clinic Details" when using master DB
            "show_modify_clinic_details": False,
            "pe_campaign_support": pe_campaign_support,
        },
    )


def patient_video(request: HttpRequest, doctor_id: str, video_code: str) -> HttpResponse:
    # Doctor/clinic display info comes from the signed payload (no DB query)
    token = (request.GET.get("d") or "").strip()
    payload = unsign_patient_payload(token) or {}

    doctor = payload.get("doctor") if isinstance(payload.get("doctor"), dict) else {}
    clinic = payload.get("clinic") if isinstance(payload.get("clinic"), dict) else {}

    # Ensure template-safe structure (doctor.user.full_name must exist)
    if not isinstance(doctor.get("user"), dict):
        doctor["user"] = {"full_name": ""}

    # Optional: trust the path doctor_id as canonical
    doctor["doctor_id"] = doctor_id
    doctor.setdefault("photo", None)

    clinic.setdefault("display_name", "")
    clinic.setdefault("clinic_phone", "")
    clinic.setdefault("clinic_whatsapp_number", "")
    clinic.setdefault("address_text", "")
    clinic.setdefault("state", "")
    clinic.setdefault("postal_code", "")

    lang = request.GET.get("lang", "en")
    if lang not in LANGUAGE_CODES:
        lang = "en"

    video = get_object_or_404(Video, code=video_code)

    vlang = (
        VideoLanguage.objects.filter(video=video, language_code=lang).first()
        or VideoLanguage.objects.filter(video=video, language_code="en").first()
    )

    return render(
        request,
        "sharing/patient_video.html",
        {
            "doctor": doctor,
            "clinic": clinic,
            "video": video,
            "vlang": vlang,
            "lang": lang,
        },
    )


def patient_bundle(request: HttpRequest, doctor_id: str, bundle_code: str) -> HttpResponse:
    token = (request.GET.get("d") or "").strip()
    payload = unsign_patient_payload(token) or {}

    doctor = payload.get("doctor") if isinstance(payload.get("doctor"), dict) else {}
    clinic = payload.get("clinic") if isinstance(payload.get("clinic"), dict) else {}

    if not isinstance(doctor.get("user"), dict):
        doctor["user"] = {"full_name": ""}

    doctor["doctor_id"] = doctor_id
    doctor.setdefault("photo", None)

    clinic.setdefault("display_name", "")
    clinic.setdefault("clinic_phone", "")
    clinic.setdefault("clinic_whatsapp_number", "")
    clinic.setdefault("address_text", "")
    clinic.setdefault("state", "")
    clinic.setdefault("postal_code", "")

    lang = request.GET.get("lang", "en")
    if lang not in LANGUAGE_CODES:
        lang = "en"

    bundle = get_object_or_404(VideoCluster, code=bundle_code)

    blang = (
        VideoClusterLanguage.objects.filter(video_cluster=bundle, language_code=lang).first()
        or VideoClusterLanguage.objects.filter(video_cluster=bundle, language_code="en").first()
    )

    videos = bundle.videos.filter(is_active=True).all()
    # NOTE: you may want language-specific titles; kept as-is.

    return render(
        request,
        "sharing/patient_bundle.html",
        {
            "doctor": doctor,
            "clinic": clinic,
            "bundle": bundle,
            "blang": blang,
            "videos": videos,
            "lang": lang,
        },
    )
