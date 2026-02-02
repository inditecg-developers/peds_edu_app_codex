from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.db import connections

from catalog.constants import LANGUAGE_CODES, LANGUAGES
from catalog.models import Video, VideoLanguage, VideoCluster, VideoClusterLanguage

from peds_edu.master_db import (
    fetch_master_doctor_row_by_id,
    master_row_to_template_context,
    build_patient_link_payload,
    sign_patient_payload,
    unsign_patient_payload,
    fetch_pe_campaign_support_for_doctor_email,
)

from .services import build_whatsapp_message_prefixes, get_catalog_json_cached


# -----------------------
# Required by sharing/urls.py
# -----------------------
def home(request: HttpRequest) -> HttpResponse:
    # Keep behaviour minimal + safe
    return redirect("accounts:login")


# -----------------------
# Campaign bundle helpers (read-only)
# -----------------------
def _fetch_all_campaign_bundle_codes() -> set[str]:
    try:
        with connections["default"].cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT vc.code
                FROM publisher_campaign pc
                JOIN catalog_videocluster vc ON vc.id = pc.video_cluster_id
                WHERE pc.video_cluster_id IS NOT NULL
                """
            )
            rows = cur.fetchall()
        return {str(r[0]).strip() for r in (rows or []) if r and r[0]}
    except Exception:
        return set()


def _fetch_allowed_bundle_codes_for_campaigns(campaign_ids: list[str]) -> set[str]:
    ids = [str(c).strip().replace("-", "") for c in (campaign_ids or []) if str(c).strip()]
    if not ids:
        return set()

    placeholders = ", ".join(["%s"] * len(ids))
    sql = f"""
        SELECT DISTINCT vc.code
        FROM publisher_campaign pc
        JOIN catalog_videocluster vc ON vc.id = pc.video_cluster_id
        WHERE REPLACE(pc.campaign_id, '-', '') IN ({placeholders})
          AND pc.video_cluster_id IS NOT NULL
    """

    try:
        with connections["default"].cursor() as cur:
            cur.execute(sql, ids)
            rows = cur.fetchall()
        return {str(r[0]).strip() for r in (rows or []) if r and r[0]}
    except Exception:
        return set()


@login_required
def doctor_share(request: HttpRequest, doctor_id: str) -> HttpResponse:
    session_doctor_id = request.session.get("master_doctor_id")
    if not session_doctor_id or session_doctor_id != doctor_id:
        return HttpResponseForbidden("Not allowed")

    row = fetch_master_doctor_row_by_id(doctor_id)
    if not row:
        return HttpResponseForbidden("Doctor not found")

    doctor_ctx, clinic_ctx = master_row_to_template_context(row)
    doctor_name = ((doctor_ctx.get("user") or {}).get("full_name") or "").strip()

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

    catalog_json = get_catalog_json_cached(force_refresh=True)
    if isinstance(catalog_json, str):
        try:
            catalog_json = json.loads(catalog_json)
        except Exception:
            catalog_json = {}
    catalog_json = dict(catalog_json or {})

    catalog_json["doctor_id"] = doctor_id
    catalog_json["message_prefixes"] = build_whatsapp_message_prefixes(doctor_name)

    patient_payload = build_patient_link_payload(doctor_ctx, clinic_ctx)
    catalog_json["doctor_payload"] = sign_patient_payload(patient_payload)

    # ------------------------------------------------------------------
    # Campaign-specific bundle filtering
    # ------------------------------------------------------------------
    all_campaign_bundle_codes = _fetch_all_campaign_bundle_codes()

    allowed_campaign_ids = [
        str(item.get("campaign_id"))
        for item in (pe_campaign_support or [])
        if isinstance(item, dict) and item.get("campaign_id")
    ]
    allowed_bundle_codes = _fetch_allowed_bundle_codes_for_campaigns(allowed_campaign_ids)

    if all_campaign_bundle_codes and isinstance(catalog_json.get("bundles"), list):
        filtered_bundles = []
        allowed_video_ids = set()

        for b in catalog_json.get("bundles", []):
            if not isinstance(b, dict):
                continue
            bcode = str(b.get("code") or "").strip()
            if not bcode:
                continue

            # keep default bundles OR allowed campaign bundles
            if bcode not in all_campaign_bundle_codes or bcode in allowed_bundle_codes:
                filtered_bundles.append(b)
                for vid in (b.get("video_codes") or []):
                    if vid:
                        allowed_video_ids.add(str(vid))

        catalog_json["bundles"] = filtered_bundles

        # ✅ CRITICAL FIX:
        # videos payload uses "id" (video code), not "code".:contentReference[oaicite:4]{index=4}
        if isinstance(catalog_json.get("videos"), list):
            catalog_json["videos"] = [
                v for v in catalog_json.get("videos", [])
                if isinstance(v, dict) and str(v.get("id") or "").strip() in allowed_video_ids
            ]

    return render(
        request,
        "sharing/share.html",
        {
            "doctor": doctor_ctx,
            "clinic": clinic_ctx,
            "catalog_json": catalog_json,
            "languages": LANGUAGES,
            "show_modify_clinic_details": False,
            "pe_campaign_support": pe_campaign_support,
        },
    )


def patient_video(request: HttpRequest, doctor_id: str, video_code: str) -> HttpResponse:
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
            "selected_lang": lang,
            "languages": LANGUAGES,
            "show_auth_links": False,
        },
    )


def patient_cluster(request: HttpRequest, doctor_id: str, cluster_code: str) -> HttpResponse:
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

    cluster = VideoCluster.objects.filter(code=cluster_code).first()
    if cluster is None and cluster_code.isdigit():
        cluster = get_object_or_404(VideoCluster, pk=int(cluster_code))
    elif cluster is None:
        cluster = get_object_or_404(VideoCluster, pk=-1)

    cl_lang = (
        VideoClusterLanguage.objects.filter(video_cluster=cluster, language_code=lang).first()
        or VideoClusterLanguage.objects.filter(video_cluster=cluster, language_code="en").first()
    )
    cluster_title = cl_lang.name if cl_lang else cluster.code

    try:
        videos = cluster.videos.all().order_by("sort_order", "id")
    except Exception:
        videos = cluster.videos.all().order_by("id")

    items = []
    for v in videos:
        vlang = (
            VideoLanguage.objects.filter(video=v, language_code=lang).first()
            or VideoLanguage.objects.filter(video=v, language_code="en").first()
        )
        items.append(
            {
                "video": v,
                "title": (vlang.title if vlang else v.code),
                "url": (vlang.youtube_url if vlang else ""),
            }
        )

    return render(
        request,
        "sharing/patient_cluster.html",
        {
            "doctor": doctor,
            "clinic": clinic,
            "cluster": cluster,
            "cluster_title": cluster_title,
            "items": items,
            "languages": LANGUAGES,
            "selected_lang": lang,
            "show_auth_links": False,
        },
    )
