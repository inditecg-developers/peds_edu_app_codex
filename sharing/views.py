from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render

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
)


from .services import build_whatsapp_message_prefixes, get_catalog_json_cached


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
    """
    session_doctor_id = request.session.get("master_doctor_id")
    if not session_doctor_id or session_doctor_id != doctor_id:
        # Optional legacy fallback (if you still allow portal DB doctors):
        # doctor = getattr(request.user, "doctor_profile", None)
        # if doctor and doctor.doctor_id == doctor_id:
        #     ...
        return HttpResponseForbidden("Not allowed")

    row = None
    try:
        row = fetch_master_doctor_row_by_id(doctor_id)
    except Exception:
        row = None

    if not row:
        return HttpResponseForbidden("Doctor not found")

    # Build template-friendly dicts
    doctor_ctx, clinic_ctx = master_row_to_template_context(row)
    doctor_name = ((doctor_ctx.get("user") or {}).get("full_name") or "").strip()

    # Force refresh once to avoid stale cache during development.
    catalog_json = get_catalog_json_cached(force_refresh=True)

    # Ensure we are working with a mutable dict
    if isinstance(catalog_json, str):
        try:
            catalog_json = json.loads(catalog_json)
        except Exception:
            catalog_json = {}

    catalog_json = dict(catalog_json or {})

    # Inject doctor-specific, non-cached fields
    catalog_json["doctor_id"] = doctor_id
    catalog_json["message_prefixes"] = build_whatsapp_message_prefixes(doctor_name)

    # NEW: signed payload with all doctor/clinic display values needed by patient pages
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
            # Hide "Modify Clinic Details" when using master DB (optional template change below)
            "show_modify_clinic_details": False,
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
            "languages": LANGUAGES,
            "selected_lang": lang,
            "show_auth_links": False,
        },
    )


def patient_cluster(request: HttpRequest, doctor_id: str, cluster_code: str) -> HttpResponse:
    # Doctor/clinic display info comes from the signed payload (no DB query)
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