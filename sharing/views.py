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

from .services import build_whatsapp_message_prefixes, get_catalog_json_cached


def home(request: HttpRequest) -> HttpResponse:
    # Keep a simple redirect to login
    return redirect("accounts:login")


@login_required
def doctor_share(request: HttpRequest, doctor_id: str) -> HttpResponse:
    doctor = getattr(request.user, "doctor_profile", None)
    if not doctor or doctor.doctor_id != doctor_id:
        return HttpResponseForbidden("Not allowed")

    # Force refresh once to avoid stale/empty cache during development.
    catalog_json = get_catalog_json_cached(force_refresh=True)

    # Ensure we are working with a mutable dict
    if isinstance(catalog_json, str):
        try:
            catalog_json = json.loads(catalog_json)
        except Exception:
            catalog_json = {}

    catalog_json = dict(catalog_json or {})

    # Doctor name lives on the related User
    try:
        doctor_name = (doctor.user.full_name or "").strip()
    except Exception:
        doctor_name = (request.user.full_name or "").strip()

    # Inject doctor-specific, non-cached fields
    catalog_json["doctor_id"] = doctor_id
    catalog_json["message_prefixes"] = build_whatsapp_message_prefixes(doctor_name)

    return render(
        request,
        "sharing/share.html",
        {
            "doctor": doctor,
            "clinic": getattr(doctor, "clinic", None),
            "catalog_json": catalog_json,
            "languages": LANGUAGES,
        },
    )


def patient_video(request: HttpRequest, doctor_id: str, video_code: str) -> HttpResponse:
    doctor = get_object_or_404(
        DoctorProfile.objects.select_related("clinic", "user"),
        doctor_id=doctor_id,
    )

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
            "clinic": doctor.clinic,
            "video": video,
            "vlang": vlang,
            "languages": LANGUAGES,
            "selected_lang": lang,
            "show_auth_links": False,
        },
    )


def patient_cluster(
    request: HttpRequest, doctor_id: str, cluster_code: str
) -> HttpResponse:
    doctor = get_object_or_404(
        DoctorProfile.objects.select_related("clinic", "user"),
        doctor_id=doctor_id,
    )

    lang = request.GET.get("lang", "en")
    if lang not in LANGUAGE_CODES:
        lang = "en"

    cluster = VideoCluster.objects.filter(code=cluster_code).first()
    if cluster is None and cluster_code.isdigit():
        cluster = get_object_or_404(VideoCluster, pk=int(cluster_code))
    elif cluster is None:
        cluster = get_object_or_404(VideoCluster, pk=-1)

    # Cluster title in selected language (with English fallback)
    cl_lang = (
        VideoClusterLanguage.objects.filter(
            video_cluster=cluster, language_code=lang
        ).first()
        or VideoClusterLanguage.objects.filter(
            video_cluster=cluster, language_code="en"
        ).first()
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
            "clinic": doctor.clinic,
            "cluster": cluster,
            "cluster_title": cluster_title,
            "items": items,
            "languages": LANGUAGES,
            "selected_lang": lang,
            "show_auth_links": False,
        },
    )
