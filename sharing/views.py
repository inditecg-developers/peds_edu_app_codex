from __future__ import annotations

import json
import logging
from typing import Any, Dict

from django.conf.global_settings import LANGUAGES
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, JsonResponse, HttpResponseForbidden
from django.shortcuts import render
from django.urls import reverse

from peds_edu.catalog_cache import get_catalog_json_cached
from peds_edu.master_db import (
    fetch_master_doctor_row_by_id,
    master_row_to_template_context,
    fetch_pe_campaign_support_for_doctor_email,
)
from peds_edu.patient_payload import build_patient_link_payload, sign_patient_payload
from sharing.message_prefix import build_whatsapp_message_prefixes

logger = logging.getLogger(__name__)


@login_required
def doctor_share(request: HttpRequest, doctor_id: str) -> HttpResponse:
    """
    Doctor/clinic landing page:
      - Authorizes based on session["master_doctor_id"] (set at login via master DB).
      - Loads display fields from master DB redflags_doctor.
      - Injects a signed doctor/clinic payload into catalog_json so JS can append it to patient links.
      - Adds PE campaign acknowledgements + banners (from MASTER DB campaign_campaign) at bottom of page.
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


@login_required
def patient_video(request: HttpRequest) -> HttpResponse:
    """
    Patient video page: reads signed 'd' payload from query, merges it into template context.
    """
    payload = request.GET.get("d", "")
    # This view is unchanged in your codebase; keep as-is if you already have it.
    # (Leaving existing implementation below.)
    from sharing.patient_views import patient_video as impl  # type: ignore

    return impl(request)


@login_required
def api_video_list(request: HttpRequest) -> JsonResponse:
    """
    JSON list of videos (used by JS). This view is unchanged in your codebase; keep as-is if you already have it.
    """
    from sharing.api import api_video_list as impl  # type: ignore

    return impl(request)
