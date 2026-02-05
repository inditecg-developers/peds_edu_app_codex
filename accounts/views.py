from __future__ import annotations

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.tokens import default_token_generator
from django.db import transaction, connections
from django.http import HttpResponseForbidden, HttpResponseServerError
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.encoding import force_bytes
from django.utils.http import urlsafe_base64_encode

from .forms import DoctorRegistrationForm, DoctorClinicDetailsForm, EmailAuthenticationForm, DoctorSetPasswordForm
from .pincode_directory import IndiaPincodeDirectoryNotReady, get_state_and_district_for_pincode, get_state_for_pincode

from publisher.models import Campaign
from . import master_db

from .models import User, Clinic, DoctorProfile

from .sendgrid_utils import send_email_via_sendgrid

from peds_edu.master_db import (
    resolve_master_doctor_auth,
    resolve_master_doctor_identity,
    get_stored_password_for_role,
    looks_like_hash,
    generate_temporary_password,
    update_master_password,
)


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

def _build_absolute_url(path: str) -> str:
    base = (settings.SITE_BASE_URL or "").rstrip("/")
    return f"{base}{path}"


def _send_doctor_links_email(doctor: DoctorProfile, campaign_id: str | None = None, password_setup: bool = True) -> bool:
    """Send doctor/staff share link + (optional) password setup/reset link, using campaign email template if present."""
    if not doctor or not doctor.user:
        return False

    clinic_link = _build_absolute_url(reverse("sharing:doctor_share", args=[doctor.doctor_id]))
    login_link = _build_absolute_url(reverse("accounts:login"))

    setup_link = ""
    if password_setup:
        token = default_token_generator.make_token(doctor.user)
        uid = urlsafe_base64_encode(force_bytes(doctor.user.pk))
        setup_link = _build_absolute_url(reverse("accounts:password_reset", args=[uid, token]))

    # Default fallback text (in case campaign template missing)
    fallback_lines = [
        f"Hello {doctor.user.full_name or doctor.user.email},",
        "",
        "Your clinic has access to the CPD in Clinic portal.",
        "",
        f"Clinic link (doctor/staff sharing screen): {clinic_link}",
        f"Login link: {login_link}",
        "",
    ]
    if setup_link:
        fallback_lines.extend(["To set/reset your password, use the link below:", setup_link, ""])
    fallback_lines.append("Thank you.")
    fallback_body = "\n".join(fallback_lines)

    template_text = ""
    if campaign_id:
        template_text = (
            Campaign.objects.filter(campaign_id=campaign_id)
            .values_list("email_registration", flat=True)
            .first()
            or ""
        )

    if template_text.strip():
        # Reuse the same placeholder strategy as the field-rep WhatsApp renderer.
        def _render(template: str) -> str:
            text = template or ""
            replacements = {
                "<doctor.user.full_name>": doctor.user.full_name or doctor.user.email,
                "<doctor_name>": doctor.user.full_name or doctor.user.email,
                "{{doctor_name}}": doctor.user.full_name or doctor.user.email,

                "<doctor_id>": doctor.doctor_id,
                "{{doctor_id}}": doctor.doctor_id,

                "<username>": doctor.user.email,
                "{{username}}": doctor.user.email,
                "<email>": doctor.user.email,
                "{{email}}": doctor.user.email,

                "<login_link>": login_link,
                "{{login_link}}": login_link,

                "<temp_password>": "",
                "{{temp_password}}": "",
                "<password>": "",
                "{{password}}": "",

                "<clinic_link>": clinic_link,
                "{{clinic_link}}": clinic_link,
                "<LinkShare>": clinic_link,

                "<setup_link>": setup_link,
                "{{setup_link}}": setup_link,
                "<LinkPW>": setup_link,
            }
            for k, v in replacements.items():
                # Replace even if v is empty (so placeholders disappear)
                text = text.replace(k, v or "")
            return text

        body = _render(template_text).strip()
    else:
        body = fallback_body

    return send_email_via_sendgrid(
        subject="CPD in Clinic portal access",
        to_emails=[doctor.user.email],
        plain_text_content=body,
    )


def _store_registration_draft(request, *, draft: dict, session_key: str) -> None:
    """Store a draft (excluding files) in session for repopulation."""
    request.session[session_key] = draft
    request.session.modified = True


def _pop_registration_draft(request, session_key: str) -> dict | None:
    draft = request.session.pop(session_key, None)
    if draft:
        request.session.modified = True
    return draft


def _master_auth_ok(email: str, raw_password: str) -> bool:
    """Return True if the given email/password authenticates against master DB."""
    try:
        return bool(resolve_master_doctor_auth(email, raw_password))
    except Exception:
        return False


def _force_set_master_password_plaintext(*, doctor_id: str, role: str, new_raw_password: str) -> bool:
    """Last-resort fallback: store plaintext password in master DB column (so verify_password can match).

    This is only used if hashed update + verification fails, to restore login functionality.
    """
    alias = getattr(settings, "MASTER_DB_ALIAS", "master")
    table = getattr(settings, "MASTER_DOCTOR_TABLE", "redflags_doctor")
    fm = getattr(settings, "MASTER_DOCTOR_FIELD_MAP", {}) or {}

    doctor_id_col = fm.get("doctor_id") or "doctor_id"
    if role == "clinic_user1":
        pwd_col = fm.get("user1_password") or "clinic_user1_password_hash"
    elif role == "clinic_user2":
        pwd_col = fm.get("user2_password") or "clinic_user2_password_hash"
    else:
        pwd_col = fm.get("doctor_password") or "clinic_password_hash"

    try:
        with connections[alias].cursor() as cursor:
            cursor.execute(
                f"UPDATE `{table}` SET `{pwd_col}`=%s WHERE `{doctor_id_col}`=%s LIMIT 1",
                [new_raw_password, doctor_id],
            )
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------
# Registration (new doctor)
# ---------------------------------------------------------------------

import json
import logging
import re
import time
import uuid

_recruit_logger = logging.getLogger("accounts.recruitment")


def _mask_email(email: str) -> str:
    e = (email or "").strip().lower()
    if "@" not in e:
        return (e[:2] + "***") if e else ""
    user, domain = e.split("@", 1)
    user_mask = (user[:2] + "***") if len(user) >= 2 else "***"
    return f"{user_mask}@{domain}"


def _mask_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", str(phone or ""))
    if not digits:
        return ""
    if len(digits) <= 4:
        return "***"
    return f"***{digits[-4:]}"


def _get_request_id(request) -> str:
    rid = (request.META.get("HTTP_X_REQUEST_ID") or "").strip()
    return rid or uuid.uuid4().hex


def _log(event: str, *, request_id: str, level: str = "info", **fields) -> None:
    payload = {
        "ts": int(time.time()),
        "event": event,
        "request_id": request_id,
        **fields,
    }
    msg = json.dumps(payload, default=str, ensure_ascii=False)

    if level == "debug":
        _recruit_logger.debug(msg)
    elif level == "warning":
        _recruit_logger.warning(msg)
    elif level == "error":
        _recruit_logger.error(msg)
    else:
        _recruit_logger.info(msg)


def _log_exception(event: str, *, request_id: str, **fields) -> None:
    payload = {
        "ts": int(time.time()),
        "event": event,
        "request_id": request_id,
        **fields,
    }
    _recruit_logger.exception(json.dumps(payload, default=str, ensure_ascii=False))


def register_doctor(request):
    """
    Register a doctor into MASTER DB and (optionally) enroll into a campaign.

    FIX:
      - Derive state from PIN (postal_code) when possible, instead of relying on the form's
        state dropdown (which often defaults to Maharashtra and causes wrong state display later).
    """
    request_id = _get_request_id(request)
    t0 = time.time()

    _log(
        "doctor_register.enter",
        request_id=request_id,
        method=request.method,
        path=request.path,
        master_db_module=getattr(master_db, "__file__", ""),
    )

    # -----------------------------
    # GET
    # -----------------------------
    if request.method == "GET":
        # Support marketing links that use "campaign-id" instead of "campaign_id"
        initial = request.GET.copy()
        if not (initial.get("campaign_id") or "").strip():
            cid_alias = (initial.get("campaign-id") or "").strip()
            if cid_alias:
                initial["campaign_id"] = cid_alias

        # If the field-rep landing page passed a doctor WhatsApp number, prefill the registration form's
        # clinic WhatsApp field. (The registration form currently collects clinic_whatsapp_number; older
        # templates do not have a separate whatsapp_number field.)
        if not (initial.get("clinic_whatsapp_number") or "").strip():
            wa_prefill = (initial.get("doctor_whatsapp_number") or "").strip()
            if wa_prefill:
                initial["clinic_whatsapp_number"] = wa_prefill


        _log(
            "doctor_register.get",
            request_id=request_id,
            query_keys=list(request.GET.keys()),
            campaign_id=(initial.get("campaign_id") or "").strip(),
            field_rep_id=(initial.get("field_rep_id") or "").strip(),
        )
        form = DoctorRegistrationForm(initial=initial)
        return render(
            request,
            "accounts/register.html",
            {"form": form, "mode": "register"},
        )

    # -----------------------------
    # POST
    # -----------------------------
    _log(
        "doctor_register.post_received",
        request_id=request_id,
        post_keys=list(request.POST.keys()),
        has_files=bool(request.FILES),
    )

    form = DoctorRegistrationForm(request.POST, request.FILES)
    if not form.is_valid():
        try:
            errs = form.errors.get_json_data()
        except Exception:
            errs = str(form.errors)

        _log(
            "doctor_register.form_invalid",
            request_id=request_id,
            level="warning",
            errors=errs,
        )
        return render(
            request,
            "accounts/register.html",
            {"form": form, "mode": "register"},
        )

    cd = form.cleaned_data

    email = (cd.get("email") or "").strip().lower()

    # NOTE:
    # - The public DoctorRegistrationForm currently collects `clinic_whatsapp_number`.
    # - Some older/alternate templates might post `whatsapp_number` (doctor WhatsApp).
    # To avoid losing the WhatsApp number (and creating master DB rows with empty whatsapp_no),
    # we fall back to the clinic WhatsApp and to the field-rep landing-page prefill param.
    doctor_whatsapp = (cd.get("whatsapp_number") or "").strip()
    clinic_whatsapp = (cd.get("clinic_whatsapp_number") or "").strip()
    if not doctor_whatsapp:
        doctor_whatsapp = (
            (request.POST.get("doctor_whatsapp_number") or "").strip()
            or (request.GET.get("doctor_whatsapp_number") or "").strip()
            or clinic_whatsapp
        )

    # campaign_id may arrive as hidden field (campaign_id) or as query param alias (campaign-id)
    campaign_id = (
        (cd.get("campaign_id") or "")
        or (request.GET.get("campaign_id") or "")
        or (request.GET.get("campaign-id") or "")
    ).strip()
    field_rep_id = ((cd.get("field_rep_id") or "") or (request.GET.get("field_rep_id") or "")).strip()
    recruited_via = "FIELD_REP" if field_rep_id else "SELF"

    postal_code = (cd.get("postal_code") or "").strip()

    # --------------------------------------------------
    # STATE / DISTRICT (PIN-BASED OVERRIDE)
    # --------------------------------------------------
    form_state = (cd.get("state") or "").strip()
    district = (cd.get("district") or "").strip()

    inferred_state = None
    try:
        inferred_state = get_state_for_pincode(postal_code)
    except IndiaPincodeDirectoryNotReady:
        inferred_state = None
    except Exception:
        inferred_state = None

    state = inferred_state or form_state or "Maharashtra"

    _log(
        "doctor_register.cleaned",
        request_id=request_id,
        email=_mask_email(email),
        whatsapp=_mask_phone(doctor_whatsapp),
        clinic_whatsapp=_mask_phone(clinic_whatsapp),
        campaign_id=campaign_id,
        field_rep_id=field_rep_id,
        recruited_via=recruited_via,
        postal_code=postal_code,
        state=state,
        district=district,
        state_source="PIN" if inferred_state else ("FORM" if form_state else "DEFAULT"),
    )

    # --------------------------------------------------
    # 1) CHECK EXISTING DOCTOR — MASTER DB
    # --------------------------------------------------
    _log(
        "doctor_register.master_lookup_start",
        request_id=request_id,
        email=_mask_email(email),
        whatsapp=_mask_phone(doctor_whatsapp),
    )

    try:
        existing_doctor_row = master_db.find_doctor_by_email_or_whatsapp(
            email=email,
            whatsapp_no=doctor_whatsapp,
        )

    except Exception:
        _log_exception(
            "doctor_register.master_lookup_exception",
            request_id=request_id,
            email=_mask_email(email),
            whatsapp=_mask_phone(doctor_whatsapp),
        )
        return HttpResponseServerError(f"Doctor registration failed (master DB lookup). (request_id: {request_id})")

    existing_doctor_id = ""
    if existing_doctor_row:
        if isinstance(existing_doctor_row, dict):
            existing_doctor_id = str(existing_doctor_row.get("doctor_id") or "").strip()
        else:
            existing_doctor_id = str(
                getattr(existing_doctor_row, "doctor_id", "") or ""
            ).strip()

    _log(
        "doctor_register.master_lookup_result",
        request_id=request_id,
        found=bool(existing_doctor_id),
        doctor_id=existing_doctor_id,
    )

    if existing_doctor_id:
        doctor = (
            DoctorProfile.objects.filter(doctor_id=existing_doctor_id)
            .select_related("user")
            .first()
        )

        _log(
            "doctor_register.portal_doctorprofile_lookup",
            request_id=request_id,
            doctor_id=existing_doctor_id,
            found=bool(doctor),
        )

        if doctor:
            if campaign_id:
                try:
                    _log(
                        "doctor_register.ensure_enrollment_start",
                        request_id=request_id,
                        doctor_id=existing_doctor_id,
                        campaign_id=campaign_id,
                        registered_by=field_rep_id or "",
                    )
                    master_db.ensure_enrollment(
                        doctor_id=existing_doctor_id,
                        campaign_id=campaign_id,
                        registered_by=field_rep_id or "",
                    )
                    _log(
                        "doctor_register.ensure_enrollment_ok",
                        request_id=request_id,
                        doctor_id=existing_doctor_id,
                        campaign_id=campaign_id,
                    )
                except Exception:
                    _log_exception(
                        "doctor_register.ensure_enrollment_exception",
                        request_id=request_id,
                        doctor_id=existing_doctor_id,
                        campaign_id=campaign_id,
                    )

            try:
                sent = _send_doctor_links_email(
                    doctor,
                    campaign_id=campaign_id or None,
                    password_setup=True,
                )
                _log(
                    "doctor_register.email_sent_existing",
                    request_id=request_id,
                    doctor_id=existing_doctor_id,
                    sent=bool(sent),
                )
            except Exception:
                _log_exception(
                    "doctor_register.email_exception_existing",
                    request_id=request_id,
                    doctor_id=existing_doctor_id,
                )
        else:
            _log(
                "doctor_register.warning_master_exists_but_no_portal_profile",
                request_id=request_id,
                level="warning",
                doctor_id=existing_doctor_id,
                note="Doctor exists in master DB but DoctorProfile not found in portal DB.",
            )

        _log(
            "doctor_register.exit_already_registered",
            request_id=request_id,
            duration_ms=int((time.time() - t0) * 1000),
        )

        return render(
            request,
            "accounts/already_registered.html",
            {
                "message": (
                    "This doctor is already registered. "
                    "Access details have been sent to the registered email."
                ),
                "login_url": reverse("accounts:login"),
            },
        )

    # --------------------------------------------------
    # 2) CREATE DOCTOR — MASTER DB
    # --------------------------------------------------
    try:
        doctor_id = master_db.create_master_doctor_id()

    except Exception:
        _log_exception(
            "doctor_register.generate_doctor_id_exception",
            request_id=request_id,
        )
        return HttpResponseServerError(
            f"Doctor registration failed (doctor_id generation). (request_id: {request_id})"
        )

    photo_path = ""
    if cd.get("photo"):
        try:
            photo_path = cd["photo"].name
        except Exception:
            photo_path = ""

    state = state
    district = district

    _log(
        "doctor_register.master_create_start",
        request_id=request_id,
        doctor_id=doctor_id,
        email=_mask_email(email),
        whatsapp=_mask_phone(doctor_whatsapp),
        campaign_id=campaign_id,
        recruited_via=recruited_via,
        has_photo=bool(photo_path),
        state=state,
        district=district,
    )

    temp_password = master_db.generate_temporary_password(length=10)

    try:
        doctor_id = master_db.create_doctor_with_enrollment(
            doctor_id=doctor_id,
            first_name=cd["first_name"].strip(),
            last_name=(cd.get("last_name") or "").strip(),
            email=email,
            whatsapp=doctor_whatsapp,
            receptionist_whatsapp_number=clinic_whatsapp,
            clinic_name=cd["clinic_name"].strip(),
            clinic_phone=(
            (cd.get("clinic_number") or "").strip()
            or (cd.get("clinic_phone") or "").strip()
            or (cd.get("clinic_appointment_number") or "").strip()
                ),
            clinic_appointment_number=(cd.get("clinic_appointment_number") or "").strip(),
            clinic_address=cd["clinic_address"].strip(),
            imc_number=cd["imc_registration_number"].strip(),
            postal_code=postal_code,
            state=state,
            district=district,
            photo_path=photo_path,
            campaign_id=campaign_id or None,
            recruited_via=recruited_via,
            registered_by=field_rep_id or None,
            initial_password_raw=temp_password,
        )

        # Verify that the password we just stored in master DB actually works for login.
        # If it doesn't (rare; usually due to unexpected master DB schema/data), force-reset it.
        if email and temp_password and not _master_auth_ok(email, temp_password):
            try:
                update_master_password(
                    doctor_id=doctor_id,
                    role="doctor",
                    new_raw_password=temp_password,
                )
            except Exception:
                pass

            if email and temp_password and not _master_auth_ok(email, temp_password):
                _force_set_master_password_plaintext(
                    doctor_id=doctor_id,
                    role="doctor",
                    new_raw_password=temp_password,
                )

        try:
            ok = _send_master_doctor_access_email(
            doctor_id=doctor_id,
            to_email=email,
            first_name=cd["first_name"].strip(),
            last_name=(cd.get("last_name") or "").strip(),
            temp_password=temp_password,
            campaign_id=(campaign_id or None),
                )

            
            _log(
                "doctor_register.email_master_sent",
                request_id=request_id,
                doctor_id=doctor_id,
                ok=ok,
            )
        except Exception:
            _log_exception(
                "doctor_register.email_master_exception",
                request_id=request_id,
                doctor_id=doctor_id,
            )

        _log(
            "doctor_register.master_create_ok",
            request_id=request_id,
            doctor_id=doctor_id,
        )
    except Exception as e:
        _log_exception(
        "doctor_register.master_create_exception",
        request_id=request_id,
        doctor_id=doctor_id,
        error=str(e),
    )

        # 🔎 TEMP DEBUG: show exact exception on screen when debug=1
        if request.GET.get("debug") == "1" or request.POST.get("debug") == "1":
            return HttpResponseServerError(
                "Doctor registration failed.\n\n"
                f"Exception type: {type(e).__name__}\n"
                f"Exception message: {str(e)}\n"
            )

        return HttpResponseServerError(
            f"Doctor registration failed. Please try again later. (request_id: {request_id})"
        )


    _log(
        "doctor_register.exit_success",
        request_id=request_id,
        doctor_id=doctor_id,
        duration_ms=int((time.time() - t0) * 1000),
    )

    return render(
        request,
        "accounts/register_success.html",
        {
            "doctor_id": doctor_id,
            "clinic_link": _build_absolute_url(
                reverse("sharing:doctor_share", args=[doctor_id])
            ),
        },
    )

# ---------------------------------------------------------------------
# Modify clinic details (from doctor's sharing screen)
# ---------------------------------------------------------------------

@login_required
def modify_clinic_details(request, doctor_id: str):
    doctor = getattr(request.user, "doctor_profile", None)
    if not doctor or doctor.doctor_id != doctor_id:
        return HttpResponseForbidden("Not allowed.")

    session_key = f"doctor_modify_draft_{doctor_id}"

    if request.method == "GET":
        initial = {
            "doctor_id": doctor.doctor_id,
            "full_name": doctor.user.full_name,
            "email": doctor.user.email,
            "whatsapp_number": doctor.whatsapp_number,
            "clinic_number": doctor.clinic.clinic_phone if doctor.clinic else "",
            "clinic_whatsapp_number": getattr(doctor.clinic, "clinic_whatsapp_number", "") if doctor.clinic else "",
            "imc_number": doctor.imc_number,
            "postal_code": doctor.postal_code or (doctor.clinic.postal_code if doctor.clinic else ""),
            "address_text": doctor.clinic.address_text if doctor.clinic else "",
        }

        draft = _pop_registration_draft(request, session_key=session_key)
        if isinstance(draft, dict):
            initial.update(draft)

        form = DoctorClinicDetailsForm(initial=initial)
        return render(request, "accounts/register.html", {"form": form, "mode": "modify"})

    # POST
    form = DoctorClinicDetailsForm(request.POST, request.FILES)
    if not form.is_valid():
        return render(request, "accounts/register.html", {"form": form, "mode": "modify"})

    # Ensure doctor_id isn't tampered (field is readonly, but still validate)
    if (form.cleaned_data.get("doctor_id") or "") != doctor_id:
        form.add_error("doctor_id", "Doctor ID mismatch.")
        return render(request, "accounts/register.html", {"form": form, "mode": "modify"})

    full_name = form.cleaned_data.get("full_name") or ""
    email = form.cleaned_data.get("email") or ""
    whatsapp_number = form.cleaned_data.get("whatsapp_number") or ""
    clinic_number = form.cleaned_data.get("clinic_number") or ""
    clinic_whatsapp_number = form.cleaned_data.get("clinic_whatsapp_number") or ""
    imc_number = form.cleaned_data.get("imc_number") or ""
    postal_code = form.cleaned_data.get("postal_code") or ""
    address_text = form.cleaned_data.get("address_text") or ""
    new_photo = form.cleaned_data.get("photo")

    try:
        state, district = get_state_and_district_for_pincode(postal_code)
    except IndiaPincodeDirectoryNotReady as e:
        return HttpResponseServerError(str(e))

    if not state:
        _store_registration_draft(
            request,
            session_key=session_key,
            draft={
                "doctor_id": doctor_id,
                "full_name": full_name,
                "email": email,
                "whatsapp_number": whatsapp_number,
                "clinic_number": clinic_number,
                "clinic_whatsapp_number": clinic_whatsapp_number,
                "imc_number": imc_number,
                "postal_code": postal_code,
                "address_text": address_text,
            },
        )
        return render(
            request,
            "accounts/pincode_invalid.html",
            {
                "return_url": reverse("accounts:modify_clinic_details", args=[doctor_id]),
            },
        )

    # Enforce uniqueness (excluding current doctor/user)
    if User.objects.filter(email=email).exclude(pk=doctor.user.pk).exists():
        form.add_error("email", "This email address is already registered.")
        return render(request, "accounts/register.html", {"form": form, "mode": "modify"})

    if DoctorProfile.objects.filter(whatsapp_number=whatsapp_number).exclude(pk=doctor.pk).exists():
        form.add_error("whatsapp_number", "This WhatsApp number is already registered.")
        return render(request, "accounts/register.html", {"form": form, "mode": "modify"})

    clinic_display_name = f"Dr. {full_name}" if full_name else ""

    with transaction.atomic():
        # Update user
        doctor.user.full_name = full_name
        doctor.user.email = email
        doctor.user.save(update_fields=["full_name", "email"])

        # Update clinic
        if doctor.clinic:
            doctor.clinic.display_name = clinic_display_name
            doctor.clinic.clinic_phone = clinic_number
            doctor.clinic.clinic_whatsapp_number = clinic_whatsapp_number
            doctor.clinic.address_text = address_text
            doctor.clinic.postal_code = postal_code
            doctor.clinic.state = state
            doctor.clinic.district = district
            doctor.clinic.save(
                update_fields=[
                    "display_name",
                    "clinic_phone",
                    "clinic_whatsapp_number",
                    "address_text",
                    "postal_code",
                    "state",
                    "district"
                ]
            )

        # Update doctor profile
        doctor.whatsapp_number = whatsapp_number
        doctor.imc_number = imc_number
        doctor.postal_code = postal_code
        if new_photo:
            doctor.photo = new_photo
            doctor.save(update_fields=["whatsapp_number", "imc_number", "postal_code", "photo"])
        else:
            doctor.save(update_fields=["whatsapp_number", "imc_number", "postal_code"])

    messages.success(request, "Clinic details updated.")
    return redirect("sharing:doctor_share", doctor_id=doctor_id)


# ---------------------------------------------------------------------
# Auth + password reset (doctor login)
# ---------------------------------------------------------------------

def doctor_login(request):
    """
    Doctor/clinic-staff login:
      1) First tries master DB (MASTER_DB_ALIAS.redflags_doctor).
      2) If not matched, falls back to existing portal auth (publisher/staff users).
    """
    if request.method == "POST":
        # Try master DB auth first
        email = (request.POST.get("username") or "").strip().lower()  # AuthenticationForm uses "username"
        raw_password = (request.POST.get("password") or "").strip()

        if email and raw_password:
            try:
                master_auth = resolve_master_doctor_auth(email, raw_password)
            except Exception:
                master_auth = None

            if master_auth:
                # Ensure a local Django user exists for session/login_required
                user, created = User.objects.get_or_create(
                    email=master_auth.login_email,
                    defaults={"full_name": master_auth.display_name},
                )
                # Keep header name reasonably updated
                if not created and (user.full_name or "").strip() != (master_auth.display_name or "").strip():
                    user.full_name = master_auth.display_name
                    user.save(update_fields=["full_name"])

                # Log in (manual backend; user may have unusable password locally)
                login(request, user, backend="django.contrib.auth.backends.ModelBackend")

                # Store doctor_id in session for authorization in doctor_share
                request.session["master_doctor_id"] = master_auth.doctor_id
                request.session["master_login_email"] = master_auth.login_email
                request.session["master_login_role"] = master_auth.role

                return redirect("sharing:doctor_share", doctor_id=master_auth.doctor_id)

        # Fall back to existing local auth (e.g., publisher users)
        form = EmailAuthenticationForm(request, data=request.POST)
        if form.is_valid():
            user = form.get_user()
            login(request, user)
            doctor = getattr(user, "doctor_profile", None)
            if doctor:
                return redirect("sharing:doctor_share", doctor_id=doctor.doctor_id)
            return redirect("publisher:dashboard")

        messages.error(request, "Invalid login.")
    else:
        prefill = ""
        try:
            # Prefer session value (set by Forgot Password flow), fall back to query param.
            prefill = (request.session.pop("prefill_login_email", "") or "").strip()
        except Exception:
            prefill = ""

        if not prefill:
            try:
                prefill = (request.GET.get("email") or "").strip()
            except Exception:
                prefill = ""

        if prefill:
            form = EmailAuthenticationForm(request, initial={"username": prefill})
        else:
            form = EmailAuthenticationForm(request)

    return render(request, "accounts/login.html", {"form": form, "show_auth_links": False})



@login_required
def doctor_logout(request):
    # logout() flushes the session; this is just explicit
    for k in ["master_doctor_id", "master_login_email", "master_login_role"]:
        try:
            request.session.pop(k, None)
        except Exception:
            pass

    logout(request)
    messages.info(request, "Logged out.")
    return redirect("accounts:login")



def _send_password_reset_email(user: User) -> bool:
    token = default_token_generator.make_token(user)
    uid = urlsafe_base64_encode(force_bytes(user.pk))
    reset_link = _build_absolute_url(reverse("accounts:password_reset", args=[uid, token]))

    body_lines = [
        f"Hello {user.full_name or user.email},",
        "",
        "To reset your password, use the link below:",
        reset_link,
        "",
        "If you did not request this, you can ignore this email.",
        "",
        "Thank you.",
    ]

    return send_email_via_sendgrid(
        subject="Password reset",
        to_emails=[user.email],
        plain_text_content="\n".join(body_lines),
    )


def request_password_reset(request):
    """
    Doctor/clinic-staff forgot password:

    - Reads the account from master DB (redflags_doctor).
    - If the stored password is plaintext (rare/insecure), emails it as-is.
    - If the stored password is a hash (common), generates a temporary password,
      updates the master DB hash, and emails the temporary password.

    Notes:
    - This requires the master DB credentials to have UPDATE permission if hashes are used.
    - Falls back to the existing portal reset-link flow for local users (publishers/admins).
    """
    if request.method == "POST":
        email = (request.POST.get("email") or "").strip().lower()

        # Prefill login form after redirect back to /accounts/login/
        if email:
            try:
                request.session["prefill_login_email"] = email
            except Exception:
                pass

        # 1) Try master DB doctor/staff accounts
        ident = None
        try:
            ident = resolve_master_doctor_identity(email)
        except Exception:
            ident = None

        if ident:
            stored = get_stored_password_for_role(ident.row, ident.role)

            password_to_send = None
            email_subject = "Your CPD in Clinic portal login password"
            greeting_name = (ident.display_name or email).strip()

            # If the DB stores plaintext, you *can* email it (as requested).
            # If it's a hash (not retrievable), we reset it to a temporary password.
            if stored and not looks_like_hash(stored):
                password_to_send = stored
            else:
                # Reset to a temporary password and update the master DB hash
                tmp = generate_temporary_password(length=10)
                try:
                    update_master_password(
                        doctor_id=ident.doctor_id,
                        role=ident.role,
                        new_raw_password=tmp,
                    )
                    password_to_send = tmp
                except Exception:
                    # If we cannot update the master DB, do not expose details.
                    password_to_send = None

                # Safety net: ensure the new password actually authenticates.
                # If hash verification fails unexpectedly, fall back to plaintext storage.
                if password_to_send and not _master_auth_ok(email, password_to_send):
                    try:
                        update_master_password(
                            doctor_id=ident.doctor_id,
                            role=ident.role,
                            new_raw_password=password_to_send,
                        )
                    except Exception:
                        pass

                    if not _master_auth_ok(email, password_to_send):
                        _force_set_master_password_plaintext(
                            doctor_id=ident.doctor_id,
                            role=ident.role,
                            new_raw_password=password_to_send,
                        )

            if password_to_send:
                body_lines = [
                    f"Hello {greeting_name},",
                    "",
                    "Use the password below to login to the CPD in Clinic portal:",
                    "",
                    f"Password: {password_to_send}",
                    "",
                    "Login link:",
                    _build_absolute_url(reverse("accounts:login")),
                    "",
                    "If you did not request this, you can ignore this email.",
                    "",
                    "Thank you.",
                ]
                send_email_via_sendgrid(
                    subject=email_subject,
                    to_emails=[email],
                    plain_text_content="\n".join(body_lines),
                )

            # Always return a generic response (avoid account enumeration)
            messages.success(
                request,
                "If the email exists in our system, an email has been sent.",
            )
            return redirect("accounts:login")

        # 2) Fallback: existing portal user reset-link (publisher/staff)
        user = User.objects.filter(email=email).first()
        if user:
            _send_password_reset_email(user)

        messages.success(
            request,
            "If the email exists in our system, an email has been sent.",
        )
        return redirect("accounts:login")

    return render(request, "accounts/request_password_reset.html")



def password_reset(request, uidb64: str, token: str):
    user = None
    try:
        from django.utils.http import urlsafe_base64_decode
        uid = urlsafe_base64_decode(uidb64).decode()
        user = User.objects.get(pk=uid)
    except Exception:
        user = None

    if not user or not default_token_generator.check_token(user, token):
        messages.error(request, "Invalid or expired password reset link.")
        return redirect("accounts:login")

    if request.method == "POST":
        form = DoctorSetPasswordForm(user=user, data=request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Password updated. You can now login.")
            return redirect("accounts:login")
    else:
        form = DoctorSetPasswordForm(user=user)

    return render(request, "accounts/password_reset.html", {"form": form})

def _send_master_doctor_access_email(
    *,
    doctor_id: str,
    to_email: str,
    first_name: str,
    last_name: str,
    temp_password: str | None,
    campaign_id: str | None = None,
) -> bool:
    """Send campaign-specific registration email if available; fallback to default."""
    full_name = (f"{first_name} {last_name}".strip()) or to_email

    clinic_link = _build_absolute_url(reverse("sharing:doctor_share", args=[doctor_id]))
    login_link = _build_absolute_url(reverse("accounts:login"))

    def _hyphenate_uuid32(s: str) -> str:
        t = (s or "").strip().replace("-", "")
        if len(t) != 32:
            return s
        return f"{t[0:8]}-{t[8:12]}-{t[12:16]}-{t[16:20]}-{t[20:32]}"

    template_text = ""
    if campaign_id:
        try:
            cid_raw = (campaign_id or "").strip()
            cid_norm = cid_raw.replace("-", "")
            cid_h = _hyphenate_uuid32(cid_raw)

            template_text = (
                Campaign.objects.filter(campaign_id__in=[cid_raw, cid_norm, cid_h])
                .values_list("email_registration", flat=True)
                .first()
                or ""
            ).strip()
        except Exception:
            template_text = ""

    if template_text:
        replacements = {
            "{{doctor_name}}": full_name,
            "<doctor_name>": full_name,
            "{{doctor_id}}": doctor_id,
            "<doctor_id>": doctor_id,
            "{{username}}": to_email,
            "{{email}}": to_email,
            "{{temp_password}}": temp_password or "",
            "{{password}}": temp_password or "",
            "{{login_link}}": login_link,
            "{{clinic_link}}": clinic_link,
            "<clinic_link>": clinic_link,
            "<LinkShare>": clinic_link,
        }

        body = template_text
        for k, v in replacements.items():
            body = body.replace(k, v)

        body = body.strip() + "\n"
    else:
        lines = [
            f"Hello {full_name},",
            "",
            "Your CPD in Clinic portal account has been created.",
            "",
            f"Doctor ID: {doctor_id}",
            f"Login link: {login_link}",
            f"Clinic sharing link: {clinic_link}",
            "",
            f"Username: {to_email}",
        ]
        if temp_password:
            lines.extend(
                [
                    f"Temporary password: {temp_password}",
                    "",
                    "If you have trouble logging in later, use the 'Forgot Password' link on the login page.",
                ]
            )
        lines.extend(["", "Thank you."])
        body = "\n".join(lines)

    return send_email_via_sendgrid(
        subject="CPD in Clinic portal access",
        to_emails=[to_email],
        plain_text_content=body,
    )

