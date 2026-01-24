from __future__ import annotations

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.tokens import default_token_generator
from django.db import transaction
from django.http import HttpResponseForbidden, HttpResponseServerError
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.encoding import force_bytes
from django.utils.http import urlsafe_base64_encode

from .forms import DoctorRegistrationForm, DoctorClinicDetailsForm, EmailAuthenticationForm, DoctorSetPasswordForm
from .pincode_directory import IndiaPincodeDirectoryNotReady, get_state_and_district_for_pincode
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

                "<clinic_link>": clinic_link,
                "{{clinic_link}}": clinic_link,
                "<LinkShare>": clinic_link,

                "<setup_link>": setup_link,
                "{{setup_link}}": setup_link,
                "<LinkPW>": setup_link,
            }
            for k, v in replacements.items():
                if v:
                    text = text.replace(k, v)
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


# ---------------------------------------------------------------------
# Registration (new doctor)
# ---------------------------------------------------------------------

from django.shortcuts import render
from django.urls import reverse
from django.http import HttpResponseServerError

from accounts import master_db


from django.http import HttpResponseServerError
from django.shortcuts import render
from django.urls import reverse

from django.http import HttpResponseServerError
from django.shortcuts import render
from django.urls import reverse

def register_doctor(request):
    # -----------------------------
    # GET
    # -----------------------------
    if request.method == "GET":
        form = DoctorRegistrationForm(initial=request.GET)
        return render(
            request,
            "accounts/register.html",
            {"form": form, "mode": "register"},
        )

    # -----------------------------
    # POST
    # -----------------------------
    form = DoctorRegistrationForm(request.POST, request.FILES)
    if not form.is_valid():
        return render(
            request,
            "accounts/register.html",
            {"form": form, "mode": "register"},
        )

    cd = form.cleaned_data

    email = cd["email"].strip().lower()
    whatsapp = cd["clinic_whatsapp_number"].strip()

    campaign_id = (cd.get("campaign_id") or "").strip()
    field_rep_id = (cd.get("field_rep_id") or "").strip()
    recruited_via = "FIELD_REP" if field_rep_id else "SELF"

    # --------------------------------------------------
    # 1️⃣ CHECK EXISTING DOCTOR — MASTER DB
    # --------------------------------------------------
    existing_doctor_row = master_db.find_doctor_by_email_or_whatsapp(
        email=email,
        whatsapp=whatsapp,
    )

    if existing_doctor_row:
        doctor = DoctorProfile.objects.filter(
            doctor_id=existing_doctor_row["doctor_id"]
        ).select_related("user").first()

        if doctor:
            if campaign_id:
                master_db.ensure_enrollment(
                    doctor_id=doctor.doctor_id,
                    campaign_id=campaign_id,
                    registered_by=field_rep_id or None,
                )

            _send_doctor_links_email(
                doctor,
                campaign_id=campaign_id or None,
                password_setup=True,
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
    # 2️⃣ CREATE DOCTOR — MASTER DB
    # --------------------------------------------------
    doctor_id = master_db.generate_doctor_id()

    photo_path = ""
    if cd.get("photo"):
        photo_path = cd["photo"].name

    state = (cd.get("state") or "").strip()
    district = (cd.get("district") or "").strip()

    try:
        master_db.create_doctor_with_enrollment(
            doctor_id=doctor_id,
            first_name=cd["first_name"].strip(),
            last_name=cd["last_name"].strip(),
            email=email,
            whatsapp=whatsapp,
            clinic_name=cd["clinic_name"].strip(),
            clinic_phone=cd["clinic_appointment_number"].strip(),
            clinic_address=cd["clinic_address"].strip(),
            imc_number=cd["imc_registration_number"].strip(),
            postal_code=cd["postal_code"].strip(),
            state=state or None,
            district=district or None,
            photo_path=photo_path,
            campaign_id=campaign_id or None,
            recruited_via=recruited_via,
            registered_by=field_rep_id or None,
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("[Doctor registration failed] error ->", e)
        return HttpResponseServerError(
            "Doctor registration failed. Please try again later."
        )

    # --------------------------------------------------
    # 3️⃣ FETCH DOCTOR PROFILE & SEND LINKS
    # --------------------------------------------------
    doctor = DoctorProfile.objects.filter(
        doctor_id=doctor_id
    ).select_related("user").first()

    if doctor:
        _send_doctor_links_email(
            doctor,
            campaign_id=campaign_id or None,
            password_setup=True,
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
        form = EmailAuthenticationForm(request)

    return render(request, "accounts/login.html", {"form": form})



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

        # 1) Try master DB doctor/staff accounts
        ident = None
        try:
            ident = resolve_master_doctor_identity(email)
        except Exception:
            ident = None

        if ident:
            stored = get_stored_password_for_role(ident.row, ident.role)

            password_to_send = None
            email_subject = "Your CPD in Clinic portal password"
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

            if password_to_send:
                body_lines = [
                    f"Hello {greeting_name},",
                    "",
                    "Your CPD in Clinic portal login password is:",
                    password_to_send,
                    "",
                    "Login link:",
                    _build_absolute_url(reverse("accounts:login")),
                    "",
                    "Thank you.",
                ]
                send_email_via_sendgrid(
                    subject=email_subject,
                    to_emails=[email],
                    plain_text_content="\\n".join(body_lines),
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
