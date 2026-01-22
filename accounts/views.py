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

def register_doctor(request):
    if request.method == "GET":
        initial = {}

        campaign_id = (request.GET.get("campaign-id") or request.GET.get("campaign_id") or "").strip()
        field_rep_id = (request.GET.get("field_rep_id") or request.GET.get("field-rep-id") or "").strip()

        if campaign_id:
            initial["campaign_id"] = campaign_id
        if field_rep_id:
            initial["field_rep_id"] = field_rep_id

        draft = _pop_registration_draft(request, session_key="doctor_registration_draft")
        if isinstance(draft, dict):
            initial.update(draft)

        form = DoctorRegistrationForm(initial=initial)
        return render(request, "accounts/register.html", {"form": form, "mode": "register"})

    # POST
    form = DoctorRegistrationForm(request.POST, request.FILES)
    if not form.is_valid():
        return render(request, "accounts/register.html", {"form": form, "mode": "register"})

    cd = form.cleaned_data

    campaign_id = (cd.get("campaign_id") or "").strip()
    field_rep_id = (cd.get("field_rep_id") or "").strip()

    first_name = cd["first_name"].strip()
    last_name = cd["last_name"].strip()
    full_name = f"{first_name} {last_name}".strip()

    email = cd["email"].strip().lower()
    clinic_name = cd["clinic_name"].strip()
    imc_registration_number = cd["imc_registration_number"].strip()
    clinic_appointment_number = cd["clinic_appointment_number"].strip()
    clinic_address = cd["clinic_address"].strip()
    postal_code = cd["postal_code"].strip()
    clinic_whatsapp_number = cd["clinic_whatsapp_number"].strip()
    photo = cd["photo"]

    # Compute State + District from PIN code directory
    try:
        state, district = get_state_and_district_for_pincode(postal_code)
    except IndiaPincodeDirectoryNotReady as e:
        return HttpResponseServerError(str(e))

    if not state:
        _store_registration_draft(
            request,
            session_key="doctor_registration_draft",
            draft={
                "campaign_id": campaign_id,
                "field_rep_id": field_rep_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "clinic_name": clinic_name,
                "imc_registration_number": imc_registration_number,
                "clinic_appointment_number": clinic_appointment_number,
                "clinic_address": clinic_address,
                "postal_code": postal_code,
                "clinic_whatsapp_number": clinic_whatsapp_number,
            },
        )
        return render(
            request,
            "accounts/pincode_invalid.html",
            {"return_url": reverse("accounts:register") + (f"?campaign-id={campaign_id}&field_rep_id={field_rep_id}" if campaign_id and field_rep_id else "")},
        )

    # ------------------------------------------------------------------
    # Duplicate handling (portal DB)
    # ------------------------------------------------------------------
    existing_user = User.objects.filter(email=email).first()
    if existing_user:
        existing_doctor = getattr(existing_user, "doctor_profile", None)
        if existing_doctor:
            # Best-effort: backfill enrollment in master DB for this campaign
            try:
                if campaign_id:
                    master_db.ensure_enrollment(
                        doctor_id=existing_doctor.doctor_id,
                        campaign_id=campaign_id,
                        registered_by=field_rep_id,
                    )
            except Exception:
                pass

            _send_doctor_links_email(existing_doctor, campaign_id=campaign_id or None, password_setup=True)

        return render(
            request,
            "accounts/already_registered.html",
            {
                "message": (
                    "This email address has already been registered for a doctor on this portal. "
                    "The link to login and use the system has been sent to your email. "
                    "Follow the instructions in the email to use your system."
                ),
                "login_url": reverse("accounts:login"),
            },
        )

    existing_whatsapp = (
        DoctorProfile.objects
        .select_related("user")
        .filter(whatsapp_number=clinic_whatsapp_number)
        .first()
    )
    if existing_whatsapp:
        try:
            if campaign_id:
                master_db.ensure_enrollment(
                    doctor_id=existing_whatsapp.doctor_id,
                    campaign_id=campaign_id,
                    registered_by=field_rep_id,
                )
        except Exception:
            pass

        _send_doctor_links_email(existing_whatsapp, campaign_id=campaign_id or None, password_setup=True)

        return render(
            request,
            "accounts/already_registered.html",
            {
                "message": (
                    "This WhatsApp number has already been registered for a doctor on this portal. "
                    "The link to login and use the system has been sent to your email. "
                    "Follow the instructions in the email to use your system."
                ),
                "login_url": reverse("accounts:login"),
            },
        )

    # ------------------------------------------------------------------
    # Create NEW doctor (portal DB) + insert into master DB
    # ------------------------------------------------------------------

    # Generate a doctor_id that is unique in BOTH portal DB and master DB
    doctor_id = DoctorProfile._meta.get_field("doctor_id").default()
    while DoctorProfile.objects.filter(doctor_id=doctor_id).exists() or master_db.doctor_id_exists(doctor_id):
        doctor_id = DoctorProfile._meta.get_field("doctor_id").default()

    # For clinic display name, prefer clinic_name; fallback to "Dr. <full_name>"
    clinic_display_name = clinic_name or (f"Dr. {full_name}" if full_name else "")

    recruited_via = "FIELD_REP" if field_rep_id else "SELF"

    with transaction.atomic():
        user = User.objects.create_user(
            email=email,
            full_name=full_name,
            password=None,
        )

        clinic = Clinic.objects.create(
            display_name=clinic_display_name,
            clinic_phone=clinic_appointment_number,
            clinic_whatsapp_number=clinic_whatsapp_number,
            address_text=clinic_address,
            postal_code=postal_code,
            state=state,
        )

        doctor = DoctorProfile.objects.create(
            user=user,
            doctor_id=doctor_id,
            clinic=clinic,
            whatsapp_number=clinic_whatsapp_number,
            imc_number=imc_registration_number,
            postal_code=postal_code,
            photo=photo,
        )

        # Insert into master DB (Doctor + DoctorCampaignEnrollment)
        # photo_path: store same relative path used by portal storage
        photo_path = getattr(doctor.photo, "name", "") or ""
        try:
            form.save_to_master_db(
                doctor_id=doctor_id,
                state=state or "",
                district=district or "",
                photo_path=photo_path,
                recruited_via=recruited_via,
            )
        except Exception as e:
            # Abort portal creation if master insert fails (keeps systems consistent)
            raise

    _send_doctor_links_email(doctor, campaign_id=campaign_id or None, password_setup=True)

    clinic_link_path = reverse("sharing:doctor_share", args=[doctor.doctor_id])
    clinic_link = _build_absolute_url(clinic_link_path)

    return render(
        request,
        "accounts/register_success.html",
        {
            "doctor": doctor,
            "clinic_link": clinic_link,
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
        state = get_state_for_pincode(postal_code)
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
            doctor.clinic.save(
                update_fields=[
                    "display_name",
                    "clinic_phone",
                    "clinic_whatsapp_number",
                    "address_text",
                    "postal_code",
                    "state",
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
    if request.method == "POST":
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
    if request.method == "POST":
        email = (request.POST.get("email") or "").strip().lower()
        user = User.objects.filter(email=email).first()
        if user:
            _send_password_reset_email(user)
        messages.success(
            request,
            "If the email exists in our system, a password reset link has been sent.",
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
