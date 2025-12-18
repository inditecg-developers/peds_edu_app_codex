from __future__ import annotations

import logging

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.encoding import force_bytes, force_str
from django.utils.http import urlsafe_base64_decode, urlsafe_base64_encode

from .forms import DoctorRegistrationForm, EmailAuthenticationForm, DoctorSetPasswordForm
from .models import Clinic, DoctorProfile, User, extract_postal_code
from .sendgrid_utils import send_email_via_sendgrid
from .tokens import doctor_password_token

logger = logging.getLogger(__name__)


def _build_absolute_url(path: str) -> str:
    base = settings.APP_BASE_URL.rstrip("/")
    return f"{base}{path}"


def _send_doctor_links_email(doctor: DoctorProfile, *, password_setup: bool) -> None:
    clinic_link = _build_absolute_url(reverse("sharing:doctor_share", kwargs={"doctor_id": doctor.doctor_id}))

    if password_setup:
        uid = urlsafe_base64_encode(force_bytes(doctor.user.pk))
        token = doctor_password_token.make_token(doctor.user)
        password_link = _build_absolute_url(reverse("accounts:password_reset", kwargs={"uidb64": uid, "token": token}))
        subject = "Your clinic education link + set your password"
        text = (
            f"Hello Dr. {doctor.user.full_name},\n\n"
            f"Your clinic's patient education system link is:\n{clinic_link}\n\n"
            "To set your password (first time), open this link:\n"
            f"{password_link}\n\n"
            "Regards,\nPatient Education Team\n"
        )
    else:
        subject = "Your clinic patient education system link"
        text = (
            f"Hello Dr. {doctor.user.full_name},\n\n"
            f"Your clinic's patient education system link is:\n{clinic_link}\n\n"
            "Regards,\nPatient Education Team\n"
        )

    # Do not crash registration flow if email fails; log and proceed.
    try:
        send_email_via_sendgrid(doctor.user.email, subject, text)
    except Exception:
        logger.exception("Failed sending doctor links email")


def register_doctor(request: HttpRequest) -> HttpResponse:
    if request.method == "GET":
        provisional_id = DoctorProfile._meta.get_field("doctor_id").default()
        form = DoctorRegistrationForm(initial={"doctor_id": provisional_id})
        return render(request, "accounts/register.html", {"form": form})

    form = DoctorRegistrationForm(request.POST, request.FILES)
    if not form.is_valid():
        return render(request, "accounts/register.html", {"form": form})

    doctor_id = form.cleaned_data["doctor_id"].strip()
    full_name = form.cleaned_data["full_name"].strip()
    email = form.cleaned_data["email"].strip().lower()
    whatsapp_number = form.cleaned_data["whatsapp_number"].strip()
    imc_number = form.cleaned_data["imc_number"].strip()
    clinic_number = (form.cleaned_data.get("clinic_number") or "").strip()
    address_text = form.cleaned_data["address_text"].strip()
    state = form.cleaned_data["state"]
    photo = form.cleaned_data.get("photo")

    postal_code = extract_postal_code(address_text) or ""

    with transaction.atomic():
        if User.objects.filter(email=email).exists():
            form.add_error("email", "This email is already registered.")
            return render(request, "accounts/register.html", {"form": form})

        if DoctorProfile.objects.filter(doctor_id=doctor_id).exists():
            doctor_id = DoctorProfile._meta.get_field("doctor_id").default()

        user = User.objects.create_user(email=email, full_name=full_name, password=None)

        clinic_display_name = f"Dr. {full_name}"
        clinic = Clinic.objects.create(
            display_name=clinic_display_name,
            clinic_phone=clinic_number,
            address_text=address_text,
            postal_code=postal_code,
            state=state,
        )

        doctor = DoctorProfile.objects.create(
            user=user,
            doctor_id=doctor_id,
            clinic=clinic,
            whatsapp_number=whatsapp_number,
            imc_number=imc_number,
            photo=photo,
        )

    _send_doctor_links_email(doctor, password_setup=True)

    clinic_link_path = reverse("sharing:doctor_share", kwargs={"doctor_id": doctor.doctor_id})
    clinic_link = _build_absolute_url(clinic_link_path)

    return render(request, "accounts/register_success.html", {"doctor": doctor, "clinic_link": clinic_link})


def doctor_login(request: HttpRequest) -> HttpResponse:
    if request.user.is_authenticated:
        return redirect("sharing:home")

    form = EmailAuthenticationForm(request, data=request.POST or None)

    if request.method == "POST" and form.is_valid():
        user = form.get_user()
        login(request, user)
        next_url = request.GET.get("next") or reverse("sharing:home")
        return redirect(next_url)

    if request.method == "POST" and not form.is_valid():
        email = (request.POST.get("username") or "").strip().lower()
        if email:
            user = User.objects.filter(email=email).first()
            if user and not user.has_usable_password():
                _send_password_reset_email(user)
                messages.success(request, "Password setup instructions have been sent to your email address")
                form = EmailAuthenticationForm(request)

    return render(request, "accounts/login.html", {"form": form})


@login_required
def doctor_logout(request: HttpRequest) -> HttpResponse:
    logout(request)
    return redirect("accounts:login")


def _send_password_reset_email(user: User) -> None:
    print(f"[_send_password_reset_email] Function called for user ID={user.pk}")

    uid = urlsafe_base64_encode(force_bytes(user.pk))
    print(f"[_send_password_reset_email] UID generated: {uid}")

    token = doctor_password_token.make_token(user)
    print(f"[_send_password_reset_email] Token generated: {token}")

    link = _build_absolute_url(
        reverse(
            "accounts:password_reset",
            kwargs={"uidb64": uid, "token": token}
        )
    )
    print(f"[_send_password_reset_email] Password reset link generated: {link}")

    subject = "Reset your password"
    text = (
        f"Hello {user.full_name},\n\n"
        "Password reset instructions:\n"
        f"{link}\n\n"
        "If you did not request this, you can ignore this message.\n\n"
        "Regards,\nPatient Education Team\n"
    )

    print(f"[_send_password_reset_email] Email subject prepared: {subject}")
    print(f"[_send_password_reset_email] Sending email to: {user.email}")

    try:
        send_email_via_sendgrid(user.email, subject, text)
        print(f"[_send_password_reset_email] Email successfully sent to {user.email}")
    except Exception as e:
        print(f"[_send_password_reset_email] ERROR while sending email: {e}")
        logger.exception("Failed sending password reset email")


def request_password_reset(request: HttpRequest) -> HttpResponse:
    print("[request_password_reset] Function called")

    if request.method == "POST":
        print("[request_password_reset] Request method is POST")

        email = (request.POST.get("email") or "").strip().lower()
        print(f"[request_password_reset] Email received: '{email}'")

        user = User.objects.filter(email=email).first()

        if user:
            print(f"[request_password_reset] User found with ID={user.pk}, email={user.email}")
            _send_password_reset_email(user)
        else:
            print("[request_password_reset] No user found for this email")

        messages.success(
            request,
            "password reset instructions have been sent to your email address"
        )
        print("[request_password_reset] Success message added")

        print("[request_password_reset] Redirecting to login page")
        return redirect("accounts:login")

    print("[request_password_reset] Request method is not POST, rendering password request page")
    return render(request, "accounts/password_request.html")


def password_reset(request: HttpRequest, uidb64: str, token: str) -> HttpResponse:
    try:
        uid = force_str(urlsafe_base64_decode(uidb64))
        user = User.objects.get(pk=uid)
    except Exception:
        user = None

    if user is None or not doctor_password_token.check_token(user, token):
        return render(request, "accounts/password_reset_invalid.html")

    form = DoctorSetPasswordForm(user, request.POST or None)
    if request.method == "POST" and form.is_valid():
        form.save()
        messages.success(request, "Password updated. Please log in again with your new password.")
        return redirect("accounts:login")

    return render(request, "accounts/password_reset.html", {"form": form})
