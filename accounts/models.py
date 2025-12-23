from __future__ import annotations

import re
import secrets
import string

from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin
from django.core.validators import RegexValidator
from django.db import models


INDIA_STATES_AND_UTS = [
    "Andhra Pradesh",
    "Arunachal Pradesh",
    "Assam",
    "Bihar",
    "Chhattisgarh",
    "Goa",
    "Gujarat",
    "Haryana",
    "Himachal Pradesh",
    "Jharkhand",
    "Karnataka",
    "Kerala",
    "Madhya Pradesh",
    "Maharashtra",
    "Manipur",
    "Meghalaya",
    "Mizoram",
    "Nagaland",
    "Odisha",
    "Punjab",
    "Rajasthan",
    "Sikkim",
    "Tamil Nadu",
    "Telangana",
    "Tripura",
    "Uttar Pradesh",
    "Uttarakhand",
    "West Bengal",
    "Andaman and Nicobar Islands",
    "Chandigarh",
    "Dadra and Nagar Haveli and Daman and Diu",
    "Delhi",
    "Jammu and Kashmir",
    "Ladakh",
    "Lakshadweep",
    "Puducherry",
]

INDIA_STATE_CHOICES = [(s, s) for s in INDIA_STATES_AND_UTS]


def extract_postal_code(address_text: str) -> str | None:
    """Legacy helper: extract a 6-digit PIN from a free-text address."""
    if not address_text:
        return None
    match = re.search(r"(\d{6})", address_text)
    return match.group(1) if match else None


# ---------------------------------------------------------------------
# Custom User (email login)
# ---------------------------------------------------------------------

class UserManager(BaseUserManager):
    def create_user(self, email, full_name="", password=None, **extra_fields):
        if not email:
            raise ValueError("The Email must be set")
        email = self.normalize_email(email)
        user = self.model(email=email, full_name=full_name, **extra_fields)
        if password:
            user.set_password(password)
        else:
            user.set_unusable_password()
        user.save(using=self._db)
        return user

    def create_superuser(self, email, full_name="", password=None, **extra_fields):
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("is_superuser", True)
        return self.create_user(email, full_name, password, **extra_fields)


class User(AbstractBaseUser, PermissionsMixin):
    email = models.EmailField(unique=True)
    full_name = models.CharField(max_length=255, blank=True)
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = []

    objects = UserManager()

    def __str__(self):
        return self.email


# ---------------------------------------------------------------------
# Clinic + DoctorProfile
# ---------------------------------------------------------------------

class Clinic(models.Model):
    display_name = models.CharField(max_length=255, blank=True)
    clinic_phone = models.CharField(
        max_length=20,
        blank=True,
        validators=[RegexValidator(r"^\d{6,15}$", "Enter a valid contact number (digits only).")],
    )
    clinic_whatsapp_number = models.CharField(
        max_length=10,
        blank=True,
        validators=[RegexValidator(r"^\d{10}$", "Enter a 10-digit WhatsApp number (without country code).")],
    )
    address_text = models.TextField()
    postal_code = models.CharField(max_length=6, blank=True)
    state = models.CharField(max_length=64, choices=INDIA_STATE_CHOICES)

    def __str__(self):
        return self.display_name or f"Clinic #{self.pk}"


def default_doctor_id() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


class DoctorProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="doctor_profile")
    doctor_id = models.CharField(max_length=20, unique=True, default=default_doctor_id)
    clinic = models.ForeignKey(Clinic, on_delete=models.SET_NULL, null=True, blank=True)

    whatsapp_number = models.CharField(
        max_length=10,
        unique=True,
        validators=[RegexValidator(r"^\d{10}$", "Enter a 10-digit WhatsApp number (without country code).")],
    )
    imc_number = models.CharField(max_length=64)
    postal_code = models.CharField(
        max_length=6,
        blank=True,
        validators=[RegexValidator(r"^\d{6}$", "Enter a valid 6-digit PIN code.")],
    )
    photo = models.ImageField(upload_to="doctor_photos/", null=True, blank=True)

    def __str__(self):
        return f"{self.user.full_name or self.user.email} ({self.doctor_id})"


