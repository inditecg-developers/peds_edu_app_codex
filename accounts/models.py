from __future__ import annotations

import re
import secrets
import string

from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin
from django.core.validators import RegexValidator
from django.db import models
from django.utils import timezone


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

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
    if not address_text:
        return None
    match = re.search(r"(\d{6})", address_text)
    return match.group(1) if match else None


# ---------------------------------------------------------------------
# Custom User
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

    # matches DB expectation
    date_joined = models.DateTimeField(default=timezone.now)

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = []

    objects = UserManager()

    def __str__(self):
        return self.email


# ---------------------------------------------------------------------
# Clinic
# ---------------------------------------------------------------------

class Clinic(models.Model):
    clinic_code = models.CharField(max_length=50, unique=True)
    display_name = models.CharField(max_length=255)
    clinic_phone = models.CharField(max_length=15)
    clinic_whatsapp_number = models.CharField(max_length=10, blank=True, null=True)
    address_text = models.TextField()
    postal_code = models.CharField(max_length=6)
    state = models.CharField(max_length=64, choices=INDIA_STATE_CHOICES)

    # REQUIRED by DB
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.display_name


# ---------------------------------------------------------------------
# Doctor Profile
# ---------------------------------------------------------------------

def default_doctor_id() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


class DoctorProfile(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name="doctor_profile"
    )

    doctor_id = models.CharField(max_length=12, unique=True, default=default_doctor_id)
    whatsapp_number = models.CharField(max_length=10, unique=True)
    imc_number = models.CharField(max_length=64)
    postal_code = models.CharField(max_length=6, blank=True, null=True)
    photo = models.ImageField(upload_to="doctor_photos/", null=True, blank=True)

    clinic = models.ForeignKey(
        Clinic,
        on_delete=models.CASCADE,
        db_column="clinic_id"
    )

    # REQUIRED by DB
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user.email} ({self.doctor_id})"
