"""Django settings for peds_edu.

This project is designed to be deployed on AWS Ubuntu with MySQL.
Configuration is done primarily via environment variables.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv

from .aws_secrets import get_secret_string  # Optional fallback for secrets

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


SECRET_KEY = env("DJANGO_SECRET_KEY", "dev-insecure-change-me")
DEBUG = env("DJANGO_DEBUG", "1") == "1"

ALLOWED_HOSTS = [h.strip() for h in env("ALLOWED_HOSTS", "*").split(",") if h.strip()]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "accounts.apps.AccountsConfig",
    "catalog.apps.CatalogConfig",
    "sharing.apps.SharingConfig",
    "publisher.apps.PublisherConfig",
    "sso.apps.SsoConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "peds_edu.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "sharing.context_processors.clinic_branding",
            ],
        },
    },
]

WSGI_APPLICATION = "peds_edu.wsgi.application"

# ---------------- DATABASE ----------------
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",  # Force MySQL
        "NAME": env("DB_NAME", "peds_edu"),
        "USER": env("DB_USER", "peds_edu"),
        "PASSWORD": env("DB_PASSWORD", "Bv9ALOgzFszxDYso"),
        "HOST": env("DB_HOST", "35.154.221.92"),
        "PORT": env("DB_PORT", "3306"),
        "OPTIONS": {"charset": "utf8mb4"},
    }
}

# ---------------------------------------------------------------------
# MASTER FORMS DB (Project1 master DB) - new-forms-rds
# ---------------------------------------------------------------------

MASTER_DB_ALIAS = "master"

# ❌ Do NOT use env or secrets
MASTER_DB_ENGINE = "django.db.backends.mysql"
MASTER_DB_NAME = "YOUR_DATABASE_NAME"
MASTER_DB_USER = "root"
MASTER_DB_PASSWORD = "Hemsod-vytsew-7qypxa"
MASTER_DB_HOST = "new-forms-rds.cbnobb8kfeuq.ap-south-1.rds.amazonaws.com"
MASTER_DB_PORT = "3306"

# Table/column config (leave as-is unless schema differs)
MASTER_DB_DOCTOR_TABLE = "Doctor"
MASTER_DB_DOCTOR_ID_COLUMN = "doctor_id"
MASTER_DB_DOCTOR_WHATSAPP_COLUMN = "whatsapp_no"

MASTER_DB_ENROLLMENT_TABLE = "DoctorCampaignEnrollment"
MASTER_DB_ENROLLMENT_DOCTOR_COLUMN = "doctor_id"
MASTER_DB_ENROLLMENT_CAMPAIGN_COLUMN = "campaign_id"
MASTER_DB_ENROLLMENT_REGISTERED_BY_COLUMN = "registered_by_id"


if MASTER_DB_NAME and MASTER_DB_USER and MASTER_DB_PASSWORD and MASTER_DB_HOST:
    DATABASES[MASTER_DB_ALIAS] = {
        "ENGINE": MASTER_DB_ENGINE,
        "NAME": MASTER_DB_NAME,
        "USER": MASTER_DB_USER,
        "PASSWORD": MASTER_DB_PASSWORD,
        "HOST": MASTER_DB_HOST,
        "PORT": MASTER_DB_PORT,
    }

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

AUTH_USER_MODEL = "accounts.User"

LANGUAGE_CODE = "en"
LANGUAGES = [
    ("en", "English"),
    ("hi", "Hindi"),
    ("te", "Telugu"),
    ("ml", "Malayalam"),
    ("mr", "Marathi"),
    ("kn", "Kannada"),
    ("ta", "Tamil"),
    ("bn", "Bengali"),
]

TIME_ZONE = env("DJANGO_TIME_ZONE", "Asia/Kolkata")
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

MEDIA_URL = env("MEDIA_URL", "/media/")
MEDIA_ROOT = Path(env("MEDIA_ROOT", "/home/ubuntu/patient-portal-media")).resolve()

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ---------------- SESSIONS ----------------
SESSION_COOKIE_AGE = int(env("SESSION_COOKIE_AGE_SECONDS", str(60 * 60 * 24 * 90)))
SESSION_EXPIRE_AT_BROWSER_CLOSE = False
SESSION_SAVE_EVERY_REQUEST = True

# ---------------- SECURITY ----------------
CSRF_COOKIE_SECURE = env("CSRF_COOKIE_SECURE", "0") == "1"
SESSION_COOKIE_SECURE = env("SESSION_COOKIE_SECURE", "0") == "1"
SECURE_SSL_REDIRECT = env("SECURE_SSL_REDIRECT", "0") == "1"

# ---------------- APP BASE URL ----------------
APP_BASE_URL = env("APP_BASE_URL", "https://portal.cpdinclinic.co.in").rstrip("/")
SITE_BASE_URL = APP_BASE_URL

# ---------------- EMAIL / SENDGRID ----------------

def _extract_sendgrid_key_from_secret(secret_raw: str) -> str:
    raw = (secret_raw or "").strip()
    if not raw:
        return ""

    if raw.startswith("{") and raw.endswith("}"):
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                for k in (
                    "SendGrid_email",
                    "sendgrid_email",
                    "SENDGRID_API_KEY",
                    "sendgrid_api_key",
                    "api_key",
                    "apikey",
                    "key",
                ):
                    v = obj.get(k)
                    if isinstance(v, str) and v.strip():
                        return v.strip()
        except Exception:
            pass

    return raw


SENDGRID_API_KEY = env("SENDGRID_API_KEY", "").strip()
if not SENDGRID_API_KEY:
    secret_raw = (get_secret_string("SendGrid_API", region_name="ap-south-1") or "").strip()
    SENDGRID_API_KEY = _extract_sendgrid_key_from_secret(secret_raw)

SENDGRID_FROM_EMAIL = env("SENDGRID_FROM_EMAIL", "products@inditech.co.in").strip()
EMAIL_BACKEND_MODE = env("EMAIL_BACKEND_MODE", "smtp").strip().lower()

if EMAIL_BACKEND_MODE == "smtp":
    EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
else:
    EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"

EMAIL_HOST = env("EMAIL_HOST", "smtp.sendgrid.net")
EMAIL_PORT = int(env("EMAIL_PORT", "587"))
EMAIL_USE_TLS = env("EMAIL_USE_TLS", "1") == "1"
EMAIL_USE_SSL = env("EMAIL_USE_SSL", "0") == "1"
EMAIL_HOST_USER = env("EMAIL_HOST_USER", "apikey")
EMAIL_HOST_PASSWORD = env("EMAIL_HOST_PASSWORD", SENDGRID_API_KEY)
DEFAULT_FROM_EMAIL = env("DEFAULT_FROM_EMAIL", SENDGRID_FROM_EMAIL)

# ---------------- CACHE ----------------
REDIS_URL = os.getenv("REDIS_URL")
if REDIS_URL:
    CACHES = {
        "default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": REDIS_URL,
            "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
            "TIMEOUT": int(env("CACHE_DEFAULT_TIMEOUT_SECONDS", "3600")),
        }
    }
else:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "peds-edu-locmem",
            "TIMEOUT": int(env("CACHE_DEFAULT_TIMEOUT_SECONDS", "3600")),
        }
    }

CATALOG_CACHE_SECONDS = int(env("CATALOG_CACHE_SECONDS", str(60 * 60)))

# ---------------- LOGGING ----------------
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": "INFO"},
}

import os

# ---------------------------------------------------------------------
# SSO consume configuration (Project2)
# ---------------------------------------------------------------------

SSO_USE_ENV = False  # TEMP: set True later when you can configure server env vars


def _sso_setting(name: str, default):
    if SSO_USE_ENV:
        return os.getenv(name, default)
    return default


SSO_EXPECTED_ISSUER = _sso_setting("SSO_EXPECTED_ISSUER", "project1")
SSO_EXPECTED_AUDIENCE = _sso_setting("SSO_EXPECTED_AUDIENCE", "project2")

SSO_SHARED_SECRET = _sso_setting(
    "SSO_SHARED_SECRET",
    _sso_setting("PUBLISHER_SSO_SHARED_SECRET", "CHANGE-ME-TO-A-LONG-RANDOM-STRING"),
)

SSO_SESSION_AGE_SECONDS = int(_sso_setting("SSO_SESSION_AGE_SECONDS", "3600"))
SSO_SESSION_KEY_IDENTITY = "sso_identity"
SSO_SESSION_KEY_CAMPAIGN = "campaign_id"


# Master DB alias name used throughout the code
MASTER_DB_ALIAS = os.getenv("MASTER_DB_ALIAS", "master").strip()

# ---------------------------------------------------------------------
# MASTER DATA TABLE NAMES (configure to match the admin Django project DB)
# ---------------------------------------------------------------------

# AuthorizedPublisher table
MASTER_DB_AUTH_PUBLISHER_TABLE = os.getenv(
    "MASTER_DB_AUTH_PUBLISHER_TABLE",
    "campaign_authorizedpublisher",   # <-- CHANGE to real table name
).strip()
MASTER_DB_AUTH_PUBLISHER_EMAIL_COLUMN = os.getenv(
    "MASTER_DB_AUTH_PUBLISHER_EMAIL_COLUMN",
    "email",
).strip()

# FieldRep table
MASTER_DB_FIELD_REP_TABLE = os.getenv(
    "MASTER_DB_FIELD_REP_TABLE",
    "campaign_campaignfieldrep;",              # <-- CHANGE to real table name
).strip()
MASTER_DB_FIELD_REP_PK_COLUMN = os.getenv("MASTER_DB_FIELD_REP_PK_COLUMN", "id").strip()
MASTER_DB_FIELD_REP_EXTERNAL_ID_COLUMN = os.getenv(
    "MASTER_DB_FIELD_REP_EXTERNAL_ID_COLUMN",
    "brand_supplied_field_rep_id",
).strip()
MASTER_DB_FIELD_REP_ACTIVE_COLUMN = os.getenv("MASTER_DB_FIELD_REP_ACTIVE_COLUMN", "is_active").strip()
MASTER_DB_FIELD_REP_FULL_NAME_COLUMN = os.getenv("MASTER_DB_FIELD_REP_FULL_NAME_COLUMN", "full_name").strip()
MASTER_DB_FIELD_REP_PHONE_COLUMN = os.getenv("MASTER_DB_FIELD_REP_PHONE_COLUMN", "phone_number").strip()

# Campaign table (the admin project’s campaign master)
MASTER_DB_CAMPAIGN_TABLE = os.getenv(
    "MASTER_DB_CAMPAIGN_TABLE",
    "campaign_campaign",              # <-- CHANGE to real table name (could be schema qualified)
).strip()
MASTER_DB_CAMPAIGN_ID_COLUMN = os.getenv("MASTER_DB_CAMPAIGN_ID_COLUMN", "campaign_id").strip()
MASTER_DB_CAMPAIGN_DOCTORS_SUPPORTED_COLUMN = os.getenv("MASTER_DB_CAMPAIGN_DOCTORS_SUPPORTED_COLUMN", "doctors_supported").strip()
MASTER_DB_CAMPAIGN_WA_ADDITION_COLUMN = os.getenv("MASTER_DB_CAMPAIGN_WA_ADDITION_COLUMN", "wa_addition").strip()
MASTER_DB_CAMPAIGN_VIDEO_CLUSTER_COLUMN = os.getenv("MASTER_DB_CAMPAIGN_VIDEO_CLUSTER_COLUMN", "new_video_cluster_name").strip()
MASTER_DB_CAMPAIGN_EMAIL_REGISTRATION_COLUMN = os.getenv("MASTER_DB_CAMPAIGN_EMAIL_REGISTRATION_COLUMN", "email_registration").strip()

# Public base URL used for absolute links
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://portal.cpdinclinic.co.in").rstrip("/")


# -----------------------------
# MASTER DB alias + connection
# -----------------------------
MASTER_DB_ALIAS = "master"

DATABASES[MASTER_DB_ALIAS] = {
    "ENGINE": "django.db.backends.mysql",
    "NAME": "healthcare_forms",
    "USER": "admin",
    "PASSWORD": "Hemsod-vytsew-7qypxa",
    "HOST": "new-forms-rds.cbnobb8kfeuq.ap-south-1.rds.amazonaws.com",
    "PORT": "3306",
    "OPTIONS": {"charset": "utf8mb4"},
    "CONN_MAX_AGE": 60,
}



# -----------------------------
# MASTER TABLES/COLUMNS (based on your DB output + models)
# -----------------------------

# FieldRep table
MASTER_DB_FIELD_REP_TABLE = "campaign_fieldrep"
MASTER_DB_FIELD_REP_PK_COLUMN = "id"
MASTER_DB_FIELD_REP_USER_ID_COLUMN = "user_id"
MASTER_DB_FIELD_REP_EXTERNAL_ID_COLUMN = "brand_supplied_field_rep_id"
MASTER_DB_FIELD_REP_ACTIVE_COLUMN = "is_active"
MASTER_DB_FIELD_REP_FULL_NAME_COLUMN = "full_name"
MASTER_DB_FIELD_REP_PHONE_COLUMN = "phone_number"

# CampaignFieldRep join table
MASTER_DB_CAMPAIGN_FIELD_REP_TABLE = "campaign_campaignfieldrep"
MASTER_DB_CAMPAIGN_FIELD_REP_PK_COLUMN = "id"
MASTER_DB_CAMPAIGN_FIELD_REP_CAMPAIGN_COLUMN = "campaign_id"
MASTER_DB_CAMPAIGN_FIELD_REP_FIELD_REP_COLUMN = "field_rep_id"

# Campaign table (IMPORTANT: id is the primary key)
MASTER_DB_CAMPAIGN_TABLE = "campaign_campaign"
MASTER_DB_CAMPAIGN_ID_COLUMN = "id"
MASTER_DB_CAMPAIGN_DOCTORS_SUPPORTED_COLUMN = "num_doctors_supported"
MASTER_DB_CAMPAIGN_WA_ADDITION_COLUMN = "add_to_campaign_message"   # used as wa_addition in Project2 flow
MASTER_DB_CAMPAIGN_VIDEO_CLUSTER_COLUMN = "name"                    # used as new_video_cluster_name in Project2 flow
MASTER_DB_CAMPAIGN_EMAIL_REGISTRATION_COLUMN = "register_message"   # used as email_registration in Project2 flow

PUBLIC_BASE_URL = "https://portal.cpdinclinic.co.in"
