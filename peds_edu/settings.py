"""Django settings for peds_edu.

This project is designed to be deployed on AWS Ubuntu with MySQL.
Configuration is done primarily via environment variables.
"""

from __future__ import annotations
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

MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"

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
SENDGRID_API_KEY = env("SENDGRID_API_KEY", "").strip()
if not SENDGRID_API_KEY:
    # Optional fallback via AWS Secrets Manager
    SENDGRID_API_KEY = (get_secret_string("SendGrid_API", region_name="ap-south-1") or "").strip()

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
