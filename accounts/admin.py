from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin

from .models import User, Clinic, DoctorProfile
from .email_log import EmailLog


# ---------------------------------------------------------------------
# Email Log
# ---------------------------------------------------------------------

@admin.register(EmailLog)
class EmailLogAdmin(admin.ModelAdmin):
    list_display = ("created_at", "to_email", "subject", "success", "status_code")
    search_fields = ("to_email", "subject", "response_body", "error")
    list_filter = ("success", "provider", "created_at")


# ---------------------------------------------------------------------
# User Admin
# ---------------------------------------------------------------------

@admin.register(User)
class UserAdmin(DjangoUserAdmin):
    ordering = ("email",)
    list_display = ("email", "full_name", "is_staff", "is_active")
    search_fields = ("email", "full_name")

    fieldsets = (
        (None, {"fields": ("email", "password", "full_name")}),
        (
            "Permissions",
            {
                "fields": (
                    "is_active",
                    "is_staff",
                    "is_superuser",
                    "groups",
                    "user_permissions",
                )
            },
        ),
        ("Important dates", {"fields": ("last_login",)}),
    )

    add_fieldsets = (
        (
            None,
            {
                "classes": ("wide",),
                "fields": ("email", "full_name", "password1", "password2"),
            },
        ),
    )


# ---------------------------------------------------------------------
# Clinic Admin
# ---------------------------------------------------------------------

@admin.register(Clinic)
class ClinicAdmin(admin.ModelAdmin):
    list_display = ("clinic_code", "display_name", "state", "clinic_phone")
    search_fields = ("display_name", "clinic_phone", "state")

    def clinic_code(self, obj):
        """
        Admin-only readable clinic identifier.
        Example: CLN-0001
        """
        return f"CLN-{obj.id:04d}" if obj.id else "-"

    clinic_code.short_description = "Clinic Code"
    clinic_code.admin_order_field = "id"


# ---------------------------------------------------------------------
# Doctor Profile Admin
# ---------------------------------------------------------------------

@admin.register(DoctorProfile)
class DoctorProfileAdmin(admin.ModelAdmin):
    list_display = (
        "doctor_id",
        "user",
        "clinic",
        "whatsapp_number",
        "imc_number",
    )
    search_fields = (
        "doctor_id",
        "user__email",
        "user__full_name",
        "whatsapp_number",
        "imc_number",
    )
    list_select_related = ("user", "clinic")
