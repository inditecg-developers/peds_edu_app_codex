from __future__ import annotations

import json
from typing import Any, Dict, List

from django import forms
from django.core.exceptions import ValidationError


class CampaignCreateForm(forms.Form):
    campaign_id = forms.CharField(widget=forms.HiddenInput())

    new_video_cluster_name = forms.CharField(
        max_length=255,
        label="New video-cluster name",
    )

    # Hidden field holding JSON array: [{"type":"video","id":..}, {"type":"cluster","id":..}]
    selected_items_json = forms.CharField(
        widget=forms.HiddenInput(),
        required=True,
    )

    doctors_supported = forms.IntegerField(
        min_value=0,
        label="Number of doctors supported",
    )

    banner_small = forms.FileField(required=True, label="Banner (small)")
    banner_large = forms.FileField(required=True, label="Banner (large)")

    banner_target_url = forms.URLField(
        max_length=500,
        label="Banner click URL",
    )

    # --- NEW FIELDS ---
    email_registration = forms.CharField(
        label="Email message for registering a new doctor",
        widget=forms.Textarea(attrs={"rows": 4}),
        required=True,
    )

    wa_addition = forms.CharField(
        label="WhatsApp message for adding an already registered doctor",
        widget=forms.Textarea(attrs={"rows": 4}),
        required=True,
    )
    # ------------------

    start_date = forms.DateField(
        widget=forms.DateInput(attrs={"type": "date"}),
        label="Start date",
    )
    end_date = forms.DateField(
        widget=forms.DateInput(attrs={"type": "date"}),
        label="End date",
    )

    def clean_selected_items_json(self) -> str:
        raw = (self.cleaned_data.get("selected_items_json") or "").strip()
        if not raw:
            raise ValidationError("Please select at least one video or video-cluster.")

        try:
            data = json.loads(raw)
        except Exception:
            raise ValidationError("Invalid selection payload.")

        if not isinstance(data, list) or not data:
            raise ValidationError("Please select at least one video or video-cluster.")

        cleaned: List[Dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            t = str(item.get("type") or "").strip().lower()
            if t not in ("video", "cluster"):
                continue
            try:
                i = int(item.get("id"))
            except Exception:
                continue
            cleaned.append({"type": t, "id": i})

        if not cleaned:
            raise ValidationError("Please select at least one valid video or video-cluster.")

        # normalize back to JSON string
        return json.dumps(cleaned)

    def clean(self):
        cleaned = super().clean()

        name = (cleaned.get("new_video_cluster_name") or "").strip()
        if name:
            cleaned["new_video_cluster_name"] = name

        sd = cleaned.get("start_date")
        ed = cleaned.get("end_date")
        if sd and ed and sd > ed:
            self.add_error("end_date", "End date must be on or after start date.")

        # --- OPTIONAL CLEANUP ---
        if "email_registration" in cleaned:
            cleaned["email_registration"] = (cleaned.get("email_registration") or "").strip()
        if "wa_addition" in cleaned:
            cleaned["wa_addition"] = (cleaned.get("wa_addition") or "").strip()
        # ----------------------

        return cleaned

class CampaignEditForm(CampaignCreateForm):
    # For edit: banners may be unchanged
    banner_small = forms.FileField(required=False, label="Replace banner (small)")
    banner_large = forms.FileField(required=False, label="Replace banner (large)")
