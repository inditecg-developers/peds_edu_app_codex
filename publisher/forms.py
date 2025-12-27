from __future__ import annotations

from typing import Optional

from django import forms
from django.core.exceptions import ValidationError
from django.db.models import Q
from django.forms import inlineformset_factory
from django.forms.models import BaseInlineFormSet

from catalog.constants import LANGUAGE_CODES
from catalog.models import (
    TherapyArea,
    Video,
    VideoCluster,
    VideoClusterLanguage,
    VideoClusterVideo,
    VideoLanguage,
    VideoTriggerMap,
    Trigger,
    TriggerCluster,
)


class TherapyAreaForm(forms.ModelForm):
    class Meta:
        model = TherapyArea
        fields = ["code", "display_name", "description", "is_active"]


class VideoClusterForm(forms.ModelForm):
    class Meta:
        model = VideoCluster
        # trigger is REQUIRED by the model; include it so bundle create works.
        fields = ["code", "display_name", "description", "trigger", "is_published", "is_active"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Default ordering should be alphabetical for selector usability.
        if "trigger" in self.fields:
            self.fields["trigger"].queryset = Trigger.objects.all().order_by("display_name", "code")


class VideoForm(forms.ModelForm):
    """
    Videos cannot exist standalone: a Video must be linked to >= 1 bundle/cluster.
    We enforce this in the publisher form (require clusters) and persist via VideoClusterVideo.
    """

    clusters = forms.ModelMultipleChoiceField(
        queryset=VideoCluster.objects.none(),
        required=True,
        widget=forms.SelectMultiple(attrs={"size": 8}),
        help_text="Select at least 1 bundle/cluster. A video cannot exist standalone.",
    )

    class Meta:
        model = Video
        fields = ["code", "thumbnail_url", "is_active"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Show active bundles by default; also include any bundles the current video already belongs to.
        if self.instance and self.instance.pk:
            existing_ids = list(self.instance.clusters.values_list("pk", flat=True))
            qs = (
                VideoCluster.objects.filter(Q(is_active=True) | Q(pk__in=existing_ids))
                .order_by("display_name", "code")
            )
            self.fields["clusters"].initial = list(self.instance.clusters.all())
        else:
            qs = VideoCluster.objects.filter(is_active=True).order_by("display_name", "code")

        self.fields["clusters"].queryset = qs


class VideoLanguageForm(forms.ModelForm):
    class Meta:
        model = VideoLanguage
        fields = ["language_code", "title", "youtube_url"]


class BaseVideoLanguageFormSet(BaseInlineFormSet):
    """Enforce that all 8 languages are present and have title + URL."""

    def clean(self):
        super().clean()

        seen = set()
        missing = set(LANGUAGE_CODES)

        for form in self.forms:
            # If the form itself is invalid, skip; Django will surface field-level errors.
            if not hasattr(form, "cleaned_data"):
                continue

            code = form.cleaned_data.get("language_code")
            title = (form.cleaned_data.get("title") or "").strip()
            url = (form.cleaned_data.get("youtube_url") or "").strip()

            if not code:
                continue

            if code in seen:
                raise ValidationError(
                    "Duplicate language detected. Each language must be entered exactly once."
                )

            seen.add(code)
            missing.discard(code)

            if not title or not url:
                raise ValidationError(
                    "Please provide both Title and YouTube URL for every language."
                )

        if missing:
            raise ValidationError(
                "Please provide Title and YouTube URL for all languages: "
                + ", ".join(sorted(missing))
            )


def make_video_language_formset(extra: int = 0):
    return inlineformset_factory(
        Video,
        VideoLanguage,
        form=VideoLanguageForm,
        formset=BaseVideoLanguageFormSet,
        fields=["language_code", "title", "youtube_url"],
        extra=extra,
        can_delete=False,
    )


class VideoClusterLanguageForm(forms.ModelForm):
    class Meta:
        model = VideoClusterLanguage
        fields = ["language_code", "name"]


class VideoClusterVideoForm(forms.ModelForm):
    # Make sort_order optional; model default will be used when empty.
    sort_order = forms.IntegerField(required=False)

    class Meta:
        model = VideoClusterVideo
        fields = ["video", "sort_order"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Alphabetical by code by default.
        if "video" in self.fields:
            self.fields["video"].queryset = Video.objects.all().order_by("code")


def make_cluster_language_formset(extra: int = 5):
    """Bundle names per language."""
    return inlineformset_factory(
        VideoCluster,
        VideoClusterLanguage,
        form=VideoClusterLanguageForm,
        fields=["language_code", "name"],
        extra=extra,
        can_delete=True,
    )


def make_cluster_video_formset(extra: int = 5):
    """Videos inside a bundle."""
    return inlineformset_factory(
        VideoCluster,
        VideoClusterVideo,
        form=VideoClusterVideoForm,
        fields=["video", "sort_order"],
        extra=extra,
        can_delete=True,
    )


class TriggerForm(forms.ModelForm):
    class Meta:
        model = Trigger
        # code + cluster are required for creation.
        fields = [
            "code",
            "display_name",
            "cluster",
            "primary_therapy",
            "doctor_trigger_label",
            "is_active",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if "cluster" in self.fields:
            self.fields["cluster"].queryset = TriggerCluster.objects.all().order_by(
                "display_name", "code"
            )
        if "primary_therapy" in self.fields:
            self.fields["primary_therapy"].queryset = TherapyArea.objects.all().order_by(
                "display_name", "code"
            )


class TriggerClusterForm(forms.ModelForm):
    class Meta:
        model = TriggerCluster
        # language_code removed from the publisher UI: clusters are required to cover all 8 languages downstream
        fields = ["code", "display_name", "description", "is_active"]


class BundleTriggerMapForm(forms.Form):
    """Maps a Trigger to a Bundle (VideoCluster) by updating VideoCluster.trigger."""

    bundle = forms.ModelChoiceField(queryset=VideoCluster.objects.none(), required=True)
    trigger = forms.ModelChoiceField(queryset=Trigger.objects.none(), required=True)

    def __init__(self, *args, bundle_instance: Optional[VideoCluster] = None, **kwargs):
        super().__init__(*args, **kwargs)

        self.bundle_instance = bundle_instance

        # Bundles: show active by default; include current bundle if editing and inactive.
        if bundle_instance and bundle_instance.pk:
            bqs = VideoCluster.objects.filter(
                Q(is_active=True) | Q(pk=bundle_instance.pk)
            ).order_by("display_name", "code")
        else:
            bqs = VideoCluster.objects.filter(is_active=True).order_by("display_name", "code")

        # Triggers: show active by default; include current trigger if editing and inactive.
        if bundle_instance and getattr(bundle_instance, "trigger_id", None):
            tqs = Trigger.objects.filter(
                Q(is_active=True) | Q(pk=bundle_instance.trigger_id)
            ).order_by("display_name", "code")
        else:
            tqs = Trigger.objects.filter(is_active=True).order_by("display_name", "code")

        self.fields["bundle"].queryset = bqs
        self.fields["trigger"].queryset = tqs

        if bundle_instance and bundle_instance.pk:
            self.fields["bundle"].initial = bundle_instance
            self.fields["bundle"].disabled = True
            if getattr(bundle_instance, "trigger_id", None):
                self.fields["trigger"].initial = bundle_instance.trigger_id

    def clean_bundle(self):
        if self.bundle_instance and self.bundle_instance.pk:
            return self.bundle_instance
        return self.cleaned_data["bundle"]


class VideoTriggerMapForm(forms.ModelForm):
    """
    Legacy. The system now uses Trigger -> Bundle (VideoCluster) mapping as the primary mechanism,
    but the model is retained for backward compatibility.
    """

    # Make sort_order optional; model default will be used when empty.
    sort_order = forms.IntegerField(required=False)

    class Meta:
        model = VideoTriggerMap
        fields = ["trigger", "video", "is_primary", "sort_order"]
