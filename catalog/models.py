from __future__ import annotations

from django.db import models
from django.utils.text import slugify

from .constants import LANGUAGES, DEFAULT_VIDEO_URL, DEFAULT_THUMBNAIL_URL


class TherapyArea(models.Model):
    code = models.CharField(max_length=50, unique=True)
    display_name = models.CharField(max_length=255)
    description = models.TextField(blank=True, default="")
    sort_order = models.IntegerField(default=0)
    is_active = models.BooleanField(default=True)

    @staticmethod
    def code_from_name(name: str) -> str:
        base = slugify((name or "").strip(), allow_unicode=False).replace("-", "_").upper()
        base = base.strip("_")
        return (base[:50] if base else "THERAPY")

    def __str__(self):
        return f"{self.code} - {self.display_name}"


class VideoCluster(models.Model):
    code = models.CharField(max_length=80, unique=True)
    display_name = models.CharField(max_length=255)
    description = models.TextField(blank=True, default="")
    trigger = models.ForeignKey(
        "Trigger",
        on_delete=models.PROTECT,
        related_name="video_clusters",
    )
    sort_order = models.IntegerField(default=0)
    is_published = models.BooleanField(default=False)
    search_keywords = models.TextField(blank=True, default="")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.code} - {self.display_name}"


class Video(models.Model):
    code = models.CharField(max_length=80, unique=True)
    description = models.TextField(blank=True, default="")
    thumbnail_url = models.URLField(
        blank=True,
        default=DEFAULT_THUMBNAIL_URL,
        max_length=500,
    )
    primary_therapy = models.ForeignKey(
        TherapyArea,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="videos",
    )
    primary_trigger = models.ForeignKey(
        "Trigger",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="primary_videos",
    )
    sort_order = models.IntegerField(default=0)
    is_published = models.BooleanField(default=False)
    search_keywords = models.TextField(blank=True, default="")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    clusters = models.ManyToManyField(
        VideoCluster,
        through="VideoClusterVideo",
        related_name="videos",
        blank=True,
    )

    def __str__(self):
        return self.code


class VideoLanguage(models.Model):
    video = models.ForeignKey(
        Video,
        on_delete=models.CASCADE,
        related_name="languages",
    )
    language_code = models.CharField(max_length=10, choices=LANGUAGES)
    title = models.CharField(max_length=255)
    youtube_url = models.URLField(
        default=DEFAULT_VIDEO_URL,
        max_length=500,
    )

    class Meta:
        unique_together = ("video", "language_code")

    def __str__(self):
        return f"{self.video.code} [{self.language_code}] {self.title}"


class VideoClusterLanguage(models.Model):
    video_cluster = models.ForeignKey(
        VideoCluster,
        on_delete=models.CASCADE,
        related_name="languages",
    )
    language_code = models.CharField(max_length=10, choices=LANGUAGES)
    name = models.CharField(max_length=255)

    class Meta:
        unique_together = ("video_cluster", "language_code")

    def __str__(self):
        return f"{self.video_cluster.code} [{self.language_code}] {self.name}"


class Trigger(models.Model):
    code = models.CharField(max_length=80, unique=True)
    display_name = models.CharField(max_length=255)
    doctor_trigger_label = models.CharField(max_length=255, blank=True, default="")
    subtopic_title = models.CharField(max_length=255, blank=True, default="")
    navigation_pathways = models.TextField(blank=True, default="")
    search_keywords = models.TextField(blank=True, default="")
    cluster = models.ForeignKey(
        "TriggerCluster",
        on_delete=models.PROTECT,
        related_name="triggers",
    )
    primary_therapy = models.ForeignKey(
        TherapyArea,
        on_delete=models.PROTECT,
        related_name="triggers",
    )
    is_active = models.BooleanField(default=True)
    sort_order = models.IntegerField(default=0)

    def __str__(self):
        return f"{self.code} - {self.display_name}"


class TriggerCluster(models.Model):
    code = models.CharField(max_length=80, unique=True)
    display_name = models.CharField(max_length=255)
    description = models.TextField(blank=True, default="")
    language_code = models.CharField(
        max_length=10,
        choices=LANGUAGES,
        default="en",
    )
    is_active = models.BooleanField(default=True)
    sort_order = models.IntegerField(default=0)

    def __str__(self):
        return f"{self.code} - {self.display_name} ({self.language_code})"


class VideoClusterVideo(models.Model):
    video_cluster = models.ForeignKey(
        VideoCluster,
        on_delete=models.CASCADE,
        related_name="cluster_videos",
    )
    video = models.ForeignKey(
        Video,
        on_delete=models.CASCADE,
    )
    sort_order = models.IntegerField(default=0)

    class Meta:
        unique_together = ("video_cluster", "video")

    def __str__(self):
        return f"{self.video_cluster.code} - {self.video.code}"


class VideoTriggerMap(models.Model):
    video = models.ForeignKey(Video, on_delete=models.CASCADE)
    trigger = models.ForeignKey(Trigger, on_delete=models.CASCADE)
    is_primary = models.BooleanField(default=False)
    sort_order = models.IntegerField(default=0)

    class Meta:
        unique_together = ("video", "trigger")
