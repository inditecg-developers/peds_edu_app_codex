from __future__ import annotations

from django.db import models
from django.utils.text import slugify

from .constants import LANGUAGES, DEFAULT_VIDEO_URL, DEFAULT_THUMBNAIL_URL


class TherapyArea(models.Model):
    code = models.CharField(max_length=50, unique=True)
    display_name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    sort_order = models.IntegerField(default=0)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.code} - {self.display_name}"


class VideoCluster(models.Model):
    code = models.CharField(max_length=50, unique=True)
    display_name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    sort_order = models.IntegerField(default=0)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.code} - {self.display_name}"


class Video(models.Model):
    code = models.CharField(max_length=50, unique=True)
    thumbnail_url = models.URLField(blank=True, default=DEFAULT_THUMBNAIL_URL)
    sort_order = models.IntegerField(default=0)
    is_active = models.BooleanField(default=True)
    clusters = models.ManyToManyField(VideoCluster, blank=True, related_name="videos")

    def __str__(self):
        return self.code


class VideoLanguage(models.Model):
    video = models.ForeignKey(Video, on_delete=models.CASCADE, related_name="languages")
    language_code = models.CharField(max_length=10, choices=LANGUAGES)
    title = models.CharField(max_length=255)
    youtube_url = models.URLField(default=DEFAULT_VIDEO_URL)

    class Meta:
        unique_together = ("video", "language_code")

    def __str__(self):
        return f"{self.video.code} [{self.language_code}] {self.title}"


class Trigger(models.Model):
    display_name = models.CharField(max_length=255)
    primary_therapy = models.ForeignKey(TherapyArea, on_delete=models.PROTECT, related_name="primary_triggers")
    is_active = models.BooleanField(default=True)
    sort_order = models.IntegerField(default=0)

    def __str__(self):
        return self.display_name


class TriggerCluster(models.Model):
    display_name = models.CharField(max_length=255)
    language_code = models.CharField(max_length=10, choices=LANGUAGES, default="en")
    is_active = models.BooleanField(default=True)
    sort_order = models.IntegerField(default=0)

    triggers = models.ManyToManyField(Trigger, blank=True, related_name="clusters")

    def __str__(self):
        return f"{self.display_name} ({self.language_code})"
