from __future__ import annotations

from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render

from catalog.models import (
    TherapyArea,
    Trigger,
    TriggerCluster,
    Video,
    VideoCluster,
    VideoTriggerMap,
    VideoClusterVideo,
)
from publisher.forms import (
    BundleTriggerMapForm,
    TherapyAreaForm,
    TriggerClusterForm,
    TriggerForm,
    VideoClusterForm,
    VideoForm,
    VideoTriggerMapForm,  # legacy, retained
    make_cluster_language_formset,
    make_cluster_video_formset,
    make_video_language_formset,
)


@staff_member_required
def dashboard(request):
    return render(request, "publisher/dashboard.html")


# ---------------------------
# Therapy Areas
# ---------------------------
@staff_member_required
def therapy_list(request):
    q = (request.GET.get("q") or "").strip()
    rows = TherapyArea.objects.all().order_by("display_name", "code")
    if q:
        rows = rows.filter(Q(code__icontains=q) | Q(display_name__icontains=q))
    return render(request, "publisher/therapy_list.html", {"rows": rows, "q": q})


@staff_member_required
def therapy_create(request):
    if request.method == "POST":
        form = TherapyAreaForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Therapy area created.")
            return redirect("publisher:therapy_list")
    else:
        form = TherapyAreaForm()
    return render(request, "publisher/therapy_form.html", {"form": form, "object": None})


@staff_member_required
def therapy_edit(request, pk):
    obj = get_object_or_404(TherapyArea, pk=pk)
    if request.method == "POST":
        form = TherapyAreaForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Therapy area updated.")
            return redirect("publisher:therapy_list")
    else:
        form = TherapyAreaForm(instance=obj)
    return render(request, "publisher/therapy_form.html", {"form": form, "object": obj})


# ---------------------------
# Trigger Clusters
# ---------------------------
@staff_member_required
def trigger_cluster_list(request):
    q = (request.GET.get("q") or "").strip()
    rows = TriggerCluster.objects.all().order_by("display_name", "code")
    if q:
        rows = rows.filter(Q(code__icontains=q) | Q(display_name__icontains=q))
    return render(request, "publisher/trigger_cluster_list.html", {"rows": rows, "q": q})


@staff_member_required
def trigger_cluster_create(request):
    if request.method == "POST":
        form = TriggerClusterForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Trigger cluster created.")
            return redirect("publisher:triggercluster_list")
    else:
        form = TriggerClusterForm()
    return render(request, "publisher/triggercluster_form.html", {"form": form, "object": None})


@staff_member_required
def trigger_cluster_edit(request, pk):
    obj = get_object_or_404(TriggerCluster, pk=pk)
    if request.method == "POST":
        form = TriggerClusterForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Trigger cluster updated.")
            return redirect("publisher:triggercluster_list")
    else:
        form = TriggerClusterForm(instance=obj)
    return render(request, "publisher/triggercluster_form.html", {"form": form, "object": obj})


# ---------------------------
# Triggers
# ---------------------------
@staff_member_required
def trigger_list(request):
    q = (request.GET.get("q") or "").strip()
    rows = Trigger.objects.select_related("cluster", "primary_therapy").all().order_by("display_name", "code")
    if q:
        rows = rows.filter(Q(code__icontains=q) | Q(display_name__icontains=q))
    return render(request, "publisher/trigger_list.html", {"rows": rows, "q": q})


@staff_member_required
def trigger_create(request):
    if request.method == "POST":
        form = TriggerForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Trigger created.")
            return redirect("publisher:trigger_list")
    else:
        form = TriggerForm()
    return render(request, "publisher/trigger_form.html", {"form": form, "object": None})


@staff_member_required
def trigger_edit(request, pk):
    obj = get_object_or_404(Trigger, pk=pk)
    if request.method == "POST":
        form = TriggerForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Trigger updated.")
            return redirect("publisher:trigger_list")
    else:
        form = TriggerForm(instance=obj)
    return render(request, "publisher/trigger_form.html", {"form": form, "object": obj})


# ---------------------------
# Videos
# ---------------------------
@staff_member_required
def video_list(request):
    q = (request.GET.get("q") or "").strip()
    rows = Video.objects.all().order_by("code")
    if q:
        rows = rows.filter(Q(code__icontains=q))
    return render(request, "publisher/video_list.html", {"rows": rows, "q": q})


@staff_member_required
def video_create(request):
    FormSet = make_video_language_formset(extra=8)

    if request.method == "POST":
        form = VideoForm(request.POST)
        formset = FormSet(request.POST)

        if form.is_valid() and formset.is_valid():
            with transaction.atomic():
                video = form.save()

                clusters = list(form.cleaned_data.get("clusters") or [])
                for cluster in clusters:
                    VideoClusterVideo.objects.get_or_create(
                        video=video,
                        video_cluster=cluster,
                        defaults={"sort_order": 0},
                    )

                formset.instance = video
                formset.save()

            messages.success(request, "Video created.")
            return redirect("publisher:video_list")
    else:
        form = VideoForm()
        initial = [{"language_code": code} for code in ("en", "hi", "mr", "te", "ta", "bn", "ml", "kn")]
        formset = FormSet(initial=initial)

    return render(request, "publisher/video_form.html", {"form": form, "formset": formset, "object": None})


@staff_member_required
def video_edit(request, pk):
    video = get_object_or_404(Video, pk=pk)

    for code in ("en", "hi", "mr", "te", "ta", "bn", "ml", "kn"):
        video.languages.get_or_create(language_code=code, defaults={"title": "", "youtube_url": ""})

    FormSet = make_video_language_formset(extra=0)

    if request.method == "POST":
        form = VideoForm(request.POST, instance=video)
        formset = FormSet(request.POST, instance=video)

        if form.is_valid() and formset.is_valid():
            with transaction.atomic():
                form.save()
                formset.save()

                selected_clusters = list(form.cleaned_data.get("clusters") or [])
                selected_ids = {c.id for c in selected_clusters}
                existing_ids = set(video.clusters.values_list("id", flat=True))

                to_add = selected_ids - existing_ids
                to_remove = existing_ids - selected_ids

                if to_remove:
                    VideoClusterVideo.objects.filter(video=video, video_cluster_id__in=to_remove).delete()
                for cid in to_add:
                    VideoClusterVideo.objects.get_or_create(
                        video=video,
                        video_cluster_id=cid,
                        defaults={"sort_order": 0},
                    )

            messages.success(request, "Video updated.")
            return redirect("publisher:video_list")
    else:
        form = VideoForm(instance=video)
        formset = FormSet(instance=video)

    return render(request, "publisher/video_form.html", {"form": form, "formset": formset, "object": video})


# ---------------------------
# Bundles / Clusters
# ---------------------------
@staff_member_required
def cluster_list(request):
    q = (request.GET.get("q") or "").strip()
    rows = VideoCluster.objects.select_related("trigger").all().order_by("display_name", "code")
    if q:
        rows = rows.filter(Q(code__icontains=q) | Q(display_name__icontains=q))
    return render(request, "publisher/cluster_list.html", {"rows": rows, "q": q})


@staff_member_required
def cluster_create(request):
    # IMPORTANT: template expects lang_fs / vid_fs and cluster / is_new
    LangFS = make_cluster_language_formset(extra=8)
    VidFS = make_cluster_video_formset(extra=8)

    cluster = VideoCluster()

    if request.method == "POST":
        form = VideoClusterForm(request.POST)
        lang_fs = LangFS(request.POST, instance=cluster)
        vid_fs = VidFS(request.POST, instance=cluster)

        if form.is_valid() and lang_fs.is_valid() and vid_fs.is_valid():
            with transaction.atomic():
                cluster = form.save()
                lang_fs.instance = cluster
                vid_fs.instance = cluster
                lang_fs.save()
                vid_fs.save()

            messages.success(request, "Bundle created.")
            return redirect("publisher:cluster_list")
    else:
        form = VideoClusterForm()
        lang_fs = LangFS(instance=cluster)
        vid_fs = VidFS(instance=cluster)

    return render(
        request,
        "publisher/cluster_form.html",
        {
            "form": form,
            "cluster": cluster,
            "is_new": True,
            "lang_fs": lang_fs,
            "vid_fs": vid_fs,
            # Back-compat if any template still reads these
            "lang_formset": lang_fs,
            "video_formset": vid_fs,
            "object": None,
        },
    )


@staff_member_required
def cluster_edit(request, pk):
    cluster = get_object_or_404(VideoCluster, pk=pk)

    LangFS = make_cluster_language_formset(extra=5)
    VidFS = make_cluster_video_formset(extra=8)

    if request.method == "POST":
        form = VideoClusterForm(request.POST, instance=cluster)
        lang_fs = LangFS(request.POST, instance=cluster)
        vid_fs = VidFS(request.POST, instance=cluster)

        if form.is_valid() and lang_fs.is_valid() and vid_fs.is_valid():
            with transaction.atomic():
                form.save()
                lang_fs.save()
                vid_fs.save()

            messages.success(request, "Bundle updated.")
            return redirect("publisher:cluster_list")
    else:
        form = VideoClusterForm(instance=cluster)
        lang_fs = LangFS(instance=cluster)
        vid_fs = VidFS(instance=cluster)

    return render(
        request,
        "publisher/cluster_form.html",
        {
            "form": form,
            "cluster": cluster,
            "is_new": False,
            "lang_fs": lang_fs,
            "vid_fs": vid_fs,
            "lang_formset": lang_fs,
            "video_formset": vid_fs,
            "object": cluster,
        },
    )


# ---------------------------
# Bundle Trigger Maps (replaces Video Trigger Maps)
# ---------------------------
@staff_member_required
def map_list(request):
    q = (request.GET.get("q") or "").strip()

    bundles = VideoCluster.objects.select_related("trigger", "trigger__primary_therapy").all().order_by("display_name", "code")
    if q:
        bundles = bundles.filter(
            Q(code__icontains=q)
            | Q(display_name__icontains=q)
            | Q(trigger__code__icontains=q)
            | Q(trigger__display_name__icontains=q)
        )

    return render(request, "publisher/map_list.html", {"items": bundles, "q": q})


@staff_member_required
def map_create(request):
    if request.method == "POST":
        form = BundleTriggerMapForm(request.POST)
        if form.is_valid():
            bundle = form.cleaned_data["bundle"]
            trigger = form.cleaned_data["trigger"]
            bundle.trigger = trigger
            bundle.save(update_fields=["trigger"])
            messages.success(request, "Bundle trigger mapping saved.")
            return redirect("publisher:map_list")
    else:
        form = BundleTriggerMapForm()

    return render(request, "publisher/map_form.html", {"form": form, "object": None})


@staff_member_required
def map_edit(request, pk):
    bundle = get_object_or_404(VideoCluster, pk=pk)

    if request.method == "POST":
        form = BundleTriggerMapForm(request.POST, bundle_instance=bundle)
        if form.is_valid():
            trigger = form.cleaned_data["trigger"]
            bundle.trigger = trigger
            bundle.save(update_fields=["trigger"])
            messages.success(request, "Bundle trigger mapping updated.")
            return redirect("publisher:map_list")
    else:
        form = BundleTriggerMapForm(bundle_instance=bundle, initial={"trigger": bundle.trigger_id})

    return render(request, "publisher/map_form.html", {"form": form, "object": bundle})


@staff_member_required
def legacy_video_trigger_map_list(request):
    q = (request.GET.get("q") or "").strip()
    items = VideoTriggerMap.objects.select_related("trigger", "video").all().order_by("trigger__code", "video__code")
    if q:
        items = items.filter(Q(video__code__icontains=q) | Q(trigger__code__icontains=q))
    return render(request, "publisher/map_list.html", {"rows": items, "q": q})
