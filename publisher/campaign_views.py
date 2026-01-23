from __future__ import annotations
import json
from typing import Any, Dict, List, Set
from urllib.parse import urlencode
from django import forms
from django.contrib import messages
from django.db import models, transaction
from django.db.models import Q
from django.http import (
    HttpRequest,
    HttpResponse,
    HttpResponseBadRequest,
    JsonResponse,
)
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.text import slugify
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from catalog.models import (
    TherapyArea,
    TriggerCluster,
    Trigger,
    Video,
    VideoLanguage,
    VideoCluster,
    VideoClusterLanguage,
    VideoClusterVideo,
)

from .campaign_auth import SESSION_CAMPAIGN_KEY, get_publisher_claims, publisher_required
from .campaign_forms import CampaignCreateForm, CampaignEditForm
from .models import Campaign


# -----------------------------
# Helpers
# -----------------------------
SESSION_CAMPAIGN_META_BY_CAMPAIGN_KEY = "publisher_campaign_meta_by_campaign"
class FieldRepWhatsAppForm(forms.Form):
    whatsapp_number = forms.CharField(
        label="Enter doctor’s WhatsApp number",
        max_length=20,
        required=True,
        widget=forms.TextInput(attrs={"placeholder": "e.g. 9876543210", "inputmode": "numeric"}),
    )

def _capture_campaign_meta(request: HttpRequest, campaign_id: str | None) -> dict[str, Any]:
    """
    Capture extra values coming from Project1 and persist them in session.
    Stored per-campaign to avoid collisions.
    """
    meta_by_campaign = request.session.get(SESSION_CAMPAIGN_META_BY_CAMPAIGN_KEY) or {}
    meta = meta_by_campaign.get(campaign_id, {}) if campaign_id else {}

    param_names = [
        "num_doctors_supported",
        "name",
        "company_name",
        "contact_person_name",
        "contact_person_phone",
        "contact_person_email",
    ]

    for key in param_names:
        v = request.GET.get(key)
        if v is None:
            v = request.GET.get(key.replace("_", "-"))  # tolerate hyphenated keys
        if v is not None:
            meta[key] = str(v).strip()

    # normalize int
    try:
        meta["num_doctors_supported"] = int(meta.get("num_doctors_supported")) if meta.get("num_doctors_supported") not in (None, "") else None
    except Exception:
        meta["num_doctors_supported"] = None

    if campaign_id:
        meta_by_campaign[campaign_id] = meta
        request.session[SESSION_CAMPAIGN_META_BY_CAMPAIGN_KEY] = meta_by_campaign
        request.session.modified = True

    return meta

def _render_campaign_text_template(template: str, *, doctor_name: str, clinic_link: str, setup_link: str = "") -> str:
    """
    Supports placeholders used historically and those described in the spec.
    """
    text = template or ""

    replacements = {
        "<doctor.user.full_name>": doctor_name,
        "<doctor_name>": doctor_name,
        "{{doctor_name}}": doctor_name,

        "<clinic_link>": clinic_link,
        "{{clinic_link}}": clinic_link,
        "<LinkShare>": clinic_link,

        "<setup_link>": setup_link,
        "{{setup_link}}": setup_link,
        "<LinkPW>": setup_link,
    }

    for k, v in replacements.items():
        if v:
            text = text.replace(k, v)

    return text


@require_http_methods(["GET", "POST"])
def field_rep_landing_page(request: HttpRequest) -> HttpResponse:
        """
        JWT-gated page (via publisher_required) for field reps to:
        - enforce doctors_supported limit (from publisher_campaign)
        - check DoctorCampaignEnrollment count (master DB)
        - if doctor exists: enroll (if needed) and open WhatsApp prefilled message
        - if not: redirect to /accounts/register/ with campaign-id and field_rep_id
        """
        from accounts import master_db  # local import avoids circulars
    
        campaign_id = (
            request.GET.get("campaign-id")
            or request.GET.get("campaign_id")
            or request.session.get(SESSION_CAMPAIGN_KEY)
            or ""
        ).strip()
    
        field_rep_id = (request.GET.get("field_rep_id") or request.GET.get("field-rep-id") or "").strip()
    
        if not campaign_id or not field_rep_id:
            return render(
                request,
                "publisher/field_rep_landing_page.html",
                {
                    "form": FieldRepWhatsAppForm(),
                    "campaign_id": campaign_id,
                    "field_rep_id": field_rep_id,
                    "limit_reached": True,
                    "limit_message": "Missing campaign-id or field_rep_id in URL.",
                },
                status=400,
            )
    
        campaign = Campaign.objects.filter(campaign_id=campaign_id).first()
        if not campaign:
            return render(
                request,
                "publisher/field_rep_landing_page.html",
                {
                    "form": FieldRepWhatsAppForm(),
                    "campaign_id": campaign_id,
                    "field_rep_id": field_rep_id,
                    "limit_reached": True,
                    "limit_message": "Unknown campaign-id (campaign details not found in publisher_campaign).",
                },
                status=400,
            )
    
        doctors_supported = int(campaign.doctors_supported or 0)
    
        try:
            enrolled_count = master_db.count_campaign_enrollments(campaign_id)
        except master_db.MasterDBNotConfigured as e:
            return render(
                request,
                "publisher/field_rep_landing_page.html",
                {
                    "form": FieldRepWhatsAppForm(),
                    "campaign_id": campaign_id,
                    "field_rep_id": field_rep_id,
                    "campaign": campaign,
                    "enrolled_count": 0,
                    "doctors_supported": doctors_supported,
                    "limit_reached": True,
                    "limit_message": str(e),
                },
                status=500,
            )
    
        limit_reached = bool(doctors_supported and enrolled_count >= doctors_supported)
    
        if request.method == "GET":
            return render(
                request,
                "publisher/field_rep_landing_page.html",
                {
                    "form": FieldRepWhatsAppForm(),
                    "campaign_id": campaign_id,
                    "field_rep_id": field_rep_id,
                    "campaign": campaign,
                    "enrolled_count": enrolled_count,
                    "doctors_supported": doctors_supported,
                    "limit_reached": limit_reached,
                    "limit_message": (
                        "This campaign already has the maximum allowed doctors registered. "
                        "If you wish to register more doctors, please speak to your brand manager "
                        "who can answer your queries and obtain more licenses."
                    ),
                },
            )
    
        # POST
        form = FieldRepWhatsAppForm(request.POST)
        if not form.is_valid():
            return render(
                request,
                "publisher/field_rep_landing_page.html",
                {
                    "form": form,
                    "campaign_id": campaign_id,
                    "field_rep_id": field_rep_id,
                    "campaign": campaign,
                    "enrolled_count": enrolled_count,
                    "doctors_supported": doctors_supported,
                    "limit_reached": limit_reached,
                    "limit_message": (
                        "This campaign already has the maximum allowed doctors registered. "
                        "If you wish to register more doctors, please speak to your brand manager "
                        "who can answer your queries and obtain more licenses."
                    ),
                },
            )
    
        if limit_reached:
            return render(
                request,
                "publisher/field_rep_landing_page.html",
                {
                    "form": form,
                    "campaign_id": campaign_id,
                    "field_rep_id": field_rep_id,
                    "campaign": campaign,
                    "enrolled_count": enrolled_count,
                    "doctors_supported": doctors_supported,
                    "limit_reached": True,
                    "limit_message": (
                        "This campaign already has the maximum allowed doctors registered. "
                        "If you wish to register more doctors, please speak to your brand manager "
                        "who can answer your queries and obtain more licenses."
                    ),
                },
            )
    
        wa_number = form.cleaned_data["whatsapp_number"]
    
        doctor = master_db.get_doctor_by_whatsapp(wa_number)
        if doctor:
            # Ensure enrollment exists (so the count reflects the registration)
            master_db.ensure_enrollment(
                doctor_id=doctor.doctor_id,
                campaign_id=campaign_id,
                registered_by=field_rep_id,
            )
    
            clinic_link = f"https://portal.cpdinclinic.co.in/clinic/{doctor.doctor_id}/share/"
    
            wa_addition_text = _render_campaign_text_template(
                campaign.wa_addition or "",
                doctor_name=(doctor.full_name or "Doctor").strip(),
                clinic_link=clinic_link,
                setup_link="",
            ).strip()
    
            message_lines = []
            if wa_addition_text:
                message_lines.append(wa_addition_text)
            if campaign.new_video_cluster_name:
                message_lines.append(str(campaign.new_video_cluster_name).strip())
            message_lines.append("Link to your clinic’s patient education system:")
            message_lines.append(clinic_link)
    
            whatsapp_url = master_db.build_whatsapp_deeplink(wa_number, "\n".join(message_lines))
    
            return redirect(whatsapp_url)
    
        # Doctor not present: redirect to registration with campaign-id and field_rep_id
        register_url = reverse("accounts:register")
        query = urlencode({"campaign-id": campaign_id, "field_rep_id": field_rep_id})
        return redirect(f"{register_url}?{query}")
    


def _video_title_en(video: Video) -> str:
    # best-effort English title fallback
    vlang = (
        VideoLanguage.objects.filter(video=video, language_code="en").first()
        or VideoLanguage.objects.filter(video=video).first()
    )
    return (vlang.title if vlang and vlang.title else video.code).strip()


def _cluster_name_en(cluster: VideoCluster) -> str:
    clang = (
        VideoClusterLanguage.objects.filter(video_cluster=cluster, language_code="en").first()
        or VideoClusterLanguage.objects.filter(video_cluster=cluster).first()
    )
    if clang and clang.name:
        return clang.name.strip()
    return (cluster.display_name or cluster.code or "").strip()


def _expand_selected_items_to_video_ids(items: List[Dict[str, Any]]) -> List[int]:
    video_ids: Set[int] = set()
    cluster_ids: Set[int] = set()

    for item in items:
        t = str(item.get("type") or "").lower().strip()
        try:
            _id = int(item.get("id"))
        except Exception:
            continue

        if t == "video":
            video_ids.add(_id)
        elif t == "cluster":
            cluster_ids.add(_id)

    if cluster_ids:
        cluster_video_ids = VideoClusterVideo.objects.filter(
            video_cluster_id__in=list(cluster_ids)
        ).values_list("video_id", flat=True)
        video_ids.update(list(cluster_video_ids))

    return sorted(video_ids)


def _get_or_create_brand_trigger() -> Trigger:
    """
    VideoCluster requires a Trigger. Campaign spec does not provide trigger selection,
    so we create/use a dedicated "BRAND_CAMPAIGNS" therapy + trigger cluster + trigger.
    """

    therapy, _ = TherapyArea.objects.get_or_create(
        code="BRAND_CAMPAIGNS",
        defaults={
            "display_name": "Brand Campaigns",
            "description": "Auto-created therapy area for brand campaigns",
            "sort_order": 999,
            "is_active": True,
        },
    )

    tcluster, _ = TriggerCluster.objects.get_or_create(
        code="BRAND_CAMPAIGNS",
        defaults={
            "display_name": "Brand Campaigns",
            "description": "Auto-created trigger cluster for brand campaigns",
            "language_code": "en",
            "sort_order": 999,
            "is_active": True,
        },
    )

    trigger, _ = Trigger.objects.get_or_create(
        code="BRAND_CAMPAIGN",
        defaults={
            "display_name": "Brand Campaign",
            "doctor_trigger_label": "Brand Campaign",
            "subtopic_title": "Brand Campaign",
            "navigation_pathways": "",
            "search_keywords": "brand,campaign",
            "cluster": tcluster,
            "primary_therapy": therapy,
            "sort_order": 999,
            "is_active": True,
        },
    )

    return trigger


def _generate_unique_cluster_code(name: str) -> str:
    base = slugify(name, allow_unicode=False).replace("-", "_").upper().strip("_")
    if not base:
        base = "CAMPAIGN_CLUSTER"

    base = base[:70]  # leave room for suffix
    code = base
    i = 1
    while VideoCluster.objects.filter(code=code).exists():
        suffix = f"_{i}"
        code = f"{base[: (80 - len(suffix))]}{suffix}"
        i += 1

    return code


# -----------------------------
# Pages
# -----------------------------

@publisher_required
@require_GET
def publisher_landing_page(request: HttpRequest) -> HttpResponse:
    claims = get_publisher_claims(request) or {}

    campaign_id = (
        request.GET.get("campaign-id")
        or request.GET.get("campaign_id")
        or request.session.get(SESSION_CAMPAIGN_KEY)
        or ""
    ).strip() or None

    # Capture extra values from Project1 into session (per campaign)
    campaign_meta = _capture_campaign_meta(request, campaign_id)

    # (extra safety) Remove token from URL if present
    if any(k in request.GET for k in ("token", "jwt", "access_token")):
        q = request.GET.copy()
        for k in ("token", "jwt", "access_token"):
            q.pop(k, None)
        return redirect(f"{request.path}?{q.urlencode()}") if q else redirect(request.path)

    return render(
        request,
        "publisher/publisher_landing_page.html",
        {
            "publisher": claims,
            "campaign_id": campaign_id,
            "campaign_meta": campaign_meta,
            "show_auth_links": False,
        },
    )



@publisher_required
@require_http_methods(["GET", "POST"])
def add_campaign_details(request: HttpRequest) -> HttpResponse:
        claims = get_publisher_claims(request) or {}
    
        campaign_id = (
            request.GET.get("campaign-id")
            or request.GET.get("campaign_id")
            or request.POST.get("campaign_id")
            or request.session.get(SESSION_CAMPAIGN_KEY)
        )
        if not campaign_id:
            return HttpResponseBadRequest("campaign-id missing")
    
        request.session[SESSION_CAMPAIGN_KEY] = campaign_id
    
        existing = Campaign.objects.filter(campaign_id=campaign_id).first()
        if existing and request.method == "GET":
            messages.info(request, "Campaign already has details. Redirected to edit screen.")
            return redirect(
                reverse("campaign_publisher:edit_campaign_details", kwargs={"campaign_id": campaign_id})
            )
    
        if request.method == "POST":
            form = CampaignCreateForm(request.POST, request.FILES)
            if form.is_valid():
                # 5.1 Check if the new video cluster name already exists
                new_cluster_name = form.cleaned_data["new_video_cluster_name"]
                if VideoClusterLanguage.objects.filter(name__iexact=new_cluster_name).exists():
                    messages.error(
                        request,
                        "video cluster name already exists.  Write a different name",
                    )
                    return render(
                        request,
                        "publisher/add_campaign_details.html",
                        {
                            "form": form,
                            "campaign_id": campaign_id,
                            "publisher": claims,
                            "show_auth_links": False,
                        },
                    )
    
                # Expand selection into final list of videos
                selected_items = json.loads(form.cleaned_data["selected_items_json"])
                video_ids = _expand_selected_items_to_video_ids(selected_items)
                if not video_ids:
                    form.add_error(None, "Please select at least one valid video or video-cluster.")
                    return render(
                        request,
                        "publisher/add_campaign_details.html",
                        {
                            "form": form,
                            "campaign_id": campaign_id,
                            "publisher": claims,
                            "show_auth_links": False,
                        },
                    )
    
                # Also block duplicates by campaign_id
                if Campaign.objects.filter(campaign_id=campaign_id).exists():
                    messages.error(request, "Campaign already exists. Use edit instead.")
                    return redirect(
                        reverse("campaign_publisher:edit_campaign_details", kwargs={"campaign_id": campaign_id})
                    )
    
                publisher_sub = str(claims.get("sub") or "")
                publisher_username = str(claims.get("username") or "")
                publisher_roles = ",".join([str(r) for r in (claims.get("roles") or [])])
    
                with transaction.atomic():
                    # 5.3 Create a new video cluster
                    trigger = _get_or_create_brand_trigger()
                    cluster_code = _generate_unique_cluster_code(new_cluster_name)
    
                    cluster = VideoCluster.objects.create(
                        code=cluster_code,
                        display_name=new_cluster_name,
                        description="",
                        trigger=trigger,
                        sort_order=0,
                        is_published=True,
                        search_keywords=new_cluster_name,
                        is_active=True,
                    )
    
                    VideoClusterLanguage.objects.create(
                        video_cluster=cluster,
                        language_code="en",
                        name=new_cluster_name,
                    )
    
                    videos = list(Video.objects.filter(id__in=video_ids).order_by("code"))
                    for idx, v in enumerate(videos, start=1):
                        VideoClusterVideo.objects.create(
                            video_cluster=cluster,
                            video=v,
                            sort_order=idx,
                        )
    
                    # 5.2 Insert all details into campaigns-table
                    Campaign.objects.create(
                        campaign_id=campaign_id,
                        new_video_cluster_name=new_cluster_name,
                        selection_json=form.cleaned_data["selected_items_json"],
                        doctors_supported=form.cleaned_data["doctors_supported"],
                        banner_small=form.cleaned_data["banner_small"],
                        banner_large=form.cleaned_data["banner_large"],
                        banner_target_url=form.cleaned_data["banner_target_url"],
                        start_date=form.cleaned_data["start_date"],
                        end_date=form.cleaned_data["end_date"],
                        video_cluster=cluster,
                        publisher_sub=publisher_sub,
                        publisher_username=publisher_username,
                        publisher_roles=publisher_roles,
                        email_registration=form.cleaned_data["email_registration"],
                        wa_addition=form.cleaned_data["wa_addition"],
                    )
    
                messages.success(request, "Campaign saved. Video cluster created successfully.")
    
                return redirect(
                    f"{reverse('campaign_publisher:publisher_landing_page')}?{urlencode({'campaign-id': campaign_id})}"
                )
        else:
            initial = {
                "campaign_id": campaign_id,
                "selected_items_json": "[]",
                "email_registration": "",
                "wa_addition": "",
            }
    
            # -------------------------------------------------------------
            # Prefill doctors_supported from Project1 if provided
            # -------------------------------------------------------------
            meta_by_campaign = request.session.get(SESSION_CAMPAIGN_META_BY_CAMPAIGN_KEY) or {}
            meta = meta_by_campaign.get(campaign_id, {}) if campaign_id else {}
            if meta.get("num_doctors_supported") is not None:
                initial["doctors_supported"] = meta["num_doctors_supported"]
    
            form = CampaignCreateForm(initial=initial)
    
        return render(
            request,
            "publisher/add_campaign_details.html",
            {
                "form": form,
                "campaign_id": campaign_id,
                "publisher": claims,
                "show_auth_links": False,
            },
        )
    

@publisher_required
@require_GET
def campaign_list(request: HttpRequest) -> HttpResponse:
    claims = get_publisher_claims(request) or {}

    q = (request.GET.get("q") or "").strip()
    rows = Campaign.objects.all().order_by("-created_at")

    if q:
        rows = rows.filter(
            Q(campaign_id__icontains=q) | Q(new_video_cluster_name__icontains=q)
        )

    return render(
        request,
        "publisher/campaign_list.html",
        {
            "publisher": claims,
            "rows": rows,
            "q": q,
            "show_auth_links": False,
        },
    )


@publisher_required
@require_http_methods(["GET", "POST"])
def edit_campaign_details(request: HttpRequest, campaign_id: str) -> HttpResponse:
    claims = get_publisher_claims(request) or {}
    campaign = get_object_or_404(Campaign, campaign_id=campaign_id)

    if request.method == "POST":
        form = CampaignEditForm(request.POST, request.FILES)
        if form.is_valid():
            new_cluster_name = form.cleaned_data["new_video_cluster_name"].strip()
            selected_items = json.loads(form.cleaned_data["selected_items_json"])
            video_ids = _expand_selected_items_to_video_ids(selected_items)

            if not video_ids:
                form.add_error(None, "Please select at least one valid video or video-cluster.")
                return render(
                    request,
                    "publisher/edit_campaign_details.html",
                    {
                        "form": form,
                        "campaign": campaign,
                        "publisher": claims,
                        "show_auth_links": False,
                    },
                )

            # Uniqueness check for cluster name (excluding current cluster)
            if campaign.video_cluster_id:
                if VideoClusterLanguage.objects.filter(name__iexact=new_cluster_name).exclude(
                    video_cluster_id=campaign.video_cluster_id
                ).exists():
                    messages.error(
                        request,
                        "video cluster name already exists.  Write a different name",
                    )
                    return render(
                        request,
                        "publisher/edit_campaign_details.html",
                        {
                            "form": form,
                            "campaign": campaign,
                            "publisher": claims,
                            "show_auth_links": False,
                        },
                    )

            with transaction.atomic():
                cluster = campaign.video_cluster

                # Update cluster display/name
                if cluster and new_cluster_name:
                    cluster.display_name = new_cluster_name
                    cluster.search_keywords = new_cluster_name
                    cluster.save()

                    cl_en = VideoClusterLanguage.objects.filter(
                        video_cluster=cluster, language_code="en"
                    ).first()
                    if cl_en:
                        cl_en.name = new_cluster_name
                        cl_en.save()
                    else:
                        VideoClusterLanguage.objects.create(
                            video_cluster=cluster,
                            language_code="en",
                            name=new_cluster_name,
                        )

                # Replace cluster videos with desired set (simple + deterministic)
                if cluster:
                    VideoClusterVideo.objects.filter(video_cluster=cluster).delete()
                    videos = list(Video.objects.filter(id__in=video_ids).order_by("code"))
                    for idx, v in enumerate(videos, start=1):
                        VideoClusterVideo.objects.create(
                            video_cluster=cluster, video=v, sort_order=idx
                        )

                # Update campaign metadata
                campaign.new_video_cluster_name = new_cluster_name
                campaign.selection_json = form.cleaned_data["selected_items_json"]
                campaign.doctors_supported = form.cleaned_data["doctors_supported"]
                campaign.banner_target_url = form.cleaned_data["banner_target_url"]
                campaign.start_date = form.cleaned_data["start_date"]
                campaign.end_date = form.cleaned_data["end_date"]
                campaign.email_registration = form.cleaned_data["email_registration"]
                campaign.wa_addition = form.cleaned_data["wa_addition"]


                if form.cleaned_data.get("banner_small"):
                    campaign.banner_small = form.cleaned_data["banner_small"]
                if form.cleaned_data.get("banner_large"):
                    campaign.banner_large = form.cleaned_data["banner_large"]

                campaign.save()

            messages.success(request, "Campaign updated successfully.")
            return redirect(reverse("campaign_publisher:campaign_list"))
    else:
        form = CampaignEditForm(
            initial={
                "campaign_id": campaign.campaign_id,
                "new_video_cluster_name": campaign.new_video_cluster_name,
                "selected_items_json": campaign.selection_json or "[]",
                "doctors_supported": campaign.doctors_supported,
                "banner_target_url": campaign.banner_target_url,
                "start_date": campaign.start_date,
                "end_date": campaign.end_date,
                "email_registration": campaign.email_registration or "",
                "wa_addition": campaign.wa_addition or "",

            }
        )

    return render(
        request,
        "publisher/edit_campaign_details.html",
        {
            "form": form,
            "campaign": campaign,
            "publisher": claims,
            "show_auth_links": False,
        },
    )


# -----------------------------
# APIs for the add/edit screen UI
# -----------------------------

@publisher_required
@require_GET
def api_search_catalog(request: HttpRequest) -> JsonResponse:
    q = (request.GET.get("q") or "").strip()
    if not q or len(q) < 2:
        return JsonResponse({"results": []})

    videos = (
        Video.objects.filter(
            Q(code__icontains=q)
            | Q(search_keywords__icontains=q)
            | Q(languages__title__icontains=q)
        )
        .distinct()
        .order_by("code")[:20]
    )

    clusters = (
        VideoCluster.objects.filter(
            Q(code__icontains=q)
            | Q(display_name__icontains=q)
            | Q(search_keywords__icontains=q)
            | Q(languages__name__icontains=q)
        )
        .distinct()
        .order_by("code")[:20]
    )

    results: List[Dict[str, Any]] = []

    for v in videos:
        results.append(
            {
                "type": "video",
                "id": v.id,
                "code": v.code,
                "title": _video_title_en(v),
            }
        )

    for c in clusters:
        results.append(
            {
                "type": "cluster",
                "id": c.id,
                "code": c.code,
                "title": _cluster_name_en(c),
            }
        )

    return JsonResponse({"results": results})


@publisher_required
@require_POST
def api_expand_selection(request: HttpRequest) -> JsonResponse:
    try:
        payload = json.loads((request.body or b"").decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "invalid json"}, status=400)

    items = payload.get("items")
    if not isinstance(items, list):
        return JsonResponse({"error": "items must be a list"}, status=400)

    # Normalize items
    normalized: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        t = str(it.get("type") or "").lower().strip()
        if t not in ("video", "cluster"):
            continue
        try:
            _id = int(it.get("id"))
        except Exception:
            continue
        normalized.append({"type": t, "id": _id})

    video_ids = _expand_selected_items_to_video_ids(normalized)
    videos = list(Video.objects.filter(id__in=video_ids).order_by("code"))

    out = [{"id": v.id, "code": v.code, "title": _video_title_en(v)} for v in videos]
    return JsonResponse({"videos": out})
