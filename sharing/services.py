from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List

from django.conf import settings
from django.core.cache import cache

from catalog.constants import LANGUAGES
from catalog.models import (
    TherapyArea,
    Trigger,
    TriggerCluster,
    Video,
    VideoLanguage,
    VideoCluster,
    VideoClusterLanguage,
    VideoClusterVideo,
)


_CATALOG_CACHE_KEY = "clinic_catalog_payload_v6"
_CATALOG_CACHE_SECONDS = 60 * 60  # 1 hour


def build_whatsapp_message_prefixes(doctor_name: str) -> Dict[str, str]:
    doctor = (doctor_name or "").strip() or "your doctor"

    templates = {
        "en": (
            "Your doctor {doctor} has sent you the following video/videos. "
            "It is very important that you view them and follow the instructions, "
            "as these are for important observations by your doctor. "
            "Your child's health and wellbeing depend upon following the instructions in the videos."
        ),
        "hi": (
            "आपके डॉक्टर {doctor} ने आपको निम्न वीडियो/वीडियो भेजे हैं। "
            "कृपया इन्हें ध्यान से देखें और वीडियो में दिए गए निर्देशों का पालन करें, "
            "क्योंकि ये आपके डॉक्टर के महत्वपूर्ण निरीक्षणों के लिए हैं। "
            "आपके बच्चे का स्वास्थ्य और भलाई इन वीडियो में दिए गए निर्देशों का पालन करने पर निर्भर है।"
        ),
        "mr": (
            "तुमचे डॉक्टर {doctor} यांनी तुम्हाला खालील व्हिडिओ/व्हिडिओ पाठवले आहेत. "
            "कृपया ते काळजीपूर्वक पहा आणि व्हिडिओमध्ये दिलेल्या सूचनांचे पालन करा, "
            "कारण हे तुमच्या डॉक्टरांच्या महत्त्वाच्या निरीक्षणांसाठी आहेत. "
            "तुमच्या मुलाचे आरोग्य आणि कल्याण व्हिडिओमधील सूचनांचे पालन करण्यावर अवलंबून आहे."
        ),
    }

    out: Dict[str, str] = {}
    for code, _label in LANGUAGES:
        tmpl = templates.get(code, templates["en"])
        out[code] = tmpl.format(doctor=doctor)
    return out


def _build_catalog_payload() -> Dict[str, Any]:
    # -----------------------------------------------------------------
    # Therapy Areas
    # -----------------------------------------------------------------
    therapy_areas = list(
        TherapyArea.objects.filter(is_active=True).order_by("sort_order", "id")
    )
    therapy_payload = [
        {
            "code": ta.code,
            "display_name": ta.display_name,
            "description": ta.description,
        }
        for ta in therapy_areas
    ]
    therapy_by_id = {ta.id: ta for ta in therapy_areas}

    # -----------------------------------------------------------------
    # Topics (TriggerClusters)
    # -----------------------------------------------------------------
    topics = list(
        TriggerCluster.objects.filter(is_active=True).order_by("sort_order", "id")
    )
    topics_payload = [
        {
            "code": tc.code,
            "display_name": tc.display_name,
            "description": tc.description,
            "language_code": tc.language_code,
        }
        for tc in topics
    ]

    # -----------------------------------------------------------------
    # Bundles (VideoClusters)
    # -----------------------------------------------------------------
    bundles = list(
        VideoCluster.objects.filter(is_active=True)
        .select_related("trigger")
        .order_by("sort_order", "id")
    )

    bundle_names_by_code: Dict[str, Dict[str, str]] = defaultdict(dict)
    bundle_display_names_by_code: Dict[str, str] = {}
    bundle_topics_by_code: Dict[str, List[str]] = defaultdict(list)
    bundle_therapy_by_code: Dict[str, List[str]] = defaultdict(list)

    bundle_lang_rows = VideoClusterLanguage.objects.filter(
        video_cluster__in=bundles
    ).select_related("video_cluster")

    for row in bundle_lang_rows:
        bundle_names_by_code[row.video_cluster.code][row.language_code] = row.name

    for b in bundles:
        bundle_display_names_by_code[b.code] = b.display_name or b.code
        if b.trigger_id:
            bundle_topics_by_code[b.code].append(b.trigger.code)

            trig = (
                Trigger.objects.filter(cluster_id=b.trigger_id)
                .select_related("cluster", "primary_therapy")
                .first()
            )
            if trig and trig.primary_therapy_id:
                ta = therapy_by_id.get(trig.primary_therapy_id)
                if ta:
                    bundle_therapy_by_code[b.code].append(ta.code)

    bundles_payload = [
        {
            "code": b.code,
            "display_name": bundle_display_names_by_code.get(b.code, b.code),
            "names": bundle_names_by_code.get(b.code, {}),
            "topic_codes": bundle_topics_by_code.get(b.code, []),
            "therapy_codes": bundle_therapy_by_code.get(b.code, []),
        }
        for b in bundles
    ]

    # -----------------------------------------------------------------
    # Videos (+ localized titles + search text)
    # -----------------------------------------------------------------
    videos = list(Video.objects.filter(is_active=True).order_by("sort_order", "id"))

    vlang_rows = VideoLanguage.objects.filter(video__in=videos).select_related("video")

    titles_by_video_code: Dict[str, Dict[str, str]] = defaultdict(dict)
    url_by_video_code: Dict[str, Dict[str, str]] = defaultdict(dict)

    for row in vlang_rows:
        titles_by_video_code[row.video.code][row.language_code] = row.title
        url_by_video_code[row.video.code][row.language_code] = row.youtube_url

    trigger_map: Dict[str, List[str]] = defaultdict(list)
    topic_map: Dict[str, List[str]] = defaultdict(list)
    therapy_map: Dict[str, List[str]] = defaultdict(list)
    bundle_map: Dict[str, List[str]] = defaultdict(list)

    # -----------------------------------------------------------------
    # Video ↔ Trigger mapping (DO NOT use Trigger.video; it doesn't exist)
    # -----------------------------------------------------------------
    from catalog.models import VideoTriggerMap

    vtm_rows = (
        VideoTriggerMap.objects
        .select_related(
            "video",
            "trigger",
            "trigger__cluster",
            "trigger__primary_therapy",
        )
        .all()
    )

    for vtm in vtm_rows:
        video_code = vtm.video.code
        tr = vtm.trigger

        for field in (
            "display_name",
            "doctor_trigger_label",
            "subtopic_title",
            "search_keywords",
        ):
            val = getattr(tr, field, "") or ""
            if val:
                trigger_map[video_code].append(val.lower())

        if getattr(tr, "cluster_id", None):
            topic_map[video_code].append(tr.cluster.code)

        if getattr(tr, "primary_therapy_id", None):
            ta = therapy_by_id.get(tr.primary_therapy_id)
            if ta:
                therapy_map[video_code].append(ta.code)

    # -----------------------------------------------------------------
    # Video bundles
    # -----------------------------------------------------------------
    vc_rows = VideoClusterVideo.objects.filter(
        video_cluster__in=bundles
    ).select_related("video_cluster", "video")

    for row in vc_rows:
        bundle_map[row.video.code].append(row.video_cluster.code)

    # -----------------------------------------------------------------
    # Build videos payload
    # -----------------------------------------------------------------
    videos_payload = []
    for v in videos:
        code = v.code
        titles = titles_by_video_code.get(code, {})
        if "en" not in titles:
            titles["en"] = v.display_name or code

        search_parts = []
        search_parts.extend([t.lower() for t in titles.values() if t])
        search_parts.extend(trigger_map.get(code, []))

        for bcode in bundle_map.get(code, []):
            search_parts.append(bcode.lower())
            search_parts.append(
                (bundle_display_names_by_code.get(bcode, "") or "").lower()
            )
            for lang_name in bundle_names_by_code.get(bcode, {}).values():
                if lang_name:
                    search_parts.append(lang_name.lower())

        videos_payload.append(
            {
                "id": code,
                "display_name": v.display_name or code,
                "titles": titles,
                "urls": url_by_video_code.get(code, {}),
                "topic_codes": topic_map.get(code, []),
                "therapy_codes": therapy_map.get(code, []),
                "bundle_codes": bundle_map.get(code, []),
                "search_text": " ".join(sorted(set(p for p in search_parts if p))),
            }
        )

    return {
        "therapy_areas": therapy_payload,
        "topics": topics_payload,
        "bundles": bundles_payload,
        "videos": videos_payload,
        "message_prefixes": build_whatsapp_message_prefixes("your doctor"),
    }


def get_catalog_json_cached(force_refresh: bool = False) -> Dict[str, Any]:
    cache_seconds = getattr(settings, "CATALOG_CACHE_SECONDS", _CATALOG_CACHE_SECONDS)

    if not force_refresh:
        cached = cache.get(_CATALOG_CACHE_KEY)
        if cached:
            return cached

    payload = _build_catalog_payload()
    cache.set(_CATALOG_CACHE_KEY, payload, cache_seconds)
    return payload
