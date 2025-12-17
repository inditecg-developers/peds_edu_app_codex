from __future__ import annotations


import hashlib
from django.core.cache import cache

from catalog.constants import LANGUAGE_CODES
import re
from typing import Any, Dict, List

from django.conf import settings
from django.core.cache import cache

from catalog.constants import LANGUAGE_CODES, LANGUAGES
from catalog.models import (
    TriggerCluster,
    Trigger,
    Video,
    VideoLanguage,
    VideoCluster,
    VideoClusterLanguage,
    VideoClusterVideo,
    VideoTriggerMap,
)


def get_catalog_json_cached() -> Dict[str, Any]:
    """Return a JSON-serializable structure for the doctor sharing UI.

    This is cached in Django's cache to avoid repeated DB hits.
    """

    def build() -> Dict[str, Any]:
        # Load clusters (chips)
        clusters_qs = TriggerCluster.objects.filter(is_active=True).order_by("sort_order")
        clusters = [{"code": c.code, "display_name": c.display_name} for c in clusters_qs]

        # Load triggers
        triggers_qs = (
            Trigger.objects.filter(is_active=True)
            .select_related("cluster", "primary_therapy")
            .order_by("doctor_trigger_label")
        )
        triggers = list(triggers_qs)
        trigger_codes = [t.code for t in triggers]

        # Published videos + English titles
        videos_qs = Video.objects.filter(is_published=True).select_related("primary_trigger")
        videos = {v.id: v for v in videos_qs}
        video_ids = list(videos.keys())

        video_titles_en = {
            vl.video_id: vl.title
            for vl in VideoLanguage.objects.filter(video_id__in=video_ids, language_code="en")
        }

        # Map: trigger_id -> list(video)
        trigger_to_videos: Dict[int, List[Video]] = {}
        for v in videos.values():
            if v.primary_trigger_id:
                trigger_to_videos.setdefault(v.primary_trigger_id, []).append(v)

        # Add additional mappings (video_trigger_map)
        for m in VideoTriggerMap.objects.filter(trigger__code__in=trigger_codes).select_related("video"):
            if m.video.is_published:
                trigger_to_videos.setdefault(m.trigger_id, []).append(m.video)

        # Deduplicate videos per trigger while preserving order by code
        for tid, lst in trigger_to_videos.items():
            seen = set()
            dedup = []
            for v in sorted(lst, key=lambda x: x.code):
                if v.id not in seen:
                    seen.add(v.id)
                    dedup.append(v)
            trigger_to_videos[tid] = dedup

        # Published video clusters + English names
        clusters_qs2 = VideoCluster.objects.filter(is_published=True).select_related("trigger")
        vclusters_by_trigger: Dict[int, List[VideoCluster]] = {}
        vcluster_ids = []
        for vc in clusters_qs2:
            vclusters_by_trigger.setdefault(vc.trigger_id, []).append(vc)
            vcluster_ids.append(vc.id)

        vcluster_names_en = {
            vcl.video_cluster_id: vcl.name
            for vcl in VideoClusterLanguage.objects.filter(video_cluster_id__in=vcluster_ids, language_code="en")
        }

        # cluster_id -> ordered video list
        cluster_videos: Dict[int, List[Dict[str, Any]]] = {}
        q = (
            VideoClusterVideo.objects.filter(video_cluster_id__in=vcluster_ids)
            .select_related("video")
            .order_by("video_cluster_id", "sort_order")
        )
        for row in q:
            if not row.video.is_published:
                continue
            cluster_videos.setdefault(row.video_cluster_id, []).append(
                {
                    "video_code": row.video.code,
                    "title_en": video_titles_en.get(row.video_id, row.video.code),
                }
            )

        # Build trigger JSON
        triggers_json: List[Dict[str, Any]] = []
        for t in triggers:
            vids = trigger_to_videos.get(t.id, [])
            vids_json = [
                {
                    "type": "video",
                    "code": v.code,
                    "title_en": video_titles_en.get(v.id, v.code),
                    "thumbnail_url": v.thumbnail_url,
                }
                for v in vids
            ]

            vc_list = sorted(vclusters_by_trigger.get(t.id, []), key=lambda x: x.sort_order)
            vc_json = [
                {
                    "type": "cluster",
                    "code": vc.code,
                    "name_en": vcluster_names_en.get(vc.id, vc.code),
                    "videos": cluster_videos.get(vc.id, []),
                }
                for vc in vc_list
            ]

            triggers_json.append(
                {
                    "code": t.code,
                    "cluster_code": t.cluster.code,
                    "cluster_name": t.cluster.display_name,
                    "therapy_area": t.primary_therapy.display_name,
                    "doctor_label": t.doctor_trigger_label,
                    "subtopic_title": t.subtopic_title,
                    "search_keywords": t.search_keywords or "",
                    "items": {
                        "videos": vids_json,
                        "video_clusters": vc_json,
                    },
                }
            )

        return {
            "clusters": clusters,
            "triggers": triggers_json,
            "languages": [{"code": c, "name": n} for c, n in LANGUAGES],
        }

    return cache.get_or_set("catalog_json_v1", build, timeout=settings.CATALOG_CACHE_SECONDS)


class TranslitEngines:
    def __init__(self):
        self.available = False
        self._XlitEngine = None
        self._engines = {}
        try:
            from ai4bharat.transliteration import XlitEngine  # type: ignore

            self._XlitEngine = XlitEngine
            self.available = True
        except Exception:
            self.available = False

    def translit_sentence(self, sentence: str, lang: str) -> str:
        if lang == "en":
            return sentence
        if not self.available:
            return sentence
        try:
            engine = self._engines.get(lang)
            if engine is None:
                engine = self._XlitEngine(lang, beam_width=6, rescore=True)
                self._engines[lang] = engine
            cleaned = sentence.replace("&", "and")
            # Keep punctuation minimally for WhatsApp readability; remove only exotic chars
            cleaned = re.sub(r"[^0-9A-Za-z\s.,'!?-]", " ", cleaned)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            out = engine.translit_sentence(cleaned)
            if isinstance(out, dict) and lang in out and out[lang]:
                return out[lang]
            return sentence
        except Exception:
            return sentence




# WhatsApp prefix templates (native language; doctor name kept as entered)
_WA_PREFIX_TEMPLATES = {
    "en": (
        "Your doctor {doctor_name} has sent you the following video/videos. "
        "It is very important that you view them and follow the instructions, as these are for important observations by your doctor. "
        "Your child's health and wellbeing depend upon following the instructions in the videos."
    ),
    "hi": (
        "आपके डॉक्टर {doctor_name} ने आपको निम्न वीडियो/वीडियो भेजे हैं। "
        "कृपया इन्हें ध्यान से देखें और वीडियो में दिए गए निर्देशों का पालन करें, क्योंकि ये आपके डॉक्टर के महत्वपूर्ण निरीक्षणों के लिए हैं। "
        "आपके बच्चे का स्वास्थ्य और भलाई इन वीडियो में दिए गए निर्देशों का पालन करने पर निर्भर है।"
    ),
    "te": (
        "మీ డాక్టర్ {doctor_name} మీరు చూడాల్సిన వీడియో/వీడియోలను పంపించారు. "
        "దయచేసి ఇవి తప్పనిసరిగా చూసి, వీడియోల్లో చెప్పిన సూచనలను పాటించండి—ఇవి మీ డాక్టర్‌కు అవసరమైన ముఖ్యమైన పరిశీలనల కోసం ఉన్నాయి. "
        "మీ పిల్లల ఆరోగ్యం మరియు శ్రేయస్సు ఈ సూచనలను పాటించడంపై ఆధారపడి ఉంటుంది."
    ),
    "ml": (
        "നിങ്ങളുടെ ഡോക്ടർ {doctor_name} നിങ്ങളിലേക്ക് താഴെ പറയുന്ന വീഡിയോ/വീഡിയോകൾ അയച്ചിട്ടുണ്ട്. "
        "ദയവായി ഇവ ശ്രദ്ധपूर्वം കാണുകയും വീഡിയോയിൽ പറഞ്ഞിരിക്കുന്ന നിർദ്ദേശങ്ങൾ പാലിക്കുകയും ചെയ്യുക—ഇവ നിങ്ങളുടെ ഡോക്ടറുടെ പ്രധാന നിരീക്ഷണങ്ങൾക്കായി ആണ്. "
        "നിങ്ങളുടെ കുട്ടിയുടെ ആരോഗ്യവും ക്ഷേമവും ഈ നിർദ്ദേശങ്ങൾ പാലിക്കുന്നതിനെ ആശ്രയിച്ചിരിക്കുന്നു."
    ),
    "mr": (
        "तुमच्या डॉक्टरांनी {doctor_name} तुम्हाला खालील व्हिडिओ/व्हिडिओ पाठवले आहेत. "
        "कृपया ते काळजीपूर्वक पहा आणि व्हिडिओमधील सूचना पाळा, कारण हे तुमच्या डॉक्टरांच्या महत्त्वाच्या निरीक्षणांसाठी आहेत. "
        "तुमच्या मुलाचे आरोग्य आणि कल्याण या सूचनांचे पालन करण्यावर अवलंबून आहे."
    ),
    "kn": (
        "ನಿಮ್ಮ ವೈದ್ಯರಾದ {doctor_name} ನಿಮಗೆ ಕೆಳಗಿನ ವೀಡಿಯೊ/ವೀಡಿಯೊಗಳನ್ನು ಕಳುಹಿಸಿದ್ದಾರೆ. "
        "ದಯವಿಟ್ಟು ಅವನ್ನು ಗಮನದಿಂದ ನೋಡಿ ಮತ್ತು ವೀಡಿಯೊಗಳಲ್ಲಿ ನೀಡಿರುವ ಸೂಚನೆಗಳನ್ನು ಅನುಸರಿಸಿ—ಇವು ನಿಮ್ಮ ವೈದ್ಯರಿಗೆ ಅಗತ್ಯವಾದ ಮಹತ್ವದ ಗಮನಿಕೆಗಳಿಗಾಗಿ. "
        "ನಿಮ್ಮ ಮಗುವಿನ ಆರೋಗ್ಯ ಮತ್ತು ಸುಖಸಮೃದ್ಧಿ ಈ ಸೂಚನೆಗಳನ್ನು ಪಾಲಿಸುವುದರ ಮೇಲೆ ಅವಲಂಬಿತವಾಗಿದೆ."
    ),
    "ta": (
        "உங்கள் மருத்துவர் {doctor_name} உங்களுக்கு கீழ்க்காணும் வீடியோ/வீடியோக்களை அனுப்பியுள்ளார். "
        "தயவுசெய்து அவற்றை கவனமாக பார்த்து, வீடியோவில் கூறியுள்ள வழிமுறைகளைப் பின்பற்றவும்—இவை உங்கள் மருத்துவரின் முக்கியமான கவனிப்புகளுக்காக அனுப்பப்பட்டவை. "
        "உங்கள் குழந்தையின் ஆரோக்கியமும் நலனும் இந்த வழிமுறைகளைப் பின்பற்றுவதில் சார்ந்துள்ளது."
    ),
    "bn": (
        "আপনার ডাক্তার {doctor_name} আপনাকে নিচের ভিডিও/ভিডিওগুলো পাঠিয়েছেন। "
        "অনুগ্রহ করে এগুলো মনোযোগ দিয়ে দেখুন এবং ভিডিওতে দেওয়া নির্দেশনা মেনে চলুন, কারণ এগুলো আপনার ডাক্তারের গুরুত্বপূর্ণ পর্যবেক্ষণের জন্য। "
        "আপনার শিশুর স্বাস্থ্য ও সুস্থতা এই নির্দেশনাগুলো অনুসরণ করার ওপর নির্ভর করে।"
    ),
}


def build_whatsapp_message_prefixes(doctor_name: str) -> dict[str, str]:
    doctor_name = (doctor_name or "").strip() or "your doctor"

    # cache per doctor_name (fast + avoids recompute)
    h = hashlib.sha1(doctor_name.encode("utf-8")).hexdigest()[:12]
    cache_key = f"wa_prefixes_v2_{h}"
    cached = cache.get(cache_key)
    if isinstance(cached, dict) and cached:
        return cached

    out: dict[str, str] = {}
    for lc in LANGUAGE_CODES:
        tmpl = _WA_PREFIX_TEMPLATES.get(lc) or _WA_PREFIX_TEMPLATES["en"]
        out[lc] = tmpl.format(doctor_name=doctor_name)

    cache.set(cache_key, out, 24 * 3600)
    return out

