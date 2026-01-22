from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

from .models import INDIA_STATES_AND_UTS


PINCODE_DIRECTORY_PATH = Path(__file__).resolve().parent / "data" / "india_pincode_directory.json"


class IndiaPincodeDirectoryNotReady(RuntimeError):
    """Raised when the pincode directory JSON is missing/unreadable."""


# Common synonyms / spelling variants => project canonical names (must match INDIA_STATES_AND_UTS)
_STATE_NORMALIZATION = {
    "NCT of Delhi": "Delhi",
    "Delhi NCR": "Delhi",
    "Orissa": "Odisha",
    "Pondicherry": "Puducherry",
    "Dadra and Nagar Haveli": "Dadra and Nagar Haveli and Daman and Diu",
    "Daman and Diu": "Dadra and Nagar Haveli and Daman and Diu",
    "Dadra & Nagar Haveli": "Dadra and Nagar Haveli and Daman and Diu",
    "Dadra & Nagar Haveli and Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu",
    "Jammu & Kashmir": "Jammu and Kashmir",
    "Andaman & Nicobar Islands": "Andaman and Nicobar Islands",
}


def _canon_state_name(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""

    # Normalize spacing and punctuation
    s = re.sub(r"\s+", " ", s)
    s = s.replace("&", "and").strip()

    # Synonyms
    for k, v in _STATE_NORMALIZATION.items():
        if s.lower() == k.lower():
            s = v
            break

    # Match against canonical list ignoring case
    for canon in INDIA_STATES_AND_UTS:
        if s.lower() == canon.lower():
            return canon

    return s


@lru_cache(maxsize=1)
def load_pincode_directory() -> dict[str, str]:
    """Load {"110001": "Delhi", ...} mapping from JSON."""
    if not PINCODE_DIRECTORY_PATH.exists():
        raise IndiaPincodeDirectoryNotReady(
            f"Missing pincode directory JSON at {PINCODE_DIRECTORY_PATH}. "
            "Create it (full Indian PIN directory) or run: "
            "python manage.py build_pincode_directory --input <csv_path>"
        )

    try:
        with PINCODE_DIRECTORY_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise IndiaPincodeDirectoryNotReady(
            f"Unable to read pincode directory JSON at {PINCODE_DIRECTORY_PATH}: {e}"
        )

    mapping: dict[str, str] = {}

    # Preferred format: dict
    if isinstance(data, dict):
        for k, v in data.items():
            pin = re.sub(r"\D", "", str(k or ""))
            if not re.fullmatch(r"\d{6}", pin):
                continue
            state = _canon_state_name(str(v or ""))
            if not state:
                continue
            mapping[pin] = state
        return mapping

    # Alternative format: list of objects
    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            pin = re.sub(r"\D", "", str(row.get("pincode") or row.get("pin") or row.get("postal_code") or ""))
            if not re.fullmatch(r"\d{6}", pin):
                continue
            state = _canon_state_name(str(row.get("state") or row.get("State") or row.get("state_name") or ""))
            if not state:
                continue
            mapping[pin] = state
        return mapping

    raise IndiaPincodeDirectoryNotReady(
        f"Unsupported pincode directory JSON format at {PINCODE_DIRECTORY_PATH}. "
        "Expected a dict or list of objects."
    )


def get_state_for_pincode(pincode: str) -> Optional[str]:
    """Return canonical state name for a 6-digit pincode, or None if not found."""
    pin = re.sub(r"\D", "", str(pincode or ""))
    if not re.fullmatch(r"\d{6}", pin):
        return None
    directory = load_pincode_directory()
    state = directory.get(pin)
    if not state:
        return None
    return _canon_state_name(state)

import os
import urllib.request

def get_district_for_pincode(pincode: str) -> Optional[str]:
    """
    Placeholder implementation to fetch district from PIN:
    - Uses India Post public API by default.
    - Replace with your internal PIN master / dataset if you have one.

    Control with env:
      PINCODE_DISTRICT_LOOKUP_MODE = "india_post_api" | "none"
    """
    mode = (os.getenv("PINCODE_DISTRICT_LOOKUP_MODE", "india_post_api") or "").strip().lower()
    if mode in ("none", "off", "0"):
        return None

    pin = re.sub(r"\D", "", str(pincode or ""))
    if not re.fullmatch(r"\d{6}", pin):
        return None

    try:
        url = f"https://api.postalpincode.in/pincode/{pin}"
        with urllib.request.urlopen(url, timeout=3) as resp:
            payload = resp.read().decode("utf-8")
        data = json.loads(payload)
        if not isinstance(data, list) or not data:
            return None
        item = data[0] if isinstance(data[0], dict) else {}
        pos = item.get("PostOffice") or []
        if not pos or not isinstance(pos, list):
            return None
        po0 = pos[0] if isinstance(pos[0], dict) else {}
        district = (po0.get("District") or "").strip()
        return district or None
    except Exception:
        return None


def get_state_and_district_for_pincode(pincode: str) -> tuple[Optional[str], Optional[str]]:
    return get_state_for_pincode(pincode), get_district_for_pincode(pincode)
