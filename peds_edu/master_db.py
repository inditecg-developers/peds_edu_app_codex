from __future__ import annotations

import re
import secrets
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional, Tuple, List

from django.conf import settings
from django.contrib.auth.hashers import check_password, identify_hasher, make_password
from django.core import signing
from django.db import connections


def _get_banner_target_url_from_local_publisher_campaign(campaign_id: str) -> Optional[str]:
    cid = (campaign_id or "").strip().replace("-", "")
    if not cid:
        return None

    try:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                """
                SELECT banner_target_url
                FROM publisher_campaign
                WHERE REPLACE(campaign_id, '-', '') = %s
                LIMIT 1
                """,
                [cid],
            )
            row = cursor.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception:
        return None

    return None


@dataclass(frozen=True)
class MasterDoctorAuthResult:
    ok: bool
    doctor_id: str
    error: str = ""


# -------------------------------------------------------------------
# MASTER DB helpers
# -------------------------------------------------------------------

def _master_alias() -> str:
    return getattr(settings, "MASTER_DB_ALIAS", "MASTER_DB_ALIAS")


def _master_table_columns(table: str) -> List[str]:
    """
    Return column names for a MASTER DB table (best-effort).
    """
    try:
        conn = connections[_master_alias()]
        schema = (conn.settings_dict.get("NAME") or "").strip()
        if not schema:
            return []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                [schema, table],
            )
            rows = cur.fetchall()
        return [str(r[0]) for r in (rows or []) if r and r[0]]
    except Exception:
        return []


def _uuid_hex_to_hyphenated(u: str) -> str:
    s = (u or "").strip().replace("-", "")
    if len(s) != 32:
        return u
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"


# -------------------------------------------------------------------
# Master doctor fetch + template shaping
# -------------------------------------------------------------------

def fetch_master_doctor_row_by_id(doctor_id: str) -> Optional[Dict[str, Any]]:
    """
    Read a doctor row from MASTER redflags_doctor (or configured table/columns).
    Returns a dict of raw columns used by master_row_to_template_context.
    """
    doctor_id = (doctor_id or "").strip()
    if not doctor_id:
        return None

    table = getattr(settings, "MASTER_DB_DOCTOR_TABLE", "redflags_doctor")

    # Column names (defaults match your export)
    id_col = getattr(settings, "MASTER_DB_DOCTOR_ID_COLUMN", "doctor_id")
    fn_col = getattr(settings, "MASTER_DB_DOCTOR_FIRST_NAME_COLUMN", "first_name")
    ln_col = getattr(settings, "MASTER_DB_DOCTOR_LAST_NAME_COLUMN", "last_name")
    email_col = getattr(settings, "MASTER_DB_DOCTOR_EMAIL_COLUMN", "email")
    wa_col = getattr(settings, "MASTER_DB_DOCTOR_WHATSAPP_COLUMN", "whatsapp_no")

    clinic_name_col = getattr(settings, "MASTER_DB_DOCTOR_CLINIC_NAME_COLUMN", "clinic_name")
    clinic_phone_col = getattr(settings, "MASTER_DB_DOCTOR_CLINIC_PHONE_COLUMN", "clinic_phone")
    clinic_appt_col = getattr(settings, "MASTER_DB_DOCTOR_CLINIC_APPOINTMENT_COLUMN", "clinic_appointment_number")
    clinic_addr_col = getattr(settings, "MASTER_DB_DOCTOR_CLINIC_ADDRESS_COLUMN", "clinic_address")
    postal_col = getattr(settings, "MASTER_DB_DOCTOR_POSTAL_COLUMN", "postal_code")
    state_col = getattr(settings, "MASTER_DB_DOCTOR_STATE_COLUMN", "state")
    district_col = getattr(settings, "MASTER_DB_DOCTOR_DISTRICT_COLUMN", "district")
    photo_col = getattr(settings, "MASTER_DB_DOCTOR_PHOTO_COLUMN", "photo_path")

    clinic_user1_email_col = getattr(settings, "MASTER_DB_DOCTOR_CLINIC_USER1_EMAIL_COLUMN", "clinic_user1_email")
    clinic_user2_email_col = getattr(settings, "MASTER_DB_DOCTOR_CLINIC_USER2_EMAIL_COLUMN", "clinic_user2_email")
    clinic_user3_email_col = getattr(settings, "MASTER_DB_DOCTOR_CLINIC_USER3_EMAIL_COLUMN", "clinic_user3_email")

    sql = f"""
        SELECT
            {id_col},
            {fn_col},
            {ln_col},
            {email_col},
            {wa_col},
            {clinic_name_col},
            {clinic_phone_col},
            {clinic_appt_col},
            {clinic_addr_col},
            {postal_col},
            {state_col},
            {district_col},
            {photo_col},
            {clinic_user1_email_col},
            {clinic_user2_email_col},
            {clinic_user3_email_col}
        FROM {table}
        WHERE {id_col} = %s
        LIMIT 1
    """

    try:
        with connections[_master_alias()].cursor() as cursor:
            cursor.execute(sql, [doctor_id])
            row = cursor.fetchone()
    except Exception:
        return None

    if not row:
        return None

    (
        did,
        fn,
        ln,
        em,
        wa,
        clinic_name,
        clinic_phone,
        clinic_appt,
        clinic_addr,
        postal,
        state,
        district,
        photo_path,
        u1,
        u2,
        u3,
    ) = row

    return {
        "doctor_id": did,
        "first_name": fn,
        "last_name": ln,
        "email": em,
        "whatsapp_no": wa,
        "clinic_name": clinic_name,
        "clinic_phone": clinic_phone,
        "clinic_appointment_number": clinic_appt,
        "clinic_address": clinic_addr,
        "postal_code": postal,
        "state": state,
        "district": district,
        "photo_path": photo_path,
        "clinic_user1_email": u1,
        "clinic_user2_email": u2,
        "clinic_user3_email": u3,
    }


_PIN_STATE_MAP = {
    # minimal fast mapping, updated elsewhere in your codebase; kept as-is
}


def _normalize_pin(pin: str) -> str:
    return re.sub(r"\D", "", str(pin or "")).strip()


def _state_from_pin(pin: str, fallback_state: str) -> str:
    p = _normalize_pin(pin)
    if len(p) >= 2:
        prefix = p[:2]
        if prefix in _PIN_STATE_MAP:
            return _PIN_STATE_MAP[prefix]
    return (fallback_state or "").strip()


def master_row_to_template_context(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Convert raw MASTER doctor row into template-safe doctor + clinic dicts.
    """
    first_name = str(row.get("first_name") or "").strip()
    last_name = str(row.get("last_name") or "").strip()
    full_name = (f"{first_name} {last_name}").strip()

    doctor_email = str(row.get("email") or "").strip()
    whatsapp_no = str(row.get("whatsapp_no") or "").strip()

    clinic_name = str(row.get("clinic_name") or "").strip()
    clinic_phone = str(row.get("clinic_phone") or "").strip() or str(row.get("clinic_appointment_number") or "").strip()
    clinic_address = str(row.get("clinic_address") or "").strip()
    postal_code = str(row.get("postal_code") or "").strip()

    state = _state_from_pin(postal_code, str(row.get("state") or "").strip())
    district = str(row.get("district") or "").strip()

    photo_path = str(row.get("photo_path") or "").strip() or None

    clinic = {
        "display_name": clinic_name or "Clinic",
        "clinic_phone": clinic_phone,
        "clinic_whatsapp_number": whatsapp_no,
        "address_text": clinic_address,
        "postal_code": postal_code,
        "state": state,
        "district": district,
    }

    doctor = {
        "doctor_id": str(row.get("doctor_id") or "").strip(),
        "photo": photo_path,
        "whatsapp_number": whatsapp_no,
        "user": {
            "full_name": full_name or "Doctor",
            "email": doctor_email,
        },
        "clinic": clinic,
    }

    return doctor, clinic


# -------------------------------------------------------------------
# Patient payload signing (used by patient pages)
# -------------------------------------------------------------------

_PATIENT_SIGN_SALT = getattr(settings, "PATIENT_SIGN_SALT", "patient-link-v1")
_PATIENT_SIGN_MAX_AGE_SECONDS = int(getattr(settings, "PATIENT_SIGN_MAX_AGE_SECONDS", 60 * 60 * 24 * 7))


def build_patient_link_payload(doctor: Dict[str, Any], clinic: Dict[str, Any]) -> Dict[str, Any]:
    return {"doctor": doctor, "clinic": clinic}


def sign_patient_payload(payload: Dict[str, Any]) -> str:
    return signing.dumps(payload, salt=_PATIENT_SIGN_SALT)


def unsign_patient_payload(token: str) -> Dict[str, Any]:
    if not token:
        return {}
    try:
        return signing.loads(token, salt=_PATIENT_SIGN_SALT, max_age=_PATIENT_SIGN_MAX_AGE_SECONDS)
    except Exception:
        return {}


# -------------------------------------------------------------------
# Campaign support + banner lookup
# -------------------------------------------------------------------

def resolve_campaign_video_cluster(campaign_id: str, campaign_name_fallback: str = "") -> str:
    """
    In Project-2, the video cluster name is stored in local default DB publisher_campaign.new_video_cluster_name.
    If missing, fall back to campaign name.
    """
    cid = (campaign_id or "").strip().replace("-", "")
    if not cid:
        return campaign_name_fallback or ""

    try:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                """
                SELECT new_video_cluster_name
                FROM publisher_campaign
                WHERE REPLACE(campaign_id, '-', '') = %s
                LIMIT 1
                """,
                [cid],
            )
            row = cursor.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception:
        pass

    return campaign_name_fallback or ""


def fetch_pe_campaign_support_for_doctor_email(
    email: str,
    *,
    extra_emails: "Sequence[str]" = (),
    phones: "Sequence[str]" = (),
) -> List[Dict[str, str]]:
    """
    Return PE-campaign acknowledgements + banner URLs for a doctor/clinic user.

    Matching logic (robust):
      - Primary match is campaign_doctor.email (case-insensitive).
      - If not present / not matching (common when staff logs in via clinic_user email),
        we also try additional emails and phone numbers.
      - Phone match uses last-10-digits comparison against campaign_doctor.phone.

    Output keys per item:
      - campaign_id, campaign_name, video_cluster, brand, banner_small_url, banner_large_url, banner_target_url
    """

    def _norm_emails(values: "Sequence[str]") -> List[str]:
        seen = set()
        out_: List[str] = []
        for v in values or ():
            s = (v or "").strip().lower()
            if s and s not in seen:
                seen.add(s)
                out_.append(s)
        return out_

    def _norm_phones(values: "Sequence[str]") -> List[str]:
        seen = set()
        out_: List[str] = []
        for v in values or ():
            digits = re.sub(r"\D", "", str(v or ""))
            if not digits:
                continue
            last10 = digits[-10:] if len(digits) > 10 else digits
            if last10 and last10 not in seen:
                seen.add(last10)
                out_.append(last10)
        return out_

    primary_email = (email or "").strip()
    email_candidates = _norm_emails([primary_email, *list(extra_emails or [])])
    phone_candidates = _norm_phones(list(phones or []))

    if not email_candidates and not phone_candidates:
        return []

    where_parts: List[str] = []
    params: List[Any] = []

    if email_candidates:
        where_parts.append(
            "LOWER(d.email) IN (" + ",".join(["%s"] * len(email_candidates)) + ")"
        )
        params.extend(email_candidates)

    if phone_candidates:
        where_parts.append(
            "RIGHT(d.phone, 10) IN (" + ",".join(["%s"] * len(phone_candidates)) + ")"
        )
        params.extend(phone_candidates)

    where_sql = " OR ".join(where_parts)

    # Enrollment table schema differs across environments; include e.active filter only if the column exists.
    try:
        enrollment_cols = [c.lower() for c in _master_table_columns("campaign_doctorcampaignenrollment")]
    except Exception:
        enrollment_cols = []
    active_clause = " AND e.active = 1" if ("active" in enrollment_cols) else ""

    sql = f"""
        SELECT
            c.id,
            c.name,
            c.banner_small_url,
            c.banner_large_url,
            c.banner_target_url,
            COALESCE(b.name, '') AS brand_name
        FROM campaign_doctor d
        JOIN campaign_doctorcampaignenrollment e ON e.doctor_id = d.id
        JOIN campaign_campaign c ON c.id = e.campaign_id
        LEFT JOIN campaign_brand b ON b.id = c.brand_id
        WHERE ({where_sql})
          AND c.system_pe = 1{active_clause}
        ORDER BY c.start_date DESC, c.created_at DESC, c.id ASC
    """

    with connections[_master_alias()].cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()

    out: List[Dict[str, str]] = []
    seen_campaign_ids = set()

    for r in rows or []:
        cid = (r[0] or "")
        if not cid or cid in seen_campaign_ids:
            continue
        seen_campaign_ids.add(cid)

        cname = str(r[1] or "").strip()
        vcluster = resolve_campaign_video_cluster(campaign_id=str(cid), campaign_name_fallback=cname)
        brand = str(r[5] or "").strip()

        banner_small_url = str(r[2] or "").strip()
        banner_large_url = str(r[3] or "").strip()
        banner_target_url = str(r[4] or "").strip()

        if not banner_target_url:
            try:
                banner_target_url = _get_banner_target_url_from_local_publisher_campaign(str(cid)) or ""
            except Exception:
                banner_target_url = ""

        out.append(
            {
                "campaign_id": str(cid),
                "campaign_name": cname,
                "video_cluster": vcluster,
                "brand": brand,
                "banner_small_url": banner_small_url,
                "banner_large_url": banner_large_url,
                "banner_target_url": banner_target_url,
            }
        )

    return out
