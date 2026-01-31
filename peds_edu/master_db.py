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
    """Normalized identity+auth result for a doctor/clinic-staff login attempt."""
    doctor_id: str
    login_email: str
    role: Literal["doctor", "clinic_user1", "clinic_user2"]
    display_name: str       # name to show in portal header/session (doctor or staff)
    doctor_full_name: str   # doctor's name for patient-facing messaging
    row: Dict[str, Any]     # raw DB row dict (all columns)


def _safe_identifier(name: str) -> str:
    """
    Validate SQL identifier (table/column) to reduce injection risk.
    Only allows letters, numbers, underscore.
    """
    if not re.match(r"^[A-Za-z0-9_]+$", name or ""):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def _master_alias() -> str:
    return getattr(settings, "MASTER_DB_ALIAS", "master")


def _doctor_table() -> str:
    return _safe_identifier(getattr(settings, "MASTER_DOCTOR_TABLE", "redflags_doctor"))


def _field_map() -> Dict[str, str]:
    """
    Column mapping (logical -> physical column name).
    Override any/all via settings.MASTER_DOCTOR_FIELD_MAP.
    """
    default = {
        # identity / names
        "doctor_id": "doctor_id",
        "first_name": "first_name",
        "last_name": "last_name",
        "email": "email",
        "whatsapp_no": "whatsapp_no",

        # clinic display
        "clinic_name": "clinic_name",
        "clinic_phone": "clinic_phone",
        "clinic_whatsapp": "receptionist_whatsapp_number",
        "clinic_address": "clinic_address",
        "state": "state",
        "postal_code": "postal_code",

        # regulatory
        "imc_number": "imc_registration_number",

        # password fields
        "doctor_password": "clinic_password_hash",
        "user1_email": "clinic_user1_email",
        "user1_name": "clinic_user1_name",
        "user1_password": "clinic_user1_password_hash",
        "user2_email": "clinic_user2_email",
        "user2_name": "clinic_user2_name",
        "user2_password": "clinic_user2_password_hash",

        # optional timestamp for password set events
        "doctor_password_set_at": "clinic_password_set_at",
    }
    override = getattr(settings, "MASTER_DOCTOR_FIELD_MAP", None)
    if isinstance(override, dict):
        default.update({k: str(v) for k, v in override.items() if v})

    # Validate identifiers
    for k, v in list(default.items()):
        default[k] = _safe_identifier(v)
    return default


def _dictfetchone(cursor) -> Optional[Dict[str, Any]]:
    row = cursor.fetchone()
    if not row:
        return None
    cols = [c[0] for c in cursor.description]
    return {cols[i]: row[i] for i in range(len(cols))}


def fetch_master_doctor_row_by_id(doctor_id: str) -> Optional[Dict[str, Any]]:
    fm = _field_map()
    table = _doctor_table()
    with connections[_master_alias()].cursor() as cursor:
        cursor.execute(
            f"SELECT * FROM `{table}` WHERE `{fm['doctor_id']}` = %s LIMIT 1",
            [doctor_id],
        )
        return _dictfetchone(cursor)


def fetch_master_doctor_row_by_email(email: str) -> Optional[Dict[str, Any]]:
    """
    Finds the row where email matches one of:
      - doctor email
      - clinic_user1_email
      - clinic_user2_email
    """
    fm = _field_map()
    table = _doctor_table()
    e = (email or "").strip().lower()
    if not e:
        return None

    with connections[_master_alias()].cursor() as cursor:
        cursor.execute(
            f"""
            SELECT * FROM `{table}`
            WHERE LOWER(`{fm['email']}`) = %s
               OR LOWER(`{fm['user1_email']}`) = %s
               OR LOWER(`{fm['user2_email']}`) = %s
            LIMIT 1
            """,
            [e, e, e],
        )
        return _dictfetchone(cursor)


def _normalize_full_name(first: str, last: str) -> str:
    parts = [p.strip() for p in [first or "", last or ""] if p and p.strip()]
    return " ".join(parts).strip()


def looks_like_hash(stored: str) -> bool:
    """
    Best-effort detection for non-reversible stored password formats.
    """
    s = (stored or "").strip()
    if not s:
        return False

    # Django-style hashes can be identified
    try:
        identify_hasher(s)
        return True
    except Exception:
        pass

    # Common bcrypt / argon2 formats
    if s.startswith("$2a$") or s.startswith("$2b$") or s.startswith("$2y$") or s.startswith("$argon2"):
        return True

    # Heuristic: long strings with separators often indicate hashes
    if len(s) >= 40 and any(ch in s for ch in "$:."):
        return True

    return False


def verify_password(raw_password: str, stored_password: str) -> bool:
    """
    Supports:
      - Django-format hashes (identify_hasher + check_password)
      - bcrypt "$2..." hashes if 'bcrypt' library is installed
      - plaintext fallback (constant-time compare)
    """
    raw = raw_password or ""
    stored = (stored_password or "").strip()
    if not raw or not stored:
        return False

    # 1) Django hash
    try:
        identify_hasher(stored)
        return check_password(raw, stored)
    except Exception:
        pass

    # 2) bcrypt if available
    if stored.startswith("$2"):
        try:
            import bcrypt  # type: ignore
            return bcrypt.checkpw(raw.encode("utf-8"), stored.encode("utf-8"))
        except Exception:
            pass

    # 3) plaintext
    return secrets.compare_digest(raw, stored)


def resolve_master_doctor_identity(email: str) -> Optional[MasterDoctorAuthResult]:
    """
    Find the doctor/staff record in master DB by email, without checking password.
    Useful for forgot-password flows.
    """
    row = fetch_master_doctor_row_by_email(email)
    if not row:
        return None

    fm = _field_map()
    e = (email or "").strip().lower()

    doctor_email = str(row.get(fm["email"], "") or "").strip()
    user1_email = str(row.get(fm["user1_email"], "") or "").strip()
    user2_email = str(row.get(fm["user2_email"], "") or "").strip()

    role: Literal["doctor", "clinic_user1", "clinic_user2"] = "doctor"
    display_name = ""

    if doctor_email.lower() == e:
        role = "doctor"
        display_name = _normalize_full_name(
            str(row.get(fm["first_name"], "") or ""),
            str(row.get(fm["last_name"], "") or ""),
        ) or doctor_email
    elif user1_email and user1_email.lower() == e:
        role = "clinic_user1"
        display_name = str(row.get(fm["user1_name"], "") or "").strip() or user1_email
    elif user2_email and user2_email.lower() == e:
        role = "clinic_user2"
        display_name = str(row.get(fm["user2_name"], "") or "").strip() or user2_email
    else:
        # Fallback if collation/matching differs
        role = "doctor"
        display_name = _normalize_full_name(
            str(row.get(fm["first_name"], "") or ""),
            str(row.get(fm["last_name"], "") or ""),
        ) or doctor_email or e

    doctor_id = str(row.get(fm["doctor_id"], "") or "").strip()
    doctor_full_name = _normalize_full_name(
        str(row.get(fm["first_name"], "") or ""),
        str(row.get(fm["last_name"], "") or ""),
    ).strip() or display_name

    if not doctor_id:
        return None

    return MasterDoctorAuthResult(
        doctor_id=doctor_id,
        login_email=e,
        role=role,
        display_name=display_name,
        doctor_full_name=doctor_full_name,
        row=row,
    )


def get_stored_password_for_role(row: Dict[str, Any], role: Literal["doctor", "clinic_user1", "clinic_user2"]) -> str:
    fm = _field_map()
    if role == "clinic_user1":
        return str(row.get(fm["user1_password"], "") or "")
    if role == "clinic_user2":
        return str(row.get(fm["user2_password"], "") or "")
    return str(row.get(fm["doctor_password"], "") or "")


def resolve_master_doctor_auth(email: str, raw_password: str) -> Optional[MasterDoctorAuthResult]:
    """
    Authenticate an email+password against master DB.
    """
    ident = resolve_master_doctor_identity(email)
    if not ident:
        return None

    stored = get_stored_password_for_role(ident.row, ident.role)
    if not verify_password(raw_password, stored):
        return None

    return ident


def master_row_to_template_context(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Convert master row into template-compatible dicts expected by the portal templates:

      - doctor.user.full_name, doctor.user.email, doctor.doctor_id, doctor.whatsapp_number, doctor.imc_number
      - doctor.clinic.display_name, clinic_phone, clinic_whatsapp_number, address_text, state, postal_code

    NOTE: We attempt to derive the state from PIN code (local pincode directory) to avoid stale/incorrect
    state values in MASTER DB (e.g., a non-Maharashtra PIN saved with default "Maharashtra").
    """
    fm = _field_map()

    doctor_id = str(row.get(fm["doctor_id"], "") or "").strip()
    first = str(row.get(fm["first_name"], "") or "")
    last = str(row.get(fm["last_name"], "") or "")
    full_name = _normalize_full_name(first, last).strip() or "Doctor"

    doctor_email = str(row.get(fm["email"], "") or "").strip()
    doctor_whatsapp = str(row.get(fm["whatsapp_no"], "") or "").strip()
    imc = str(row.get(fm["imc_number"], "") or "").strip()

    clinic_name = str(row.get(fm["clinic_name"], "") or "").strip()
    clinic_display = clinic_name or f"Dr. {full_name}"

    clinic_phone = str(row.get(fm["clinic_phone"], "") or "").strip()
    clinic_whatsapp = str(row.get(fm["clinic_whatsapp"], "") or "").strip()

    clinic_address = str(row.get(fm["clinic_address"], "") or "").strip()
    postal_code = str(row.get(fm["postal_code"], "") or "").strip()
    state_raw = str(row.get(fm["state"], "") or "").strip()

    # Prefer state inferred from PIN (local lookup), if available.
    inferred_state: Optional[str] = None
    if postal_code:
        try:
            from accounts.pincode_directory import get_state_for_pincode  # type: ignore
            inferred_state = get_state_for_pincode(postal_code)
        except Exception:
            inferred_state = None

    state = (inferred_state or state_raw).strip()

    clinic: Dict[str, Any] = {
        "display_name": clinic_display,
        "clinic_phone": clinic_phone,
        "clinic_whatsapp_number": clinic_whatsapp,
        "address_text": clinic_address,
        "state": state,
        "postal_code": postal_code,
    }

    doctor: Dict[str, Any] = {
        "doctor_id": doctor_id,
        "whatsapp_number": doctor_whatsapp,
        "imc_number": imc,
        "photo": None,  # optional: map photo if you have a URL/field to use
        "user": {
            "full_name": full_name,
            "email": doctor_email,
        },
        # IMPORTANT: templates expect doctor.clinic.* (nested), not a separate top-level "clinic"
        "clinic": clinic,
    }

    return doctor, clinic


def build_patient_link_payload(doctor: Dict[str, Any], clinic: Dict[str, Any]) -> Dict[str, Any]:
    """
    Payload embedded into patient_link (signed). Keep it small and patient-display-focused.
    """
    user = doctor.get("user") if isinstance(doctor.get("user"), dict) else {}
    return {
        "doctor": {
            "doctor_id": doctor.get("doctor_id", ""),
            "user": {"full_name": user.get("full_name", "")},
        },
        "clinic": {
            "display_name": clinic.get("display_name", ""),
            "clinic_phone": clinic.get("clinic_phone", ""),
            "clinic_whatsapp_number": clinic.get("clinic_whatsapp_number", ""),
            "address_text": clinic.get("address_text", ""),
            "state": clinic.get("state", ""),
            "postal_code": clinic.get("postal_code", ""),
        },
        "v": 1,  # payload version
    }


def sign_patient_payload(payload: Dict[str, Any]) -> str:
    return signing.dumps(payload, compress=True)


def unsign_patient_payload(token: str) -> Optional[Dict[str, Any]]:
    if not token:
        return None
    try:
        obj = signing.loads(token, max_age=None)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def generate_temporary_password(length: int = 10) -> str:
    # Excludes ambiguous characters for phone dictation.
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
    return "".join(secrets.choice(alphabet) for _ in range(max(8, length)))


def update_master_password(
    *,
    doctor_id: str,
    role: Literal["doctor", "clinic_user1", "clinic_user2"],
    new_raw_password: str,
) -> bool:
    """
    Used by the forgot-password flow when stored passwords are hashes (not retrievable).
    Updates the appropriate password hash column in redflags_doctor.

    Requires UPDATE privilege on the master DB.
    """
    fm = _field_map()
    table = _doctor_table()

    if role == "clinic_user1":
        pwd_col = fm["user1_password"]
    elif role == "clinic_user2":
        pwd_col = fm["user2_password"]
    else:
        pwd_col = fm["doctor_password"]

    # Store Django-style hash (pbkdf2_sha256 by default)
    new_hash = make_password(new_raw_password)

    with connections[getattr(settings, "MASTER_DB_ALIAS", "master")].cursor() as cursor:
        if role == "doctor" and fm.get("doctor_password_set_at"):
            cursor.execute(
                f"UPDATE `{table}` SET `{pwd_col}`=%s, `{fm['doctor_password_set_at']}`=NOW() WHERE `{fm['doctor_id']}`=%s LIMIT 1",
                [new_hash, doctor_id],
            )
        else:
            cursor.execute(
                f"UPDATE `{table}` SET `{pwd_col}`=%s WHERE `{fm['doctor_id']}`=%s LIMIT 1",
                [new_hash, doctor_id],
            )
    return True

# ---------------------------------------------------------------------
# Campaign acknowledgements & banners (Doctor/Clinic sharing portal)
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class PECampaignSupport:
    campaign_id: str
    video_cluster: str
    brand: str
    banner_small_url: str
    banner_large_url: str
    banner_target_url: str


def _master_db_name() -> str:
    conn = connections[_master_alias()]
    return str((conn.settings_dict.get("NAME") or "")).strip()


def _master_table_exists(table_name: str) -> bool:
    """
    Checks if a table exists in MASTER DB.
    Uses information_schema when possible; falls back to a direct SELECT probe.
    """
    tn = _safe_identifier(table_name)
    db = _master_db_name()
    if not db:
        return False

    try:
        with connections[_master_alias()].cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                LIMIT 1
                """,
                [db, tn],
            )
            return cursor.fetchone() is not None
    except Exception:
        # Fallback: probe query
        try:
            with connections[_master_alias()].cursor() as cursor:
                cursor.execute(f"SELECT 1 FROM `{tn}` LIMIT 1")
                return True
        except Exception:
            return False


def _master_table_columns(table_name: str) -> List[str]:
    tn = _safe_identifier(table_name)
    db = _master_db_name()
    if not db:
        return []
    try:
        with connections[_master_alias()].cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                [db, tn],
            )
            rows = cursor.fetchall() or []
        return [str(r[0]) for r in rows if r and r[0]]
    except Exception:
        return []


def _pick_first_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    m = {c.lower(): c for c in (cols or [])}
    for cand in candidates:
        if cand.lower() in m:
            return m[cand.lower()]
    return None


def _uuid_hex_to_hyphenated(hex32: str) -> str:
    """
    Convert 32-hex UUID (no dashes) to standard UUID with dashes.
    """
    h = (hex32 or "").strip().replace("-", "")
    if len(h) != 32:
        return (hex32 or "").strip()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _get_video_cluster_from_master_mapping(campaign_id: str) -> Optional[str]:
    """
    Attempts to read video_cluster from MASTER DB mapping table.
    Your prompt references: campaign_videocluster in master DB.
    The provided MASTER_DB_ALIAS.sql does not include this table, so this is best-effort.

    Expected: a table (default 'campaign_videocluster') with columns including:
      - campaign_id (or similar)
      - video_cluster (or similar)
    """
    cid = (campaign_id or "").strip().replace("-", "")
    if not cid:
        return None

    table = getattr(settings, "MASTER_CAMPAIGN_VIDEOCLUSTER_TABLE", "campaign_videocluster") or "campaign_videocluster"
    try:
        table = _safe_identifier(str(table))
    except Exception:
        table = "campaign_videocluster"

    if not _master_table_exists(table):
        return None

    cols = _master_table_columns(table)
    if not cols:
        return None

    campaign_col = _pick_first_col(cols, ["campaign_id", "campaign", "campaign_uuid"])
    vc_col = _pick_first_col(
        cols,
        ["video_cluster", "video_cluster_code", "video_cluster_name", "video_cluster_id", "cluster", "cluster_code"],
    )
    if not campaign_col or not vc_col:
        return None

    cid_h = _uuid_hex_to_hyphenated(cid)

    try:
        with connections[_master_alias()].cursor() as cursor:
            cursor.execute(
                f"SELECT `{vc_col}` FROM `{table}` WHERE `{campaign_col}` = %s OR `{campaign_col}` = %s LIMIT 1",
                [cid, cid_h],
            )
            row = cursor.fetchone()
        if not row or row[0] is None:
            return None
        return str(row[0]).strip()
    except Exception:
        return None


def _get_video_cluster_from_local_publisher_campaign(campaign_id: str) -> Optional[str]:
    """
    Fallback resolver:
    publisher_campaign.campaign_id is a UUID string WITH dashes in default DB.
    master campaign ids are usually CHAR(32) WITHOUT dashes.
    """
    cid = (campaign_id or "").strip().replace("-", "")
    if not cid:
        return None

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
        return None

    return None


def resolve_campaign_video_cluster(*, campaign_id: str, campaign_name_fallback: str = "") -> str:
    """
    Priority:
      1) MASTER mapping table (campaign_videocluster) if present
      2) local default DB publisher_campaign.new_video_cluster_name
      3) campaign name (from campaign_campaign.name)
      4) campaign_id (last resort)
    """
    cid = (campaign_id or "").strip()
    if not cid:
        return (campaign_name_fallback or "").strip() or ""

    vc = _get_video_cluster_from_master_mapping(cid)
    if vc:
        return vc

    vc = _get_video_cluster_from_local_publisher_campaign(cid)
    if vc:
        return vc

    return (campaign_name_fallback or "").strip() or cid


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
        # campaign_doctor.phone is typically stored as digits; we compare last-10 to handle +91 prefixes.
        where_parts.append(
            "RIGHT(d.phone, 10) IN (" + ",".join(["%s"] * len(phone_candidates)) + ")"
        )
        params.extend(phone_candidates)

    where_sql = " OR ".join(where_parts)

    # IMPORTANT:
    # The shared MASTER_DB_ALIAS.sql shows campaign_doctorcampaignenrollment has NO "active" column.
    # If we filter on e.active, the query fails and banners never render (doctor_share catches and shows none).
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
          AND c.system_pe = 1
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

        # Fallback: if master has no target URL, try local publisher_campaign.banner_target_url
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
