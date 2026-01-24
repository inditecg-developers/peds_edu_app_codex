from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

from django.conf import settings
from django.db import connections, IntegrityError

import secrets

from django.db import transaction
from django.db.models import Q

from .models import RedflagsDoctor


import logging

_master_logger = logging.getLogger("accounts.master_db")
_MASTER_CONN_LOGGED = False


def _mask_email_for_log(email: str) -> str:
    e = (email or "").strip().lower()
    if "@" not in e:
        return (e[:2] + "***") if e else ""
    u, d = e.split("@", 1)
    return f"{(u[:2] + '***') if len(u) >= 2 else '***'}@{d}"


def _mask_phone_for_log(phone: str) -> str:
    digits = re.sub(r"\D", "", str(phone or ""))
    return (f"***{digits[-4:]}") if len(digits) > 4 else ("***" if digits else "")


def _conn_info(conn) -> dict:
    d = getattr(conn, "settings_dict", {}) or {}
    return {
        "ENGINE": d.get("ENGINE"),
        "NAME": d.get("NAME"),
        "HOST": d.get("HOST"),
        "PORT": d.get("PORT"),
        "USER": d.get("USER"),
    }


def _log_db(event: str, level: str = "info", **fields) -> None:
    msg = json.dumps({"ts": int(time.time()), "event": event, **fields}, default=str, ensure_ascii=False)
    if level == "debug":
        _master_logger.debug(msg)
    elif level == "warning":
        _master_logger.warning(msg)
    elif level == "error":
        _master_logger.error(msg)
    else:
        _master_logger.info(msg)


def _log_db_exc(event: str, **fields) -> None:
    _master_logger.exception(
        json.dumps({"ts": int(time.time()), "event": event, **fields}, default=str, ensure_ascii=False)
    )


class MasterDBNotConfigured(RuntimeError):
    pass


def master_alias() -> str:
    return getattr(settings, "MASTER_DB_ALIAS", "master")


def get_master_connection():
    alias = master_alias()
    if alias not in connections.databases:
        raise MasterDBNotConfigured(
            f"MASTER DB alias '{alias}' is not configured in settings.DATABASES."
        )
    return connections[alias]


def qn(name: str) -> str:
    """
    Quote identifiers safely per backend.
    Supports schema-qualified names like: schema.table
    """
    conn = get_master_connection()
    parts = [p for p in (name or "").split(".") if p]
    if len(parts) > 1:
        return ".".join(conn.ops.quote_name(p) for p in parts)
    return conn.ops.quote_name(name)


# -------------------------------
# WhatsApp helpers
# -------------------------------

def normalize_wa_for_lookup(raw: str) -> str:
    s = re.sub(r"\D", "", str(raw or ""))
    if len(s) == 12 and s.startswith("91"):
        return s[2:]
    return s


def wa_link_number(raw: str, default_country_code: str = "91") -> str:
    s = re.sub(r"\D", "", str(raw or ""))
    if len(s) == 10:
        return f"{default_country_code}{s}"
    if s.startswith("0") and len(s) == 11:
        return f"{default_country_code}{s[1:]}"
    return s


def build_whatsapp_deeplink(raw_phone: str, message: str) -> str:
    phone = wa_link_number(raw_phone)
    return f"https://wa.me/{phone}?text={quote(message or '')}"


# -------------------------------
# MASTER: AuthorizedPublisher
# -------------------------------

def authorized_publisher_exists(email: str) -> bool:
    """
    Checks AuthorizedPublisher in MASTER DB.
    """
    e = (email or "").strip().lower()
    if not e:
        return False

    conn = get_master_connection()
    table = getattr(settings, "MASTER_DB_AUTH_PUBLISHER_TABLE", "publisher_authorizedpublisher")
    email_col = getattr(settings, "MASTER_DB_AUTH_PUBLISHER_EMAIL_COLUMN", "email")

    sql = f"SELECT 1 FROM {qn(table)} WHERE LOWER({qn(email_col)}) = LOWER(%s) LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, [e])
        return cur.fetchone() is not None


# -------------------------------
# MASTER: FieldRep
# -------------------------------

@dataclass(frozen=True)
class MasterFieldRep:
    id: str
    full_name: str
    phone_number: str
    brand_supplied_field_rep_id: str
    is_active: bool


# -------------------------------
# MASTER: Campaign
# -------------------------------

@dataclass(frozen=True)
class MasterCampaign:
    campaign_id: str
    doctors_supported: int
    wa_addition: str
    new_video_cluster_name: str
    email_registration: str


def get_campaign(campaign_id: str) -> Optional[MasterCampaign]:
    """
    MASTER Campaign lookup for the admin-project schema.

    - Campaign PK column is 'id' (UUIDField).
    - In MySQL, UUIDField is typically stored as CHAR(32) without hyphens.
    - Accepts both:
        7ea0883d-9791-4703-b569-c1f9f8d25705  (hyphenated)
        7ea0883d97914703b569c1f9f8d25705      (hex)
    """
    cid_raw = (campaign_id or "").strip()
    if not cid_raw:
        return None

    cid_norm = cid_raw.replace("-", "")

    conn = get_master_connection()

    table = getattr(settings, "MASTER_DB_CAMPAIGN_TABLE", "campaign_campaign")
    id_col = getattr(settings, "MASTER_DB_CAMPAIGN_ID_COLUMN", "id")

    ds_col = getattr(settings, "MASTER_DB_CAMPAIGN_DOCTORS_SUPPORTED_COLUMN", "num_doctors_supported")
    wa_col = getattr(settings, "MASTER_DB_CAMPAIGN_WA_ADDITION_COLUMN", "add_to_campaign_message")
    vc_col = getattr(settings, "MASTER_DB_CAMPAIGN_VIDEO_CLUSTER_COLUMN", "name")
    er_col = getattr(settings, "MASTER_DB_CAMPAIGN_EMAIL_REGISTRATION_COLUMN", "register_message")

    sql = (
        f"SELECT {qn(id_col)}, {qn(ds_col)}, {qn(wa_col)}, {qn(vc_col)}, {qn(er_col)} "
        f"FROM {qn(table)} "
        f"WHERE {qn(id_col)} = %s OR {qn(id_col)} = %s "
        f"LIMIT 1"
    )

    with conn.cursor() as cur:
        cur.execute(sql, [cid_norm, cid_raw])
        row = cur.fetchone()

    if not row:
        return None

    try:
        doctors_supported = int(row[1] or 0)
    except Exception:
        doctors_supported = 0

    return MasterCampaign(
        campaign_id=str(row[0] or "").strip(),  # will typically be CHAR(32) (no hyphens)
        doctors_supported=doctors_supported,
        wa_addition=str(row[2] or ""),          # mapped from add_to_campaign_message
        new_video_cluster_name=str(row[3] or ""),  # mapped from name
        email_registration=str(row[4] or ""),   # mapped from register_message
    )



# -------------------------------
# MASTER: Doctor & Enrollment (as before)
# -------------------------------

@dataclass(frozen=True)
class MasterDoctor:
    doctor_id: str
    first_name: str = ""
    last_name: str = ""
    email: str = ""
    whatsapp_no: str = ""

    @property
    def full_name(self) -> str:
        return (f"{self.first_name} {self.last_name}").strip()


def doctor_id_exists(doctor_id: str) -> bool:
    did = (doctor_id or "").strip()
    if not did:
        return False
    alias = master_alias()
    return RedflagsDoctor.objects.using(alias).filter(doctor_id=did).exists()




def get_doctor_by_whatsapp(whatsapp_no: str) -> Optional[MasterDoctor]:
    """
    Lookup doctor in MASTER DB by WhatsApp number from table redflags_doctor.

    Handles common formatting:
      - 10 digit
      - 91 + 10 digit
      - +91 + 10 digit
      - 0 + 10 digit
      - mixed formatting with spaces/dashes

    Returns MasterDoctor or None.
    """
    import re

    raw = (whatsapp_no or "").strip()
    digits = re.sub(r"\D", "", raw)

    if not digits:
        return None

    # Normalize down to 10 digits where possible
    base10 = digits
    if len(base10) == 12 and base10.startswith("91"):
        base10 = base10[2:]
    elif len(base10) == 11 and base10.startswith("0"):
        base10 = base10[1:]
    elif len(base10) > 10:
        # last 10 digits is often the actual number
        base10 = base10[-10:]

    candidates = []
    seen = set()

    def _add(x: str) -> None:
        x = (x or "").strip()
        if x and x not in seen:
            seen.add(x)
            candidates.append(x)

    # Add candidates in likely-match order
    _add(base10)                 # 10-digit
    _add("91" + base10)          # 91XXXXXXXXXX
    _add("+91" + base10)         # +91XXXXXXXXXX
    _add("0" + base10)           # 0XXXXXXXXXX
    _add(digits)                 # original digits-only form (fallback)

    conn = get_master_connection()

    table = getattr(settings, "MASTER_DB_DOCTOR_TABLE", "redflags_doctor")
    id_col = getattr(settings, "MASTER_DB_DOCTOR_ID_COLUMN", "doctor_id")
    fn_col = getattr(settings, "MASTER_DB_DOCTOR_FIRST_NAME_COLUMN", "first_name")
    ln_col = getattr(settings, "MASTER_DB_DOCTOR_LAST_NAME_COLUMN", "last_name")
    email_col = getattr(settings, "MASTER_DB_DOCTOR_EMAIL_COLUMN", "email")
    wa_col = getattr(settings, "MASTER_DB_DOCTOR_WHATSAPP_COLUMN", "whatsapp_no")

    where = " OR ".join([f"{qn(wa_col)} = %s"] * len(candidates))
    sql = (
        f"SELECT {qn(id_col)}, {qn(fn_col)}, {qn(ln_col)}, {qn(email_col)}, {qn(wa_col)} "
        f"FROM {qn(table)} "
        f"WHERE {where} "
        f"LIMIT 1"
    )

    with conn.cursor() as cur:
        cur.execute(sql, candidates)
        row = cur.fetchone()

    if not row:
        return None

    return MasterDoctor(
        doctor_id=str(row[0] or "").strip(),
        first_name=str(row[1] or "").strip(),
        last_name=str(row[2] or "").strip(),
        email=str(row[3] or "").strip(),
        whatsapp_no=str(row[4] or "").strip(),
    )



from typing import Dict, List, Optional, Tuple

_ENROLLMENT_META_CACHE: Optional[Dict[str, str]] = None


def _db_schema_name(conn) -> str:
    return (conn.settings_dict.get("NAME") or "").strip()


def _table_exists(conn, table_name: str) -> bool:
    schema = _db_schema_name(conn)
    if not schema or not table_name:
        return False
    sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, [schema, table_name])
        return cur.fetchone() is not None


def _find_table_by_patterns(conn, patterns: List[str]) -> Optional[str]:
    """
    Find the first table whose name matches any of the LIKE patterns (case-insensitive).
    """
    schema = _db_schema_name(conn)
    if not schema:
        return None

    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND LOWER(table_name) LIKE %s
        ORDER BY LENGTH(table_name) ASC, table_name ASC
        LIMIT 1
    """

    for pat in patterns:
        with conn.cursor() as cur:
            cur.execute(sql, [schema, pat.lower()])
            row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
    return None


def _get_table_columns(conn, table_name: str) -> List[str]:
    schema = _db_schema_name(conn)
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, [schema, table_name])
        rows = cur.fetchall()
    return [str(r[0]) for r in (rows or []) if r and r[0]]


def _pick_first_column(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_l = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cols_l:
            return cols_l[cand.lower()]
    return None


def _normalize_uuid_for_mysql(raw: str) -> str:
    """
    MySQL Django UUIDField commonly stored as CHAR(32) without hyphens.
    """
    return (raw or "").strip().replace("-", "")


def _get_enrollment_meta() -> Dict[str, str]:
    """
    Discover enrollment table + columns once per process and cache.

    Returns dict:
      {
        "table": <table_name>,
        "campaign_col": <campaign_column_name>,
        "doctor_col": <doctor_column_name>,
        "registered_by_col": <optional column name or "">,
      }
    """
    global _ENROLLMENT_META_CACHE
    if _ENROLLMENT_META_CACHE is not None:
        return _ENROLLMENT_META_CACHE

    conn = get_master_connection()

    # If you hardcoded an explicit table in settings, prefer it.
    explicit_table = (getattr(settings, "MASTER_DB_ENROLLMENT_TABLE", "") or "").strip()
    if explicit_table and _table_exists(conn, explicit_table):
        table = explicit_table
    else:
        # Auto-discover by Django model naming patterns
        patterns = [
            "%doctorcampaignenrollment%",
            "%doctor_campaign_enrollment%",
            "%campaignenrollment%",
            "%enrolment%",  # UK spelling fallback
        ]
        table = _find_table_by_patterns(conn, patterns)

    if not table:
        raise RuntimeError(
            "Enrollment table not found in master DB. "
            "Create/migrate the enrollment model or set settings.MASTER_DB_ENROLLMENT_TABLE explicitly."
        )

    cols = _get_table_columns(conn, table)
    if not cols:
        raise RuntimeError(f"Could not read columns for enrollment table '{table}' in master DB.")

    # Common Django FK column naming: campaign_id, doctor_id, registered_by_id
    campaign_col = _pick_first_column(cols, ["campaign_id", "campaign", "campaign_uuid"])
    doctor_col = _pick_first_column(cols, ["doctor_id", "doctor", "doctor_uuid"])

    # registered_by may be FK to FieldRep => registered_by_id
    registered_by_col = _pick_first_column(
        cols,
        ["registered_by_id", "registered_by", "field_rep_id", "fieldrep_id"],
    )

    if not campaign_col or not doctor_col:
        raise RuntimeError(
            f"Enrollment table '{table}' does not have expected columns. "
            f"Found columns: {cols}"
        )

    _ENROLLMENT_META_CACHE = {
        "table": table,
        "campaign_col": campaign_col,
        "doctor_col": doctor_col,
        "registered_by_col": registered_by_col or "",
    }

    print(json.dumps({
        "ts": int(time.time()),
        "event": "master_db.enrollment_meta.discovered",
        "table": table,
        "campaign_col": campaign_col,
        "doctor_col": doctor_col,
        "registered_by_col": registered_by_col or "",
    }, default=str))

    return _ENROLLMENT_META_CACHE


def count_campaign_enrollments(campaign_id: str) -> int:
    """
    Count doctors enrolled for a campaign in MASTER DB.
    Tries both hyphenated and non-hyphenated UUID formats.
    """
    meta = _get_enrollment_meta()
    table = meta["table"]
    campaign_col = meta["campaign_col"]

    cid_raw = (campaign_id or "").strip()
    if not cid_raw:
        return 0

    cid_norm = _normalize_uuid_for_mysql(cid_raw)

    conn = get_master_connection()

    sql = f"SELECT COUNT(*) FROM {qn(table)} WHERE {qn(campaign_col)} = %s"

    counts = []
    with conn.cursor() as cur:
        # normalized (CHAR32)
        cur.execute(sql, [cid_norm])
        row = cur.fetchone()
        counts.append(int(row[0] or 0))

        # raw (hyphenated) only if different
        if cid_raw != cid_norm:
            cur.execute(sql, [cid_raw])
            row = cur.fetchone()
            counts.append(int(row[0] or 0))

    return max(counts) if counts else 0



def insert_doctor_row(
    *,
    doctor_id: str,
    first_name: str,
    last_name: str,
    email: str,
    clinic_name: str,
    imc_registration_number: str,
    clinic_phone: str,
    clinic_appointment_number: str,
    clinic_address: str,
    postal_code: str,
    state: str,
    district: str,
    whatsapp_no: str,
    receptionist_whatsapp_number: str,
    photo_path: str,
    field_rep_id: str = "",
    recruited_via: str = "FIELD_REP",
) -> None:
    conn = get_master_connection()
    table = getattr(settings, "MASTER_DB_DOCTOR_TABLE", "Doctor")

    _log_db("master_db.doctor.insert.start", doctor_id=doctor_id, email=_mask_email_for_log(email),
            whatsapp=_mask_phone_for_log(whatsapp_no), table=table)

    cols = (
        "doctor_id",
        "first_name",
        "last_name",
        "email",
        "clinic_name",
        "imc_registration_number",
        "clinic_phone",
        "clinic_appointment_number",
        "clinic_address",
        "postal_code",
        "state",
        "district",
        "whatsapp_no",
        "receptionist_whatsapp_number",
        "photo",
        "field_rep_id",
        "recruited_via",
    )

    vals = [
        doctor_id,
        first_name,
        last_name,
        email.lower(),
        clinic_name,
        imc_registration_number,
        clinic_phone,
        clinic_appointment_number,
        clinic_address,
        postal_code,
        state,
        district,
        normalize_wa_for_lookup(whatsapp_no) or whatsapp_no,
        normalize_wa_for_lookup(receptionist_whatsapp_number) or receptionist_whatsapp_number,
        photo_path or "",
        field_rep_id or "",
        recruited_via or "FIELD_REP",
    ]

    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {qn(table)} ({', '.join(qn(c) for c in cols)}) VALUES ({placeholders})"
    with conn.cursor() as cur:
        cur.execute(sql, vals)
        _log_db("master_db.doctor.insert.ok", doctor_id=doctor_id, rowcount=getattr(cur, "rowcount", None))


def ensure_enrollment(*, doctor_id: str, campaign_id: str, registered_by: str) -> None:
    """
    Insert enrollment row in MASTER DB (MySQL) if not already present.
    Uses INSERT IGNORE to avoid duplicate key failures (if unique constraint exists).
    """

    _log_db("master_db.enrollment.ensure.start", doctor_id=doctor_id, campaign_id=campaign_id)


    if not (doctor_id and campaign_id):
        return

    meta = _get_enrollment_meta()
    table = meta["table"]
    doctor_col = meta["doctor_col"]
    campaign_col = meta["campaign_col"]
    registered_by_col = meta.get("registered_by_col") or ""

    cid_raw = (campaign_id or "").strip()
    cid_norm = _normalize_uuid_for_mysql(cid_raw)

    cols = [doctor_col, campaign_col]
    vals = [doctor_id, cid_norm]

    if registered_by_col:
        cols.append(registered_by_col)
        vals.append(registered_by or "")

    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT IGNORE INTO {qn(table)} ({', '.join(qn(c) for c in cols)}) VALUES ({placeholders})"

    conn = get_master_connection()
    with conn.cursor() as cur:
        cur.execute(sql, vals)
        _log_db("master_db.enrollment.ensure.done", doctor_id=doctor_id, campaign_id=cid_norm,
                rowcount=getattr(cur, "rowcount", None))



def normalize_campaign_id(campaign_id: str) -> str:
    """
    Your join table stores campaign_id WITHOUT hyphens:
      7ea0883d97914703b569c1f9f8d25705
    but URLs pass UUID with hyphens:
      7ea0883d-9791-4703-b569-c1f9f8d25705

    Normalize by removing hyphens and trimming.
    """
    return (campaign_id or "").strip().replace("-", "")


def get_campaign_fieldrep_link_fieldrep_id(*, campaign_id: str, link_pk: int) -> Optional[int]:
    """
    Treat `field_rep_id` URL as join-table primary key and resolve to actual field_rep_id.
    """
    conn = get_master_connection()

    table = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_TABLE", "campaign_campaignfieldrep")
    pk_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_PK_COLUMN", "id")
    campaign_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_CAMPAIGN_COLUMN", "campaign_id")
    fr_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_FIELD_REP_COLUMN", "field_rep_id")

    cid = normalize_campaign_id(campaign_id)
    sql = (
        f"SELECT {qn(fr_col)} "
        f"FROM {qn(table)} "
        f"WHERE {qn(pk_col)} = %s AND {qn(campaign_col)} = %s "
        f"LIMIT 1"
    )

    with conn.cursor() as cur:
        cur.execute(sql, [int(link_pk), cid])
        row = cur.fetchone()

    if not row:
        return None

    try:
        return int(row[0])
    except Exception:
        return None


def is_fieldrep_linked_to_campaign(*, campaign_id: str, field_rep_id: int) -> bool:
    """
    Enforce that the field rep is allowed for this campaign (join table must contain row).
    """
    conn = get_master_connection()

    table = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_TABLE", "campaign_campaignfieldrep")
    campaign_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_CAMPAIGN_COLUMN", "campaign_id")
    fr_col = getattr(settings, "MASTER_DB_CAMPAIGN_FIELD_REP_FIELD_REP_COLUMN", "field_rep_id")

    cid = normalize_campaign_id(campaign_id)
    sql = (
        f"SELECT 1 FROM {qn(table)} "
        f"WHERE {qn(campaign_col)} = %s AND {qn(fr_col)} = %s "
        f"LIMIT 1"
    )

    with conn.cursor() as cur:
        cur.execute(sql, [cid, int(field_rep_id)])
        return cur.fetchone() is not None


def get_field_rep(field_rep_id: str) -> Optional[MasterFieldRep]:
    """
    Deterministic FieldRep lookup.

    Priority:
      1) brand_supplied_field_rep_id exact match (string)
      2) id (pk) match if numeric (or trailing digits)
      3) user_id match if numeric (or trailing digits)

    This prevents the bug where id=15 accidentally returns the row whose user_id=15.
    """
    fid_raw = (field_rep_id or "").strip()
    if not fid_raw:
        return None

    # pull trailing digits e.g. "fieldrep_15" -> 15
    tail_digits = None
    m = re.search(r"(\d+)$", fid_raw)
    if m:
        try:
            tail_digits = int(m.group(1))
        except Exception:
            tail_digits = None

    conn = get_master_connection()

    table = getattr(settings, "MASTER_DB_FIELD_REP_TABLE", "campaign_fieldrep")
    pk_col = getattr(settings, "MASTER_DB_FIELD_REP_PK_COLUMN", "id")
    user_id_col = getattr(settings, "MASTER_DB_FIELD_REP_USER_ID_COLUMN", "user_id")
    ext_col = getattr(settings, "MASTER_DB_FIELD_REP_EXTERNAL_ID_COLUMN", "brand_supplied_field_rep_id")
    active_col = getattr(settings, "MASTER_DB_FIELD_REP_ACTIVE_COLUMN", "is_active")
    name_col = getattr(settings, "MASTER_DB_FIELD_REP_FULL_NAME_COLUMN", "full_name")
    phone_col = getattr(settings, "MASTER_DB_FIELD_REP_PHONE_COLUMN", "phone_number")

    def _row_to_obj(row):
        return MasterFieldRep(
            id=str(row[0]),
            full_name=str(row[1] or "").strip(),
            phone_number=str(row[2] or "").strip(),
            brand_supplied_field_rep_id=str(row[3] or "").strip(),
            is_active=bool(row[4]),
        )

    # 1) external id exact match
    sql_ext = (
        f"SELECT {qn(pk_col)}, {qn(name_col)}, {qn(phone_col)}, {qn(ext_col)}, {qn(active_col)} "
        f"FROM {qn(table)} WHERE {qn(ext_col)} = %s LIMIT 1"
    )
    with conn.cursor() as cur:
        cur.execute(sql_ext, [fid_raw])
        row = cur.fetchone()
    if row:
        return _row_to_obj(row)

    # numeric candidates: fid_raw if numeric, else trailing digits
    num = int(fid_raw) if fid_raw.isdigit() else tail_digits
    if num is None:
        return None

    # 2) pk match FIRST
    sql_pk = (
        f"SELECT {qn(pk_col)}, {qn(name_col)}, {qn(phone_col)}, {qn(ext_col)}, {qn(active_col)} "
        f"FROM {qn(table)} WHERE {qn(pk_col)} = %s LIMIT 1"
    )
    with conn.cursor() as cur:
        cur.execute(sql_pk, [num])
        row = cur.fetchone()
    if row:
        return _row_to_obj(row)

    # 3) user_id match
    sql_uid = (
        f"SELECT {qn(pk_col)}, {qn(name_col)}, {qn(phone_col)}, {qn(ext_col)}, {qn(active_col)} "
        f"FROM {qn(table)} WHERE {qn(user_id_col)} = %s LIMIT 1"
    )
    with conn.cursor() as cur:
        cur.execute(sql_uid, [num])
        row = cur.fetchone()
    if row:
        return _row_to_obj(row)

    return None



def resolve_field_rep_for_campaign(*, campaign_id: str, field_rep_identifier: str, token_sub: str = "") -> Optional[MasterFieldRep]:
    """
    The one function you should call from field_rep_landing_page.

    It supports the real-world situation you have:
      - URL field_rep_id may actually be the join-table id (e.g. 56)
      - token_sub may be "fieldrep_15"

    Resolution order:
      1) Try identifier directly in FieldRep table (id/user_id/external id)
         and enforce join exists for campaign.
      2) If not found, treat identifier as join-table PK:
         resolve to actual field_rep_id, fetch FieldRep, enforce join exists.
      3) Try token_sub (and token_sub tail digits) as fallback (same enforcement).
    """
    cid = normalize_campaign_id(campaign_id)

    # 1) direct candidates first
    direct_candidates = [field_rep_identifier]
    if token_sub:
        direct_candidates.append(token_sub)
        m = re.search(r"(\d+)$", token_sub)
        if m:
            direct_candidates.append(m.group(1))

    # Try direct fieldrep matches + campaign link enforcement
    for cand in direct_candidates:
        if not cand:
            continue
        fr = get_field_rep(cand)
        if fr and fr.is_active:
            try:
                if is_fieldrep_linked_to_campaign(campaign_id=cid, field_rep_id=int(fr.id)):
                    return fr
            except Exception:
                # if id is non-numeric, skip link check (should not happen with your schema)
                pass

    # 2) treat URL identifier as join-table pk
    if (field_rep_identifier or "").strip().isdigit():
        link_pk = int(field_rep_identifier.strip())
        fr_id = get_campaign_fieldrep_link_fieldrep_id(campaign_id=cid, link_pk=link_pk)
        if fr_id:
            fr = get_field_rep(str(fr_id))
            if fr and fr.is_active:
                if is_fieldrep_linked_to_campaign(campaign_id=cid, field_rep_id=fr_id):
                    return fr

    return None


def find_doctor_by_email_or_whatsapp(*, email: str, whatsapp: str) -> Optional[MasterDoctor]:
    email_n = (email or "").strip().lower()
    wa_raw = (whatsapp or "").strip()

    # Use your existing normalization helper
    wa_n = normalize_wa_for_lookup(wa_raw)

    if not email_n and not wa_n:
        return None

    alias = master_alias()

    q = Q()
    if email_n:
        q |= Q(email__iexact=email_n)

    # WhatsApp can be stored with/without country code; include a few candidates
    if wa_n:
        candidates = []
        seen = set()

        def _add(x: str) -> None:
            x = (x or "").strip()
            if x and x not in seen:
                seen.add(x)
                candidates.append(x)

        base10 = wa_n
        if len(base10) == 12 and base10.startswith("91"):
            base10 = base10[2:]
        elif len(base10) == 11 and base10.startswith("0"):
            base10 = base10[1:]
        elif len(base10) > 10:
            base10 = base10[-10:]

        _add(base10)
        _add("91" + base10)
        _add("+91" + base10)
        _add("0" + base10)
        _add(wa_n)

        q |= Q(whatsapp_no__in=candidates)

    obj = (
        RedflagsDoctor.objects.using(alias)
        .filter(q)
        .only("doctor_id", "first_name", "last_name", "email", "whatsapp_no")
        .first()
    )
    if not obj:
        return None

    return MasterDoctor(
        doctor_id=str(obj.doctor_id or "").strip(),
        first_name=str(obj.first_name or "").strip(),
        last_name=str(obj.last_name or "").strip(),
        email=str(obj.email or "").strip(),
        whatsapp_no=str(obj.whatsapp_no or "").strip(),
    )



def generate_doctor_id(prefix: str = "DR", max_tries: int = 200) -> str:
    prefix = (prefix or "DR").upper().strip()
    suffix_len = 8 - len(prefix)
    if suffix_len <= 0:
        raise ValueError("generate_doctor_id: prefix too long for an 8-char doctor_id")

    digits = "0123456789"

    for _ in range(max_tries):
        did = prefix + "".join(secrets.choice(digits) for _ in range(suffix_len))
        if not doctor_id_exists(did):
            return did

    raise RuntimeError("Unable to generate unique doctor_id in MASTER DB")



def create_doctor_with_enrollment(
    *,
    doctor_id: str,
    first_name: str,
    last_name: str,
    email: str,
    whatsapp: str,
    clinic_name: str,
    clinic_phone: str = "",
    clinic_appointment_number: str = "",
    clinic_address: str = "",
    imc_number: str = "",
    postal_code: str = "",
    state: str = "",
    district: str = "",
    photo_path: str = "",
    campaign_id: Optional[str] = None,
    recruited_via: str = "SELF",
    registered_by: Optional[str] = None,
) -> None:
    """
    Atomically:
      1) Insert doctor into MASTER DB using ORM (redflags_doctor)
      2) Ensure campaign enrollment (if campaign_id provided)

    IMPORTANT: no silent IntegrityError swallowing.
    """

    alias = master_alias()

    email_n = (email or "").strip().lower()
    wa_n = normalize_wa_for_lookup(whatsapp) or (whatsapp or "").strip()

    # Fill basics defensively
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()

    clinic_name = (clinic_name or "").strip()
    clinic_phone = (clinic_phone or "").strip()
    clinic_appointment_number = (clinic_appointment_number or "").strip()

    clinic_address = (clinic_address or "").strip()
    imc_number = (imc_number or "").strip()
    postal_code = (postal_code or "").strip()
    state = (state or "Maharashtra").strip()
    district = (district or "").strip()

    recruited_via = (recruited_via or "SELF").strip()
    field_rep_id = (registered_by or "").strip()

    with transaction.atomic(using=alias):
        try:
            RedflagsDoctor.objects.using(alias).create(
                doctor_id=doctor_id,
                first_name=first_name,
                last_name=last_name,
                email=email_n,
                whatsapp_no=wa_n,

                clinic_name=clinic_name,
                clinic_phone=clinic_phone,
                clinic_appointment_number=clinic_appointment_number,

                clinic_address=clinic_address,
                postal_code=postal_code,
                state=state,
                district=district,

                imc_registration_number=imc_number,

                receptionist_whatsapp_number="",
                photo=photo_path or "",

                field_rep_id=field_rep_id,
                recruited_via=recruited_via,
            )
        except IntegrityError:
            # Race/duplicate case: treat as OK only if it already exists by email/whatsapp
            existing = find_doctor_by_email_or_whatsapp(email=email_n, whatsapp=wa_n)
            if existing:
                return
            # Otherwise, it is a real insert failure; raise it.
            raise

        if campaign_id:
            ensure_enrollment(
                doctor_id=doctor_id,
                campaign_id=campaign_id,
                registered_by=field_rep_id,
            )

