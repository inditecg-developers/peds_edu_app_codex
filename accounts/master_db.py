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

from django.contrib.auth.hashers import make_password
from django.utils import timezone

from .models import RedflagsDoctor
from django.db import transaction
from django.db.models import Q

from .models import RedflagsDoctor


import logging

_master_logger = logging.getLogger("accounts.master_db")
_MASTER_CONN_LOGGED = False


def master_alias() -> str:
    return getattr(settings, "MASTER_DB_ALIAS", "MASTER_DB_ALIAS")


def get_master_connection():
    global _MASTER_CONN_LOGGED
    alias = master_alias()
    conn = connections[alias]

    # lightweight one-time log (helps ops confirm which alias is used)
    if not _MASTER_CONN_LOGGED:
        _master_logger.info("MASTER DB connection alias=%s vendor=%s", alias, getattr(conn, "vendor", None))
        _MASTER_CONN_LOGGED = True

    return conn


def _log_db(event: str, **kwargs):
    try:
        _master_logger.info("%s %s", event, json.dumps(kwargs, default=str))
    except Exception:
        _master_logger.info("%s %s", event, kwargs)


def _log_db_exc(event: str, **kwargs):
    try:
        _master_logger.exception("%s %s", event, json.dumps(kwargs, default=str))
    except Exception:
        _master_logger.exception("%s %s", event, kwargs)


def qn(name: str) -> str:
    """Quote names for the current master connection."""
    conn = get_master_connection()
    return conn.ops.quote_name(name)


def normalize_wa_for_lookup(raw: str) -> str:
    if raw is None:
        return ""
    digits = re.sub(r"\D", "", str(raw))
    # Keep last 10 digits for Indian numbers
    if len(digits) > 10:
        digits = digits[-10:]
    return digits


def _normalize_uuid_for_mysql(value: str) -> str:
    """UUID -> 32hex without hyphens."""
    return (value or "").strip().replace("-", "")


# -----------------------------------------------------------------------------
# MASTER enrollment table discovery (legacy)
# -----------------------------------------------------------------------------

_ENROLLMENT_META_CACHE: Optional[dict] = None


def _db_schema_name(conn) -> str:
    """
    Determine current DB/schema name for INFORMATION_SCHEMA queries.
    """
    try:
        # MySQL
        return conn.settings_dict.get("NAME") or ""
    except Exception:
        return ""


def _table_exists(conn, table: str) -> bool:
    schema = _db_schema_name(conn)
    if not schema:
        return False
    sql = """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        LIMIT 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [schema, table])
            return cur.fetchone() is not None
    except Exception:
        return False


def _get_table_columns(conn, table: str) -> list[str]:
    schema = _db_schema_name(conn)
    if not schema:
        return []
    sql = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    with conn.cursor() as cur:
        cur.execute(sql, [schema, table])
        rows = cur.fetchall() or []
    return [r[0] for r in rows if r and r[0]]


def _pick_first_column(cols: list[str], candidates: list[str]) -> str:
    """
    Return first candidate that exists in cols (case-insensitive).
    """
    low = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    return ""


def _get_enrollment_meta() -> dict:
    """
    Legacy discovery path (kept for compatibility).
    This is NOT sufficient for campaign_doctorcampaignenrollment schema,
    but we keep it as a fallback if campaign_* tables are absent.
    """
    global _ENROLLMENT_META_CACHE
    if _ENROLLMENT_META_CACHE is not None:
        return _ENROLLMENT_META_CACHE

    conn = get_master_connection()

    # Default to the known Django table name if present
    candidate_tables = [
        getattr(settings, "MASTER_DB_ENROLLMENT_TABLE", ""),
        "campaign_doctorcampaignenrollment",
        "campaign_doctor_campaigns",
    ]
    candidate_tables = [t for t in candidate_tables if t]

    table = ""
    for t in candidate_tables:
        if _table_exists(conn, t):
            table = t
            break

    if not table:
        # As a last resort, keep old behavior: assume it exists
        table = getattr(settings, "MASTER_DB_ENROLLMENT_TABLE", "campaign_doctorcampaignenrollment")

    cols = _get_table_columns(conn, table)

    # Heuristics
    doctor_col = _pick_first_column(cols, ["doctor_id", "doctor", "redflags_doctor_id", "doctor_code"])
    campaign_col = _pick_first_column(cols, ["campaign_id", "campaign"])
    registered_by_col = _pick_first_column(cols, ["registered_by_id", "registered_by", "field_rep_id"])

    if not doctor_col:
        doctor_col = "doctor_id"
    if not campaign_col:
        campaign_col = "campaign_id"

    _ENROLLMENT_META_CACHE = {
        "table": table,
        "doctor_col": doctor_col,
        "campaign_col": campaign_col,
        "registered_by_col": registered_by_col,
    }
    return _ENROLLMENT_META_CACHE


# -----------------------------------------------------------------------------
# Doctor lookup helpers
# -----------------------------------------------------------------------------

@dataclass
class MasterDoctor:
    doctor_id: str
    email: str
    whatsapp_no: str


def find_doctor_by_email_or_whatsapp(*, email: str, whatsapp_no: str) -> Optional[MasterDoctor]:
    alias = master_alias()
    email = (email or "").strip().lower()
    wa = normalize_wa_for_lookup(whatsapp_no)

    if not email and not wa:
        return None

    qs = RedflagsDoctor.objects.using(alias).all()

    q = Q()
    if email:
        q |= Q(email__iexact=email)
    if wa:
        q |= Q(whatsapp_no__endswith=wa)

    row = qs.filter(q).only("doctor_id", "email", "whatsapp_no").first()
    if not row:
        return None

    return MasterDoctor(
        doctor_id=str(row.doctor_id),
        email=str(row.email or ""),
        whatsapp_no=str(row.whatsapp_no or ""),
    )


# -----------------------------------------------------------------------------
# Doctor create/update in MASTER redflags_doctor
# -----------------------------------------------------------------------------

def create_master_doctor_id() -> str:
    # DR + 6 digits (simple)
    return f"DR{secrets.randbelow(900000) + 100000}"


def insert_redflags_doctor(
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
    field_rep_id: str,
    recruited_via: str,
) -> None:
    conn = get_master_connection()
    table = "redflags_doctor"

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


# -----------------------------------------------------------------------------
# Campaign enrollment (FIXED)
# -----------------------------------------------------------------------------

def _campaign_exists(conn, campaign_id_norm: str) -> bool:
    """Returns True if campaign exists in MASTER campaign_campaign."""
    cid = (campaign_id_norm or "").strip()
    if not cid:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {qn('campaign_campaign')} WHERE {qn('id')}=%s LIMIT 1",
                [cid],
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def _row_exists_by_id(conn, table: str, row_id: int, *, id_col: str = "id") -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {qn(table)} WHERE {qn(id_col)}=%s LIMIT 1",
                [int(row_id)],
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def normalize_campaign_id(campaign_id: str) -> str:
    """
    MASTER join tables store campaign_id WITHOUT hyphens:
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


def _resolve_registered_by_fieldrep_id(conn, *, campaign_id_norm: str, registered_by: str) -> Optional[int]:
    """
    Resolve `registered_by` (from URL/form) to MASTER campaign_fieldrep.id if possible.

    Supported real-world inputs:
      - "15" (fieldrep id OR join-table pk)
      - "fieldrep_15" (token style)
    """
    raw = (registered_by or "").strip()
    if not raw:
        return None

    # Extract trailing digits (handles "fieldrep_15")
    m = re.search(r"(\d+)$", raw)
    if not m:
        return None

    try:
        cand = int(m.group(1))
    except Exception:
        return None

    # 1) direct campaign_fieldrep.id
    if _row_exists_by_id(conn, "campaign_fieldrep", cand, id_col="id"):
        return cand

    # 2) treat as join-table pk in campaign_campaignfieldrep => resolve to field_rep_id
    try:
        fr_id = get_campaign_fieldrep_link_fieldrep_id(campaign_id=campaign_id_norm, link_pk=cand)
    except Exception:
        fr_id = None

    if fr_id and _row_exists_by_id(conn, "campaign_fieldrep", int(fr_id), id_col="id"):
        return int(fr_id)

    return None


def _get_or_create_campaign_doctor_id(
    conn,
    *,
    full_name: str,
    email: str,
    phone: str,
    city: str = "",
    state: str = "",
) -> Optional[int]:
    """
    Ensure a row exists in MASTER campaign_doctor and return its numeric id.

    Matching:
      - LOWER(email) exact OR RIGHT(phone, 10) match (handles +91 / 91 prefixes)
    """
    email_l = (email or "").strip().lower()
    phone_digits = re.sub(r"\D", "", str(phone or ""))
    phone_last10 = phone_digits[-10:] if len(phone_digits) > 10 else phone_digits

    if not email_l and not phone_last10:
        return None

    where_parts = []
    params = []

    if email_l:
        where_parts.append(f"LOWER({qn('email')})=%s")
        params.append(email_l)

    if phone_last10:
        where_parts.append(f"RIGHT({qn('phone')}, 10)=%s")
        params.append(phone_last10)

    where_sql = " OR ".join(where_parts)

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {qn('id')} FROM {qn('campaign_doctor')} WHERE {where_sql} ORDER BY {qn('id')} DESC LIMIT 1",
                params,
            )
            row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        # fall through to create
        pass

    # Create (best-effort)
    full_name_n = (full_name or "").strip() or (email_l or phone_last10 or "")
    city_n = (city or "").strip()
    state_n = (state or "").strip()

    phone_store = phone_digits or (phone or "").strip()

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {qn('campaign_doctor')}
                    ({qn('full_name')}, {qn('email')}, {qn('phone')}, {qn('city')}, {qn('state')}, {qn('created_at')})
                VALUES
                    (%s, %s, %s, %s, %s, NOW(6))
                """,
                [full_name_n, email_l, phone_store, city_n, state_n],
            )
            return int(getattr(cur, "lastrowid", 0) or 0) or None
    except Exception:
        return None


def ensure_enrollment(*, doctor_id: str, campaign_id: str, registered_by: str) -> None:
    """
    Ensure a doctor is enrolled into a campaign in MASTER DB.

    MASTER uses:
      1) `redflags_doctor` (login/profile) keyed by string doctor_id (e.g. DR061755)
      2) `campaign_doctor` + `campaign_doctorcampaignenrollment` for campaign membership (doctor_id is BIGINT FK)

    This function:
      - Normalizes campaign_id (UUID -> 32-hex without hyphens)
      - Creates/gets `campaign_doctor` row using redflags doctor email/phone
      - Inserts into `campaign_doctorcampaignenrollment` with required NOT NULL columns
      - Idempotent (won’t create duplicates)

    Fallback:
      - If campaign tables aren't present, uses legacy discovery path (_get_enrollment_meta).
    """
    _log_db("master_db.enrollment.ensure.start", doctor_id=doctor_id, campaign_id=campaign_id)

    if not (doctor_id and campaign_id):
        return

    conn = get_master_connection()
    cid_norm = normalize_campaign_id(campaign_id) or _normalize_uuid_for_mysql(campaign_id)

    # Preferred path: campaign_* tables exist
    try:
        if _table_exists(conn, "campaign_doctor") and _table_exists(conn, "campaign_doctorcampaignenrollment") and _table_exists(conn, "campaign_campaign"):
            if not _campaign_exists(conn, cid_norm):
                _log_db("master_db.enrollment.skip.campaign_missing", doctor_id=doctor_id, campaign_id=cid_norm)
                return

            # Resolve numeric campaign_doctor.id
            campaign_doctor_id: Optional[int] = None

            if str(doctor_id).strip().isdigit():
                campaign_doctor_id = int(str(doctor_id).strip())
                if not _row_exists_by_id(conn, "campaign_doctor", campaign_doctor_id, id_col="id"):
                    campaign_doctor_id = None
            else:
                alias = master_alias()
                doc = (
                    RedflagsDoctor.objects.using(alias)
                    .filter(doctor_id=str(doctor_id).strip())
                    .only("first_name", "last_name", "email", "whatsapp_no", "district", "state")
                    .first()
                )
                if not doc:
                    _log_db("master_db.enrollment.skip.redflags_doctor_missing", doctor_id=doctor_id, campaign_id=cid_norm)
                    return

                full_name = (f"{(doc.first_name or '').strip()} {(doc.last_name or '').strip()}").strip()
                email = (doc.email or "").strip()
                phone = (doc.whatsapp_no or "").strip()
                city = (getattr(doc, "district", "") or "").strip()
                state = (getattr(doc, "state", "") or "").strip()

                campaign_doctor_id = _get_or_create_campaign_doctor_id(
                    conn,
                    full_name=full_name,
                    email=email,
                    phone=phone,
                    city=city,
                    state=state,
                )

            if not campaign_doctor_id:
                _log_db("master_db.enrollment.skip.campaign_doctor_unresolved", doctor_id=doctor_id, campaign_id=cid_norm)
                return

            # Idempotency check
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {qn('campaign_doctorcampaignenrollment')} WHERE {qn('campaign_id')}=%s AND {qn('doctor_id')}=%s LIMIT 1",
                    [cid_norm, campaign_doctor_id],
                )
                if cur.fetchone() is not None:
                    _log_db("master_db.enrollment.exists", doctor_id=doctor_id, campaign_id=cid_norm)
                    return

            # Insert enrollment row (schema: campaign_doctorcampaignenrollment).
            # We intentionally avoid INFORMATION_SCHEMA dependency here because some DB users
            # do not have permissions for it, but do have INSERT/SELECT permissions.
            fr_id = _resolve_registered_by_fieldrep_id(
                conn, campaign_id_norm=cid_norm, registered_by=registered_by
            )

            try:
                # Full schema (includes registered_by_id)
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT IGNORE INTO {qn('campaign_doctorcampaignenrollment')}
                            ({qn('whitelabel_enabled')}, {qn('whitelabel_subdomain')}, {qn('registered_at')},
                             {qn('campaign_id')}, {qn('doctor_id')}, {qn('registered_by_id')})
                        VALUES
                            (%s, %s, NOW(6), %s, %s, %s)
                        """,
                        [1, "", cid_norm, campaign_doctor_id, fr_id],
                    )
            except Exception:
                # Older schema without registered_by_id (still must satisfy NOT NULL columns)
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT IGNORE INTO {qn('campaign_doctorcampaignenrollment')}
                            ({qn('whitelabel_enabled')}, {qn('whitelabel_subdomain')}, {qn('registered_at')},
                             {qn('campaign_id')}, {qn('doctor_id')})
                        VALUES
                            (%s, %s, NOW(6), %s, %s)
                        """,
                        [1, "", cid_norm, campaign_doctor_id],
                    )

            _log_db(
                "master_db.enrollment.ensure.done",
                doctor_id=doctor_id,
                campaign_id=cid_norm,
                campaign_doctor_id=campaign_doctor_id,
            )
            return
    except Exception:
        _log_db_exc("master_db.enrollment.ensure.error", doctor_id=doctor_id, campaign_id=campaign_id)

    # Fallback path: legacy meta-driven insert
    try:
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

        with conn.cursor() as cur:
            cur.execute(sql, vals)

        _log_db("master_db.enrollment.ensure.fallback_done", doctor_id=doctor_id, campaign_id=cid_norm)
    except Exception:
        _log_db_exc("master_db.enrollment.ensure.fallback_error", doctor_id=doctor_id, campaign_id=campaign_id)


# -----------------------------------------------------------------------------
# Doctor create with enrollment
# -----------------------------------------------------------------------------

def create_doctor_with_enrollment(
    *,
    first_name: str,
    last_name: str,
    email: str,
    whatsapp_no: str,
    clinic_name: str,
    imc_registration_number: str,
    clinic_phone: str,
    clinic_appointment_number: str,
    clinic_address: str,
    postal_code: str,
    state: str,
    district: str,
    receptionist_whatsapp_number: str,
    photo_path: str,
    campaign_id: str,
    registered_by: str,
) -> str:
    """
    Creates doctor in MASTER redflags_doctor and enrolls into campaign tables.
    Returns created doctor_id (DRxxxxxx).
    """
    alias = master_alias()
    doctor_id = create_master_doctor_id()

    with transaction.atomic(using=alias):
        # Create doctor in redflags_doctor (ORM)
        doc = RedflagsDoctor(
            doctor_id=doctor_id,
            first_name=(first_name or "").strip(),
            last_name=(last_name or "").strip(),
            email=(email or "").strip().lower(),
            clinic_name=(clinic_name or "").strip(),
            imc_registration_number=(imc_registration_number or "").strip(),
            clinic_phone=(clinic_phone or "").strip(),
            clinic_appointment_number=(clinic_appointment_number or "").strip(),
            clinic_address=(clinic_address or "").strip(),
            postal_code=(postal_code or "").strip(),
            state=(state or "").strip(),
            district=(district or "").strip(),
            whatsapp_no=normalize_wa_for_lookup(whatsapp_no) or (whatsapp_no or "").strip(),
            receptionist_whatsapp_number=normalize_wa_for_lookup(receptionist_whatsapp_number) or (receptionist_whatsapp_number or "").strip(),
            photo=(photo_path or "").strip(),
            field_rep_id=(registered_by or "").strip(),
            recruited_via="FIELD_REP" if registered_by else "SELF",
            password=make_password(secrets.token_urlsafe(12)),  # temp, not used if you manage separately
        )
        doc.save(using=alias)

        # Enroll into campaign tables (FIXED)
        if campaign_id:
            ensure_enrollment(doctor_id=doctor_id, campaign_id=campaign_id, registered_by=registered_by or "")

    return doctor_id
