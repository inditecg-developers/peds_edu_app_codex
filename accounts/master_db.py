
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

def _mask_email_for_log(email: str) -> str:
    e = (email or "").strip()
    if not e or "@" not in e:
        return (e[:2] + "…") if e else ""
    local, domain = e.split("@", 1)
    return (local[:2] + "…@" + domain) if local else ("…@" + domain)


def authorized_publisher_exists(email: str) -> bool:
    """
    Checks AuthorizedPublisher in MASTER DB.

    - Never raises (missing table/column previously caused 401).
    - Tries configured table first, then a small list of common fallback table names.
    """
    e = (email or "").strip().lower()
    if not e:
        return False

    conn = get_master_connection()

    cfg_table = getattr(settings, "MASTER_DB_AUTH_PUBLISHER_TABLE", "publisher_authorizedpublisher")
    cfg_col = getattr(settings, "MASTER_DB_AUTH_PUBLISHER_EMAIL_COLUMN", "email")

    candidates = [
        (cfg_table, cfg_col),
        ("campaign_authorizedpublisher", "email"),
        ("publisher_authorizedpublisher", "email"),
        ("authorized_publisher", "email"),
        ("authorizedpublisher", "email"),
    ]

    masked = _mask_email_for_log(e)

    last_err = None
    for table, col in candidates:
        try:
            sql = f"SELECT 1 FROM {qn(table)} WHERE LOWER({qn(col)}) = LOWER(%s) LIMIT 1"
            with conn.cursor() as cur:
                cur.execute(sql, [e])
                ok = cur.fetchone() is not None
            if ok:
                return True
        except Exception as ex:
            last_err = f"{type(ex).__name__}: {ex}"
            continue

    return False



# def authorized_publisher_exists(email: str) -> bool:
#     """
#     Checks AuthorizedPublisher in MASTER DB.

#     Tries configured table first, then a few fallback table names.
#     Never raises (returns False on errors), but logs diagnostics.
#     """
#     e = (email or "").strip().lower()
#     if not e:
#         _log_db("publisher_auth.empty_email")
#         return False

#     conn = get_master_connection()

#     cfg_table = getattr(settings, "MASTER_DB_AUTH_PUBLISHER_TABLE", "publisher_authorizedpublisher")
#     cfg_col = getattr(settings, "MASTER_DB_AUTH_PUBLISHER_EMAIL_COLUMN", "email")

#     candidates = [
#         (cfg_table, cfg_col),
#         ("campaign_authorizedpublisher", "email"),
#         ("publisher_authorizedpublisher", "email"),
#         ("authorized_publisher", "email"),
#         ("authorizedpublisher", "email"),
#     ]

#     last_err = None
#     for table, col in candidates:
#         try:
#             sql = f"SELECT 1 FROM {qn(table)} WHERE LOWER({qn(col)}) = LOWER(%s) LIMIT 1"
#             with conn.cursor() as cur:
#                 cur.execute(sql, [e])
#                 if cur.fetchone() is not None:
#                     _log_db("publisher_auth.ok", table=table, col=col)
#                     return True

#             _log_db("publisher_auth.no_match", table=table, col=col)

#         except Exception as ex:
#             last_err = f"{type(ex).__name__}: {ex}"
#             _log_db("publisher_auth.check_error", table=table, col=col, error=last_err)
#             continue

#     _log_db("publisher_auth.not_found", configured_table=cfg_table, configured_col=cfg_col, last_error=last_err or "")
#     return False

#def master_alias() -> str:
 #   return getattr(settings, "MASTER_DB_ALIAS", "master")

def master_alias() -> str:
    return getattr(settings, "MASTER_DB_ALIAS", "master")


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




def build_whatsapp_deeplink(phone_number: str, message: str) -> str:
    """Build a WhatsApp deep-link (wa.me) for a given phone number and message.

    - Accepts phone number in many common formats (spaces, +91, 0-prefix, etc.)
    - If a 10-digit number is provided, assumes India and prefixes country code 91.
    - Message is URL-encoded; newlines become %0A.

    Returns a URL suitable for redirecting a browser (mobile will open WhatsApp app when available).
    """
    digits = re.sub(r"\D", "", str(phone_number or ""))
    if digits:
        # Drop leading zeros (common when people enter 0XXXXXXXXXX)
        while digits.startswith("0") and len(digits) > 10:
            digits = digits[1:]

        # If it looks like an Indian 10-digit mobile number, prefix country code.
        if len(digits) == 10:
            digits = "91" + digits

    text = quote(str(message or ""), safe="")

    if digits:
        return f"https://wa.me/{digits}?text={text}"
    return f"https://wa.me/?text={text}"
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
      - "FR09" (brand_supplied_field_rep_id)
    """
    raw = (registered_by or "").strip()
    if not raw:
        return None

    # 0) Direct lookup in campaign_fieldrep (pk or external brand-supplied id)
    try:
        fr = get_field_rep(raw)  # supports pk id, token ids, and brand_supplied_field_rep_id (FR09)
        if fr:
            return int(fr.id)
    except Exception:
        pass

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
    doctor_id: str = "",
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
    campaign_id: str = "",
    registered_by: str = "",
    recruited_via: str = "",
    initial_password_raw: str | None = None,
) -> str:
    """
    Creates doctor in MASTER redflags_doctor and enrolls into campaign tables.
    Returns created doctor_id (e.g. DR123456).

    IMPORTANT (MASTER DB schema alignment):
      - redflags_doctor has several NOT NULL columns without defaults (clinic_password_hash,
        clinic_user1_*, clinic_user2_*). Django model fields allow NULL, so we MUST supply
        values explicitly to avoid IntegrityError.
      - Portal login uses clinic_password_hash; we store a Django hash when
        initial_password_raw is provided.
    """

    alias = master_alias()

    # ------------------------------------------------------------------
    # doctor_id (optional pre-generated) — avoid collisions
    # ------------------------------------------------------------------
    did = (doctor_id or "").strip()
    if not did:
        for _ in range(15):
            cand = create_master_doctor_id()
            try:
                if not RedflagsDoctor.objects.using(alias).filter(doctor_id=cand).exists():
                    did = cand
                    break
            except Exception:
                # If the existence check fails (rare), fall back to the candidate
                did = cand
                break
        if not did:
            did = create_master_doctor_id()

    # ------------------------------------------------------------------
    # Normalize inputs and guarantee NOT NULL columns get non-NULL values
    # ------------------------------------------------------------------
    email_l = (email or "").strip().lower()

    wa = normalize_wa_for_lookup(whatsapp_no) or (whatsapp_no or "").strip()
    rec_wa = normalize_wa_for_lookup(receptionist_whatsapp_number) or (receptionist_whatsapp_number or "").strip()

    campaign_id_s = (campaign_id or "").strip()
    registered_by_s = (registered_by or "").strip()

    recruited_via_s = (recruited_via or "").strip()
    if not recruited_via_s:
        recruited_via_s = "FIELD_REP" if registered_by_s else "SELF"

    # Password handling (MASTER stores clinic_password_hash)
    pwd_hash = ""
    pwd_set_at = None
    if initial_password_raw:
        pwd_hash = make_password(initial_password_raw)
        try:
            pwd_set_at = timezone.now()
        except Exception:
            pwd_set_at = None

    # MASTER schema requires these NOT NULL (empty string is OK)
    user1_name = ""
    user1_email = ""
    user1_pwd = ""
    user2_name = ""
    user2_email = ""
    user2_pwd = ""

    with transaction.atomic(using=alias):
        doc = RedflagsDoctor(
            doctor_id=did,
            first_name=(first_name or "").strip(),
            last_name=(last_name or "").strip(),
            email=email_l,
            whatsapp_no=wa,
            clinic_name=(clinic_name or "").strip(),
            clinic_phone=(clinic_phone or "").strip(),
            clinic_appointment_number=(clinic_appointment_number or "").strip(),
            clinic_address=(clinic_address or "").strip(),
            imc_registration_number=(imc_registration_number or "").strip(),
            photo=(photo_path or "").strip() or None,
            postal_code=(postal_code or "").strip(),
            state=(state or "").strip(),
            district=(district or "").strip(),
            receptionist_whatsapp_number=rec_wa,
            field_rep_id=registered_by_s or "",
            recruited_via=recruited_via_s,
            clinic_password_hash=pwd_hash,
            clinic_password_set_at=pwd_set_at,
            clinic_user1_name=user1_name,
            clinic_user1_email=user1_email,
            clinic_user1_password_hash=user1_pwd,
            clinic_user2_name=user2_name,
            clinic_user2_email=user2_email,
            clinic_user2_password_hash=user2_pwd,
        )
        doc.save(using=alias)

        # Enroll into campaign tables (best-effort; ensure_enrollment never raises by design)
        if campaign_id_s:
            ensure_enrollment(doctor_id=did, campaign_id=campaign_id_s, registered_by=registered_by_s or "")

    return did



# =============================================================================
# Campaign fetch (MASTER DB) — robust + includes banners
# DO NOT MOVE ABOVE: kept at end so it safely overrides any older get_campaign()
# =============================================================================

@dataclass(frozen=True)
class MasterCampaign:
    campaign_id: str
    doctors_supported: int
    wa_addition: str
    new_video_cluster_name: str
    email_registration: str

    # NEW: banner URLs stored in MASTER campaign_campaign
    banner_small_url: str = ""
    banner_large_url: str = ""
    banner_target_url: str = ""


def get_campaign(campaign_id: str) -> Optional[MasterCampaign]:
    """
    Fetch campaign details from MASTER DB (campaign_campaign).

    Must support:
      - UUID with hyphens (9ca882cf-13da-...)
      - 32-hex without hyphens (9ca882cf13da...)

    Must return banner_small_url / banner_large_url / banner_target_url.
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

    # banner cols are fixed in your schema; allow override via settings if ever needed
    bs_col = getattr(settings, "MASTER_DB_CAMPAIGN_BANNER_SMALL_URL_COLUMN", "banner_small_url")
    bl_col = getattr(settings, "MASTER_DB_CAMPAIGN_BANNER_LARGE_URL_COLUMN", "banner_large_url")
    bt_col = getattr(settings, "MASTER_DB_CAMPAIGN_BANNER_TARGET_URL_COLUMN", "banner_target_url")

    # Some DBs store id as CHAR(32) (no hyphens). We query both.
    sql = (
        f"SELECT {qn(id_col)}, {qn(ds_col)}, {qn(wa_col)}, {qn(vc_col)}, {qn(er_col)}, "
        f"{qn(bs_col)}, {qn(bl_col)}, {qn(bt_col)} "
        f"FROM {qn(table)} "
        f"WHERE {qn(id_col)} = %s OR {qn(id_col)} = %s "
        f"LIMIT 1"
    )

    try:
        with conn.cursor() as cur:
            cur.execute(sql, [cid_norm, cid_raw])
            row = cur.fetchone()
    except Exception as ex:
        _log_db_exc(
            "master_db.get_campaign.error",
            campaign_id=cid_raw,
            campaign_id_norm=cid_norm,
            table=table,
            id_col=id_col,
            error=f"{type(ex).__name__}: {ex}",
        )
        return None

    if not row:
        _log_db(
            "master_db.get_campaign.not_found",
            campaign_id=cid_raw,
            campaign_id_norm=cid_norm,
            table=table,
            id_col=id_col,
        )
        return None

    # row layout matches SELECT order
    try:
        ds_val = int(row[1] or 0)
    except Exception:
        ds_val = 0

    return MasterCampaign(
        campaign_id=str(row[0] or "").strip(),
        doctors_supported=ds_val,
        wa_addition=str(row[2] or ""),
        new_video_cluster_name=str(row[3] or ""),
        email_registration=str(row[4] or ""),
        banner_small_url=str(row[5] or ""),
        banner_large_url=str(row[6] or ""),
        banner_target_url=str(row[7] or ""),
    )



# =============================================================================
# FieldRep fetch (MASTER DB) — robust override
# Appended at end intentionally (does not remove any existing code).
# =============================================================================

@dataclass(frozen=True)
class MasterFieldRep:
    id: int
    full_name: str
    phone_number: str
    is_active: bool
    brand_supplied_field_rep_id: str = ""


def get_field_rep(field_rep_id: str) -> Optional[MasterFieldRep]:
    """
    Robust FieldRep lookup against MASTER DB.

    Supports:
      - numeric pk id (e.g. "12")
      - brand_supplied_field_rep_id (e.g. "FR09")
      - token-style ids (e.g. "fieldrep_12")

    Reads from settings:
      MASTER_DB_FIELD_REP_TABLE (default campaign_fieldrep)
      MASTER_DB_FIELD_REP_PK_COLUMN (default id)
      MASTER_DB_FIELD_REP_ACTIVE_COLUMN (default is_active)
      MASTER_DB_FIELD_REP_FULL_NAME_COLUMN (default full_name)
      MASTER_DB_FIELD_REP_PHONE_COLUMN (default phone_number)
      MASTER_DB_FIELD_REP_EXTERNAL_ID_COLUMN (default brand_supplied_field_rep_id)
    """
    raw = (field_rep_id or "").strip()
    if not raw:
        return None

    # Extract trailing digits from token-style inputs like "fieldrep_12"
    m = re.search(r"(\d+)$", raw)
    numeric_candidate = m.group(1) if m else ""
    is_numeric = raw.isdigit() or bool(numeric_candidate)

    conn = get_master_connection()

    table = getattr(settings, "MASTER_DB_FIELD_REP_TABLE", "campaign_fieldrep")
    pk_col = getattr(settings, "MASTER_DB_FIELD_REP_PK_COLUMN", "id")
    active_col = getattr(settings, "MASTER_DB_FIELD_REP_ACTIVE_COLUMN", "is_active")
    name_col = getattr(settings, "MASTER_DB_FIELD_REP_FULL_NAME_COLUMN", "full_name")
    phone_col = getattr(settings, "MASTER_DB_FIELD_REP_PHONE_COLUMN", "phone_number")
    ext_col = getattr(settings, "MASTER_DB_FIELD_REP_EXTERNAL_ID_COLUMN", "brand_supplied_field_rep_id")

    # 1) Try primary key lookup if numeric
    if is_numeric:
        try:
            pk = int(raw) if raw.isdigit() else int(numeric_candidate)
        except Exception:
            pk = None

        if pk is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT {qn(pk_col)}, {qn(name_col)}, {qn(phone_col)}, {qn(active_col)}, {qn(ext_col)}
                        FROM {qn(table)}
                        WHERE {qn(pk_col)} = %s
                        LIMIT 1
                        """,
                        [pk],
                    )
                    row = cur.fetchone()
                if row:
                    return MasterFieldRep(
                        id=int(row[0]),
                        full_name=str(row[1] or "").strip(),
                        phone_number=str(row[2] or "").strip(),
                        is_active=bool(int(row[3] or 0)) if str(row[3] or "").isdigit() else bool(row[3]),
                        brand_supplied_field_rep_id=str(row[4] or "").strip(),
                    )
            except Exception as ex:
                _log_db_exc("master_db.get_field_rep.pk_lookup_error", field_rep_id=raw, error=f"{type(ex).__name__}: {ex}")

    # 2) Try external brand-supplied id lookup (FR09 etc)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {qn(pk_col)}, {qn(name_col)}, {qn(phone_col)}, {qn(active_col)}, {qn(ext_col)}
                FROM {qn(table)}
                WHERE {qn(ext_col)} = %s
                LIMIT 1
                """,
                [raw],
            )
            row = cur.fetchone()
        if row:
            return MasterFieldRep(
                id=int(row[0]),
                full_name=str(row[1] or "").strip(),
                phone_number=str(row[2] or "").strip(),
                is_active=bool(int(row[3] or 0)) if str(row[3] or "").isdigit() else bool(row[3]),
                brand_supplied_field_rep_id=str(row[4] or "").strip(),
            )
    except Exception as ex:
        _log_db_exc("master_db.get_field_rep.external_lookup_error", field_rep_id=raw, error=f"{type(ex).__name__}: {ex}")

    return None

# =============================================================================
# Enrollment count (MASTER DB) — robust override
# Appended at end intentionally (does not remove any existing code).
# =============================================================================

def count_campaign_enrollments(campaign_id: str) -> int:
    """
    Counts enrolled doctors for a campaign in MASTER DB.

    Supports both possible schemas:

    A) New campaigns schema:
       - table: campaign_doctorcampaignenrollment
       - columns: campaign_id (CHAR32), doctor_id (BIGINT FK -> campaign_doctor.id)
       - may optionally have: active

    B) Legacy schema (older admin DB):
       - table: DoctorCampaignEnrollment (or settings.MASTER_DB_ENROLLMENT_TABLE)
       - columns commonly: campaign_id, doctor_id
       - may optionally have: active

    Always returns int, never raises.
    """
    cid_raw = (campaign_id or "").strip()
    if not cid_raw:
        return 0

    # Normalize to 32-char (no hyphens) for campaign tables that store char32 IDs
    cid_norm = cid_raw.replace("-", "")

    conn = get_master_connection()

    # Prefer the actual campaign enrollment table if present
    preferred = "campaign_doctorcampaignenrollment"
    configured = getattr(settings, "MASTER_DB_ENROLLMENT_TABLE", "") or ""
    candidates = [preferred]
    if configured and configured not in candidates:
        candidates.append(configured)

    # Fallbacks you might have in older DBs
    for t in ("DoctorCampaignEnrollment", "campaign_doctor_campaigns"):
        if t not in candidates:
            candidates.append(t)

    table = None
    for t in candidates:
        try:
            if _table_exists(conn, t):
                table = t
                break
        except Exception:
            continue

    if not table:
        _log_db("master_db.count_campaign_enrollments.no_table", campaign_id=cid_raw)
        return 0

    # Identify columns safely (case-insensitive)
    try:
        cols = _get_table_columns(conn, table)
        cols_l = {c.lower(): c for c in cols}
    except Exception:
        cols = []
        cols_l = {}

    campaign_col = cols_l.get("campaign_id") or getattr(settings, "MASTER_DB_ENROLLMENT_CAMPAIGN_COLUMN", "campaign_id")
    doctor_col = cols_l.get("doctor_id") or getattr(settings, "MASTER_DB_ENROLLMENT_DOCTOR_COLUMN", "doctor_id")

    # Optional active column
    active_col = cols_l.get("active")

    # Build WHERE: try both cid_norm and cid_raw because some tables store hyphenated UUIDs
    where = f"{qn(campaign_col)} = %s OR {qn(campaign_col)} = %s"
    params = [cid_norm, cid_raw]

    if active_col:
        where = f"({where}) AND {qn(active_col)} = 1"

    # Count distinct doctors
    sql = f"""
        SELECT COUNT(DISTINCT {qn(doctor_col)})
        FROM {qn(table)}
        WHERE {where}
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    except Exception as ex:
        _log_db_exc(
            "master_db.count_campaign_enrollments.error",
            table=table,
            campaign_id=cid_raw,
            campaign_id_norm=cid_norm,
            error=f"{type(ex).__name__}: {ex}",
        )
        return 0

# =============================================================================
# Doctor lookup by WhatsApp (MASTER DB) — robust override
# Appended at end intentionally (does not remove any existing code).
# =============================================================================

@dataclass(frozen=True)
class MasterDoctorLite:
    doctor_id: str
    email: str
    full_name: str
    whatsapp_no: str


def get_doctor_by_whatsapp(whatsapp_number: str) -> Optional[MasterDoctorLite]:
    """
    Looks up doctor in MASTER redflags_doctor by WhatsApp number.

    - Normalizes to digits and matches by last-10 digits (handles +91/91 prefix).
    - Uses settings MASTER_DB_DOCTOR_TABLE + column names if provided, else defaults to redflags_doctor schema.
    - Never raises; returns None on not found.
    """
    raw = (whatsapp_number or "").strip()
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None
    last10 = digits[-10:] if len(digits) > 10 else digits

    conn = get_master_connection()

    # Your live schema is redflags_doctor (as per your settings bottom block)
    table = getattr(settings, "MASTER_DB_DOCTOR_TABLE", "redflags_doctor")
    id_col = getattr(settings, "MASTER_DB_DOCTOR_ID_COLUMN", "doctor_id")
    fn_col = getattr(settings, "MASTER_DB_DOCTOR_FIRST_NAME_COLUMN", "first_name")
    ln_col = getattr(settings, "MASTER_DB_DOCTOR_LAST_NAME_COLUMN", "last_name")
    email_col = getattr(settings, "MASTER_DB_DOCTOR_EMAIL_COLUMN", "email")
    wa_col = getattr(settings, "MASTER_DB_DOCTOR_WHATSAPP_COLUMN", "whatsapp_no")

    # We match on RIGHT(whatsapp_no,10) to tolerate stored +91/91 prefixes or longer numbers.
    sql = f"""
        SELECT {qn(id_col)}, {qn(fn_col)}, {qn(ln_col)}, {qn(email_col)}, {qn(wa_col)}
        FROM {qn(table)}
        WHERE RIGHT({qn(wa_col)}, 10) = %s
           OR {qn(wa_col)} = %s
        LIMIT 1
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql, [last10, digits])
            row = cur.fetchone()
    except Exception as ex:
        _log_db_exc(
            "master_db.get_doctor_by_whatsapp.error",
            table=table,
            whatsapp_last10=last10,
            error=f"{type(ex).__name__}: {ex}",
        )
        return None

    if not row:
        return None

    doctor_id = str(row[0] or "").strip()
    first = str(row[1] or "").strip()
    last = str(row[2] or "").strip()
    email = str(row[3] or "").strip()
    wa = str(row[4] or "").strip()

    full_name = (f"{first} {last}").strip() or doctor_id or "Doctor"

    return MasterDoctorLite(
        doctor_id=doctor_id,
        email=email,
        full_name=full_name,
        whatsapp_no=wa,
    )

# -----------------------------------------------------------------------------
# Compatibility aliases (do NOT remove)
# -----------------------------------------------------------------------------

# Keep a local fallback generator so registration never fails because of an import issue.
try:
    from peds_edu.master_db import generate_temporary_password as _gen_tmp_pwd  # type: ignore
except Exception:
    _gen_tmp_pwd = None


def generate_temporary_password(length: int = 10) -> str:
    if _gen_tmp_pwd:
        try:
            return _gen_tmp_pwd(length=length)
        except Exception:
            pass

    # Fallback: excludes ambiguous characters for phone dictation.
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
    try:
        n = max(8, int(length))
    except Exception:
        n = 10
    return "".join(secrets.choice(alphabet) for _ in range(n))


def generate_doctor_id() -> str:
    return create_master_doctor_id()


# Preserve the core implementation before overriding the public name below.
_create_doctor_with_enrollment_impl = create_doctor_with_enrollment


def create_doctor_with_enrollment_compat(**kwargs) -> str:
    """Backwards/forwards compatible wrapper for create_doctor_with_enrollment()."""
    rec_wa = (
        (kwargs.get("receptionist_whatsapp_number") or "").strip()
        or (kwargs.get("clinic_whatsapp_number") or "").strip()
        or (kwargs.get("clinic_whatsapp") or "").strip()
    )

    mapped = {
        "doctor_id": (kwargs.get("doctor_id") or "").strip(),
        "first_name": (kwargs.get("first_name") or "").strip(),
        "last_name": (kwargs.get("last_name") or "").strip(),
        "email": (kwargs.get("email") or "").strip(),
        # Some legacy call sites used "whatsapp" instead of "whatsapp_no"
        "whatsapp_no": (kwargs.get("whatsapp_no") or kwargs.get("whatsapp") or "").strip(),
        "clinic_name": (kwargs.get("clinic_name") or "").strip(),
        # Legacy call sites used "imc_number"
        "imc_registration_number": (kwargs.get("imc_registration_number") or kwargs.get("imc_number") or "").strip(),
        "clinic_phone": (kwargs.get("clinic_phone") or "").strip(),
        "clinic_appointment_number": (kwargs.get("clinic_appointment_number") or "").strip(),
        "clinic_address": (kwargs.get("clinic_address") or "").strip(),
        "postal_code": (kwargs.get("postal_code") or "").strip(),
        "state": (kwargs.get("state") or "").strip(),
        "district": (kwargs.get("district") or "").strip(),
        "receptionist_whatsapp_number": rec_wa,
        "photo_path": (kwargs.get("photo_path") or "").strip(),
        "campaign_id": (kwargs.get("campaign_id") or "").strip(),
        "registered_by": (kwargs.get("registered_by") or "").strip(),
        "recruited_via": (kwargs.get("recruited_via") or "").strip(),
        "initial_password_raw": kwargs.get("initial_password_raw"),
    }

    return _create_doctor_with_enrollment_impl(**mapped)


# Alias to preserve the name used across the project.
create_doctor_with_enrollment = create_doctor_with_enrollment_compat
