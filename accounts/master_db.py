from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

from django.conf import settings
from django.db import connections, IntegrityError


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
    conn = get_master_connection()
    table = getattr(settings, "MASTER_DB_DOCTOR_TABLE", "Doctor")
    id_col = getattr(settings, "MASTER_DB_DOCTOR_ID_COLUMN", "doctor_id")

    sql = f"SELECT 1 FROM {qn(table)} WHERE {qn(id_col)} = %s LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, [doctor_id])
        return cur.fetchone() is not None


def get_doctor_by_whatsapp(whatsapp_no: str) -> Optional[MasterDoctor]:
    conn = get_master_connection()
    table = getattr(settings, "MASTER_DB_DOCTOR_TABLE", "Doctor")
    id_col = getattr(settings, "MASTER_DB_DOCTOR_ID_COLUMN", "doctor_id")
    wa_col = getattr(settings, "MASTER_DB_DOCTOR_WHATSAPP_COLUMN", "whatsapp_no")

    wa = normalize_wa_for_lookup(whatsapp_no)
    if not wa:
        return None

    candidates = [wa]
    if len(wa) == 10:
        candidates.append(f"91{wa}")

    placeholders = " OR ".join([f"{qn(wa_col)} = %s"] * len(candidates))
    sql = (
        f"SELECT {qn(id_col)}, {qn('first_name')}, {qn('last_name')}, {qn('email')}, {qn(wa_col)} "
        f"FROM {qn(table)} WHERE {placeholders} LIMIT 1"
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


def count_campaign_enrollments(campaign_id: str) -> int:
    conn = get_master_connection()
    table = getattr(settings, "MASTER_DB_ENROLLMENT_TABLE", "DoctorCampaignEnrollment")
    campaign_col = getattr(settings, "MASTER_DB_ENROLLMENT_CAMPAIGN_COLUMN", "campaign_id")

    cid = str(campaign_id or "").strip()
    if not cid:
        return 0

    sql = f"SELECT COUNT(*) FROM {qn(table)} WHERE {qn(campaign_col)} = %s"
    with conn.cursor() as cur:
        cur.execute(sql, [cid])
        row = cur.fetchone()

    try:
        return int(row[0])
    except Exception:
        return 0


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
        email,
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


def ensure_enrollment(*, doctor_id: str, campaign_id: str, registered_by: str) -> None:
    if not (doctor_id and campaign_id):
        return

    conn = get_master_connection()
    table = getattr(settings, "MASTER_DB_ENROLLMENT_TABLE", "DoctorCampaignEnrollment")

    doctor_col = getattr(settings, "MASTER_DB_ENROLLMENT_DOCTOR_COLUMN", "doctor_id")
    campaign_col = getattr(settings, "MASTER_DB_ENROLLMENT_CAMPAIGN_COLUMN", "campaign_id")
    registered_by_col = getattr(settings, "MASTER_DB_ENROLLMENT_REGISTERED_BY_COLUMN", "registered_by_id")

    sql = (
        f"INSERT INTO {qn(table)} ({qn(doctor_col)}, {qn(campaign_col)}, {qn(registered_by_col)}) "
        f"VALUES (%s, %s, %s)"
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [doctor_id, campaign_id, registered_by or ""])
    except IntegrityError:
        return


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
