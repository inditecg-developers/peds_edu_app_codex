from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Sequence
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
            f"MASTER DB alias '{alias}' is not configured in settings.DATABASES. "
            "Set MASTER_DB_* env vars / Secrets Manager mapping and restart."
        )
    return connections[alias]


def qn(name: str) -> str:
    """Quote identifiers safely per backend."""
    conn = get_master_connection()
    return conn.ops.quote_name(name)


def normalize_wa_for_lookup(raw: str) -> str:
    """
    Normalize WhatsApp number for DB lookup:
    - digits only
    - prefer 10-digit form if input includes '91' prefix
    """
    s = re.sub(r"\D", "", str(raw or ""))
    if len(s) == 12 and s.startswith("91"):
        return s[2:]
    return s


def wa_link_number(raw: str, default_country_code: str = "91") -> str:
    """Digits only; for wa.me use 91XXXXXXXXXX (no '+')."""
    s = re.sub(r"\D", "", str(raw or ""))
    if len(s) == 10:
        return f"{default_country_code}{s}"
    if s.startswith("0") and len(s) == 11:
        return f"{default_country_code}{s[1:]}"
    return s


def build_whatsapp_deeplink(raw_phone: str, message: str) -> str:
    phone = wa_link_number(raw_phone)
    return f"https://wa.me/{phone}?text={quote(message or '')}"


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
        candidates.append(f"91{wa}")  # some DBs may store with country code

    placeholders = " OR ".join([f"{qn(wa_col)} = %s"] * len(candidates))

    sql = (
        f"SELECT {qn(id_col)}, {qn('first_name')}, {qn('last_name')}, {qn('email')}, {qn(wa_col)} "
        f"FROM {qn(table)} "
        f"WHERE {placeholders} "
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

    cols: Sequence[str] = (
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


def insert_enrollment_row(*, doctor_id: str, campaign_id: str, registered_by: str) -> None:
    conn = get_master_connection()
    table = getattr(settings, "MASTER_DB_ENROLLMENT_TABLE", "DoctorCampaignEnrollment")

    doctor_col = getattr(settings, "MASTER_DB_ENROLLMENT_DOCTOR_COLUMN", "doctor_id")
    campaign_col = getattr(settings, "MASTER_DB_ENROLLMENT_CAMPAIGN_COLUMN", "campaign_id")
    registered_by_col = getattr(settings, "MASTER_DB_ENROLLMENT_REGISTERED_BY_COLUMN", "registered_by_id")

    cols = [doctor_col, campaign_col, registered_by_col]
    vals = [doctor_id, campaign_id, registered_by]

    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {qn(table)} ({', '.join(qn(c) for c in cols)}) VALUES ({placeholders})"

    with conn.cursor() as cur:
        cur.execute(sql, vals)


def ensure_enrollment(*, doctor_id: str, campaign_id: str, registered_by: str) -> None:
    """Insert enrollment; ignore if already enrolled (unique constraint)."""
    if not (doctor_id and campaign_id):
        return
    try:
        insert_enrollment_row(doctor_id=doctor_id, campaign_id=campaign_id, registered_by=registered_by or "")
    except IntegrityError:
        return
