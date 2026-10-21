"""Read OIS daily facts and the preceding equal-length comparison period."""
from datetime import date

from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.ga4 import Ga4UserAcquisition
from app.etl.ga4.config import ALLOWED_SOURCES
from app.utils.period_comparison import previous_period_range


async def fetch_ois_payload(session: AsyncSession, start_date: date, end_date: date) -> dict:
    if start_date > end_date:
        raise ValueError("Start date cannot be after end date.")
    days = (end_date - start_date).days + 1
    if days > 366:
        raise ValueError("Select a range of at most 366 days.")
    previous_start, previous_end = previous_period_range(start_date, end_date)
    query = select(Ga4UserAcquisition).where(
        or_(Ga4UserAcquisition.date.between(previous_start, previous_end),
            Ga4UserAcquisition.date.between(start_date, end_date)),
        Ga4UserAcquisition.first_user_source.in_(ALLOWED_SOURCES),
    ).order_by(Ga4UserAcquisition.date, Ga4UserAcquisition.first_user_source)
    rows = (await session.execute(query)).scalars().all()
    columns = [column.name for column in Ga4UserAcquisition.__table__.columns if column.name != "id"]

    def serialize(row):
        return {name: value.isoformat() if isinstance(value := getattr(row, name), date) else value
                for name in columns}
    return {
        "current_rows": [serialize(row) for row in rows if row.date >= start_date],
        "previous_rows": [serialize(row) for row in rows if row.date < start_date],
        "previous_start": previous_start.isoformat(),
        "previous_end": previous_end.isoformat(),
    }


async def fetch_ois_registrations(session: AsyncSession, start_date: date, end_date: date) -> dict:
    """Registration cohorts with subscription snapshots; never add first and last revenue."""
    from app.db.models.ois import OisRegistration
    if start_date > end_date or (end_date - start_date).days >= 366:
        raise ValueError("Select a valid range of at most 366 days.")
    previous_start, previous_end = previous_period_range(start_date, end_date)
    fields = ("tf_id", "registration_date", "tgl_regis", "utm_source", "utm_medium", "utm_campaign",
              "user_status", "assign_date", "first_assign_date", "first_subs", "first_subs_idr",
              "last_subs", "last_subs_nominal_idr", "cabang", "status", "pool", "last_activity", "pull_date",
              "email", "phone", "username")
    result = await session.execute(select(*(getattr(OisRegistration, field) for field in fields)).where(
        or_(OisRegistration.registration_date.between(start_date, end_date),
            OisRegistration.registration_date.between(previous_start, previous_end)),
        OisRegistration.utm_source.in_(ALLOWED_SOURCES),
    ).order_by(OisRegistration.registration_date, OisRegistration.tf_id))
    current, previous, contacts = [], [], []
    for record in result.mappings():
        serialized = {field: value.isoformat() if isinstance(value, date) else value
                      for field, value in record.items()}
        if record["registration_date"] >= start_date:
            contacts.append({field: serialized[field] for field in (
                "tf_id", "email", "phone", "username", "tgl_regis", "utm_source", "utm_medium", "utm_campaign")})
        for field in ("email", "phone", "username"):
            serialized.pop(field)
        (current if record["registration_date"] >= start_date else previous).append(serialized)
    return {"current_rows": current, "previous_rows": previous, "contact_rows": contacts,
            "previous_start": previous_start.isoformat(), "previous_end": previous_end.isoformat()}
