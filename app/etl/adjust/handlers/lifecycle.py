"""Normalize and load uninstall/reinstall facts without changing installs."""
from sqlalchemy import func
from sqlalchemy.dialects.sqlite import insert
from app.db.models.adjust import AdjustLifecycle
from app.etl.adjust.common import (
    PROFILE_FIELDS, normalize_context, normalize_identity, optional_integer,
    optional_text, payload_hash, reporting_date, required_text, unix_timestamp,
)

LIFECYCLE_TIMESTAMPS = {
    "uninstall": "uninstalled_at",
    "reinstall": "reinstalled_at",
}


def normalize_lifecycle(payload: dict, raw_id: int) -> dict:
    kind = required_text(payload, "activity_kind").lower()
    if kind not in LIFECYCLE_TIMESTAMPS:
        raise ValueError("Unsupported lifecycle activity")
    identity = normalize_identity(payload)
    created = unix_timestamp(payload, "created_at", required=True)
    occurred = unix_timestamp(payload, LIFECYCLE_TIMESTAMPS[kind], required=True)
    key = payload_hash(dict(identity, activity_kind=kind, occurred_at=occurred.isoformat()))
    row = dict(identity, lifecycle_key=key, activity_kind=kind,
               occurred_at=occurred, created_at=created, date=reporting_date(occurred), raw_id=raw_id)
    for field in ("uninstalled_at", "reinstalled_at"):
        row[field] = unix_timestamp(payload, field)
    for field in ("time_to_uninstall", "time_to_reinstall"):
        row[field] = optional_integer(payload, field)
    row.update({field: optional_text(value) for field, value in normalize_context(payload).items()})
    row.update({field: optional_text(payload.get(field)) for field in PROFILE_FIELDS})
    return row


async def load_lifecycle(session, fact):
    statement = insert(AdjustLifecycle).values(**fact)
    # An occurrence retains its original context; duplicate callbacks may fill NULLs.
    immutable = {"lifecycle_key", "app_token", "environment", "adid", "activity_kind",
                 "occurred_at", "created_at", "date", "raw_id"}
    updates = {
        column.name: func.coalesce(getattr(AdjustLifecycle, column.name), getattr(statement.excluded, column.name))
        for column in AdjustLifecycle.__table__.columns if column.name not in immutable
    }
    await session.execute(statement.on_conflict_do_update(index_elements=["lifecycle_key"], set_=updates))
