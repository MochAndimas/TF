"""Replayable attribution history; never mutate original install attribution."""
from sqlalchemy import func
from sqlalchemy.dialects.sqlite import insert
from app.db.models.adjust import AdjustAttributionHistory
from app.etl.adjust.common import (
    PROFILE_FIELDS, normalize_context, normalize_identity, optional_integer,
    optional_text, payload_hash, reporting_date, required_text, unix_timestamp,
)

ATTRIBUTION_TIMESTAMPS = {
    "reattribution": "reattributed_at",
    "attribution_update": "attribution_updated_at",
    "updated_attribution": "attribution_updated_at",
    "reattribution_reinstall": "reattributed_at",
}


def normalize_attribution(payload: dict, raw_id: int) -> dict:
    kind = required_text(payload, "activity_kind").lower()
    if kind not in ATTRIBUTION_TIMESTAMPS:
        raise ValueError("Unsupported attribution activity")
    canonical_kind = "attribution_update" if kind == "updated_attribution" else kind
    identity = normalize_identity(payload)
    created = unix_timestamp(payload, "created_at", required=True)
    occurred = unix_timestamp(payload, ATTRIBUTION_TIMESTAMPS[kind], required=True)
    row = dict(identity, activity_kind=canonical_kind, occurred_at=occurred,
               created_at=created, date=reporting_date(occurred), raw_id=raw_id)
    for field in ("reattributed_at", "attribution_updated_at", "uninstalled_at", "reinstalled_at"):
        row[field] = unix_timestamp(payload, field, required=(kind == "reattribution_reinstall" and field == "reinstalled_at"))
    row["time_to_reinstall"] = optional_integer(payload, "time_to_reinstall")
    row.update(normalize_context(payload))
    row.update({field: optional_text(payload.get(field)) for field in PROFILE_FIELDS})
    for field in ("first_tracker_token", "last_tracker_token", "outdated_tracker_token"):
        row[field] = optional_text(payload.get(field))
    # Reuse Adjust display-string decoding for the previous tracker labels.
    for field in ("last_tracker_name", "outdated_tracker_name"):
        row[field] = normalize_context({"tracker_name": payload.get(field)})["tracker_name"]
    key_data = dict(identity, activity_kind=canonical_kind, occurred_at=occurred.isoformat())
    if canonical_kind == "attribution_update":
        # Multiple corrections can share a second: preserve distinct tracker transitions.
        key_data.update(tracker_token=row["tracker_token"], outdated_tracker_token=row["outdated_tracker_token"])
    if kind == "reattribution_reinstall":
        key_data["reinstalled_at"] = row["reinstalled_at"].isoformat()
    row["attribution_key"] = payload_hash(key_data)
    return row


async def load_attribution(session, fact):
    statement = insert(AdjustAttributionHistory).values(**fact)
    immutable = {"attribution_key", "app_token", "environment", "adid", "activity_kind",
                 "occurred_at", "created_at", "date", "raw_id"}
    updates = {
        column.name: func.coalesce(getattr(AdjustAttributionHistory, column.name), getattr(statement.excluded, column.name))
        for column in AdjustAttributionHistory.__table__.columns if column.name not in immutable
    }
    await session.execute(statement.on_conflict_do_update(index_elements=["attribution_key"], set_=updates))
