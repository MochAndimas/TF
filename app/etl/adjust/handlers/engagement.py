"""Click/impression normalization with conservative delivery deduplication."""
from sqlalchemy.dialects.sqlite import insert
from app.db.models.adjust import AdjustEngagement
from app.etl.adjust.common import (
    PROFILE_FIELDS, normalize_context, optional_text, payload_hash,
    reporting_date, required_text, unix_timestamp,
)

ENGAGEMENT_TIMESTAMPS = {"click": "click_time", "impression": "impression_time"}


def normalize_engagement(payload: dict, raw_id: int) -> dict:
    kind = required_text(payload, "activity_kind").lower()
    if kind not in ENGAGEMENT_TIMESTAMPS:
        raise ValueError("Unsupported engagement activity")
    environment = required_text(payload, "environment").lower()
    if environment not in {"sandbox", "production"}:
        raise ValueError("Invalid environment")
    created = unix_timestamp(payload, "created_at", required=True)
    timestamps = {field: unix_timestamp(payload, field) for field in ENGAGEMENT_TIMESTAMPS.values()}
    # Placeholder data may be unavailable; created_at still describes the activity.
    occurred = timestamps[ENGAGEMENT_TIMESTAMPS[kind]] or created
    row = dict(engagement_key=payload_hash(payload), app_token=required_text(payload, "app_token"),
               environment=environment, adid=optional_text(payload.get("adid")), activity_kind=kind,
               occurred_at=occurred, created_at=created, date=reporting_date(occurred), raw_id=raw_id,
               **timestamps)
    row.update(normalize_context(payload))
    row.update({field: optional_text(payload.get(field)) for field in PROFILE_FIELDS})
    return row


async def load_engagement(session, fact):
    statement = insert(AdjustEngagement).values(**fact)
    # Same raw payload remains one fact on retry/replay. Distinct payloads are retained:
    # device + second is not a reliable unique ID for clicks or impressions.
    immutable = {"engagement_key", "raw_id"}
    updates = {column.name: getattr(statement.excluded, column.name)
               for column in AdjustEngagement.__table__.columns if column.name not in immutable}
    await session.execute(statement.on_conflict_do_update(index_elements=["engagement_key"], set_=updates))
