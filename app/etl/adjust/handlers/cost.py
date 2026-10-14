"""Ad spend callback facts, retaining individual updates and exact decimals."""
import re
from decimal import Decimal, InvalidOperation
from sqlalchemy.dialects.sqlite import insert
from app.db.models.adjust import AdjustCost
from app.etl.adjust.common import PROFILE_FIELDS, normalize_context, optional_text, payload_hash, reporting_date, required_text, unix_timestamp

# Adjust's help pages document both spellings. Keep one normalized activity.
COST_KINDS = ("cost_update", "cost_updated")


def cost_decimal(payload, key):
    value = optional_text(payload.get(key))
    if value is None:
        return None
    try:
        number = Decimal(value)
        if not number.is_finite() or number < 0 or len(value) > 128:
            raise ValueError()
        return str(number)
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{key} must be a nonnegative finite decimal") from error


def normalize_cost(payload, raw_id):
    if required_text(payload, "activity_kind").lower() not in COST_KINDS:
        raise ValueError("Unsupported cost activity")
    environment = required_text(payload, "environment").lower()
    if environment not in {"sandbox", "production"}:
        raise ValueError("Invalid environment")
    created = unix_timestamp(payload, "created_at", required=True)
    currency = optional_text(payload.get("cost_currency"))
    if currency:
        currency = currency.upper()
        if not re.fullmatch(r"[A-Z]{3}", currency):
            raise ValueError("cost_currency must be a three-letter code")
    amount = cost_decimal(payload, "cost_amount")
    if amount is not None and currency is None:
        raise ValueError("cost_currency is required when cost_amount is present")
    row = dict(cost_key=payload_hash(payload), app_token=required_text(payload, "app_token"),
               environment=environment, activity_kind="cost_update", adid=optional_text(payload.get("adid")),
               occurred_at=created, created_at=created, date=reporting_date(created), raw_id=raw_id,
               cost_amount=amount, cost_currency=currency, reporting_cost=cost_decimal(payload, "reporting_cost"),
               cost_type=optional_text(payload.get("cost_type")), cost_id_md5=optional_text(payload.get("cost_id_md5")))
    row.update(normalize_context(payload))
    row.update({field: optional_text(payload.get(field)) for field in PROFILE_FIELDS})
    row["custom_parameters"] = {key: value for key, value in payload.items() if key not in set(row) | {"network_name"}}
    return row


async def load_cost(session, fact):
    statement = insert(AdjustCost).values(**fact)
    # Retain changed payloads even with the same cost ID: these may be corrections.
    updates = {column.name: getattr(statement.excluded, column.name)
               for column in AdjustCost.__table__.columns if column.name not in {"cost_key", "raw_id"}}
    await session.execute(statement.on_conflict_do_update(index_elements=["cost_key"], set_=updates))
