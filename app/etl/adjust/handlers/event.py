"""Business event normalization and retry-safe transaction loading."""
import re
from decimal import Decimal, InvalidOperation
from sqlalchemy import case, func, select
from sqlalchemy.dialects.sqlite import insert
from app.db.models.adjust import AdjustRegister, AdjustDeposit, AdjustFirstDeposit, AdjustSubscribeSuccess
from app.etl.adjust.common import (
    PROFILE_FIELDS, normalize_context, normalize_identity, optional_text,
    payload_hash, reporting_date, required_text, unix_timestamp,
)

BUSINESS_EVENTS = {
    "Register Completed": "register",
    "Deposit Completed": "deposit",
    "First Deposit Completed": "first_deposit",
    "Subscribe Success": "subscribe_success",
}


EVENT_MODELS = {"register": AdjustRegister, "deposit": AdjustDeposit, "first_deposit": AdjustFirstDeposit, "subscribe_success": AdjustSubscribeSuccess}
SUBSCRIBE_FIELDS = ('average_sl', 'average_tp', 'channel_id', 'channel_name', 'first_subscription', 'losing_month', 'medal', 'month', 'platform', 'price', 'profit_pips', 'profit_usd', 'score_label', 'score_value', 'signal_last_post', 'signal_post_monthly', 'status', 'subscribers', 'subscription_duration', 'symbol_count', 'tf_id')


def normalize_event(payload: dict, raw_id: int) -> dict:
    if required_text(payload, "activity_kind").lower() != "event":
        raise ValueError("Only event activity is supported")
    identity = normalize_identity(payload)
    token = required_text(payload, "event_token")
    name = normalize_context({"tracker_name": required_text(payload, "event_name")})["tracker_name"]
    if name not in BUSINESS_EVENTS:
        raise ValueError("Unsupported Adjust business event_name")
    occurred = unix_timestamp(payload, "created_at", required=True)
    transaction_id = optional_text(payload.get("transaction_id"))
    amount = optional_text(payload.get("revenue_amount")) or optional_text(payload.get("revenue_float"))
    currency = optional_text(payload.get("currency"))
    if currency is not None:
        currency = currency.upper()
        if not re.fullmatch(r"[A-Z]{3}", currency):
            raise ValueError("currency must be a three-letter code")
    if amount is not None:
        try:
            number = Decimal(amount)
            if not number.is_finite() or number < 0 or len(amount) > 128:
                raise ValueError("Invalid amount")
            amount = str(number)
        except (InvalidOperation, ValueError) as error:
            raise ValueError("revenue_amount must be a nonnegative finite decimal") from error
        if currency is None:
            raise ValueError("currency is required when revenue_amount is present")
    # A transaction is unique within app/environment/event token, not across event types.
    key = payload_hash(dict(app_token=identity["app_token"], environment=identity["environment"],
                            event_token=token, transaction_id=transaction_id)) if transaction_id else payload_hash(payload)
    row = dict(identity, event_key=key, activity_kind="event", event_token=token, event_name=name,
               event_type=BUSINESS_EVENTS[name], transaction_id=transaction_id,
               occurred_at=occurred, created_at=occurred, date=reporting_date(occurred),
               revenue_amount=amount, currency=currency, raw_id=raw_id)
    row.update(normalize_context(payload))
    row.update({field: optional_text(payload.get(field)) for field in PROFILE_FIELDS})
    standard = set(row) | {"network_name", "revenue_float"}
    row["custom_parameters"] = {key: value for key, value in payload.items() if key not in standard}
    extra_fields = set(EVENT_MODELS[row["event_type"]].__table__.columns.keys()) - set(row)
    row.update({field: optional_text(payload.get(field)) for field in extra_fields})
    return row


async def load_event(session, fact):
    model = EVENT_MODELS[fact["event_type"]]
    statement = insert(model).values(**fact)
    immutable = {"event_key", "app_token", "environment", "adid", "activity_kind", "event_token",
                 "event_name", "event_type", "transaction_id", "occurred_at", "created_at", "date", "raw_id"}
    updates = {column.name: func.coalesce(getattr(model, column.name), getattr(statement.excluded, column.name))
               for column in model.__table__.columns if column.name not in immutable}
    # Amount and currency must be enriched together, never independently mixed.
    updates["currency"] = case(
        (model.revenue_amount.is_not(None), model.currency),
        (statement.excluded.revenue_amount.is_not(None), statement.excluded.currency),
        else_=func.coalesce(model.currency, statement.excluded.currency),
    )
    existing_custom = await session.scalar(select(model.custom_parameters).where(model.event_key == fact["event_key"]))
    updates["custom_parameters"] = {**(existing_custom or {}), **fact["custom_parameters"]}
    await session.execute(statement.on_conflict_do_update(index_elements=["event_key"], set_=updates))
