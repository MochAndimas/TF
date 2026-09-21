"""Read daily subscription analytics without treating daily subscriber counts as unique users."""
from datetime import date, timedelta
from app.utils.period_comparison import previous_period_range, growth_percentage
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import validate_date_range
from app.db.models.external_api import AllSubscription

FLOW_FIELDS = (
    "total_subscription_amount", "new_subscription_amount",
    "total_subscription_qty", "new_subscription_qty",
)
DAILY_FIELDS = (*FLOW_FIELDS, "total_subscribers", "new_subscribers")


def summarize(rows: list[dict]) -> dict:
    totals = {
        field: float(sum((Decimal(str(row[field])) for row in rows), Decimal(0)))
        if field.endswith("amount") else sum(row[field] for row in rows)
        for field in FLOW_FIELDS
    }
    latest = rows[-1] if rows else {}
    totals.update(
        total_subscribers=latest.get("total_subscribers"),
        new_subscribers=latest.get("new_subscribers"),
        subscriber_date=latest.get("date"),
    )
    return totals


async def fetch_subscription_payload(session: AsyncSession, start_date: date, end_date: date) -> dict:
    validate_date_range(start_date, end_date)
    days = (end_date - start_date).days + 1
    previous_start, previous_end = previous_period_range(start_date, end_date)
    result = await session.execute(
        select(AllSubscription).where(
            AllSubscription.date.between(previous_start, end_date)
        ).order_by(AllSubscription.date)
    )
    current, previous = [], []
    for record in result.scalars():
        row = {field: getattr(record, field) for field in DAILY_FIELDS}
        row.update(date=record.date.isoformat(), pull_date=record.pull_date.isoformat())
        if record.date >= start_date:
            current.append(row)
        elif record.date <= previous_end:
            previous.append(row)
    current_metrics, previous_metrics = summarize(current), summarize(previous)
    growth = {}
    for field in DAILY_FIELDS:
        baseline = previous_metrics[field]
        growth[field] = (
            growth_percentage(current_metrics[field], baseline)
            if current and previous else None
        )
    return {
        "start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
        "previous_start_date": previous_start.isoformat(), "previous_end_date": previous_end.isoformat(),
        "metrics": current_metrics, "previous_metrics": previous_metrics, "growth_percentage": growth,
        "daily_rows": current, "days_with_data": len(current), "expected_days": days,
        "previous_days_with_data": len(previous),
        "last_updated": max((row["pull_date"] for row in current), default=None),
    }
