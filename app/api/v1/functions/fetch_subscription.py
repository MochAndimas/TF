"""Read daily subscription analytics without treating daily subscriber counts as unique users."""
from datetime import date, timedelta
from app.utils.period_comparison import previous_period_range, growth_percentage
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import validate_date_range
from app.db.models.external_api import AllSubscription, FirstSubs

FLOW_FIELDS = (
    "total_subscription_amount", "new_subscription_amount",
    "total_subscription_qty", "new_subscription_qty",
)
DAILY_FIELDS = (*FLOW_FIELDS, "unique_subscribers")


def summarize(rows: list[dict]) -> dict:
    totals = {
        field: float(sum((Decimal(str(row[field])) for row in rows), Decimal(0)))
        if field.endswith("amount") else sum(row[field] for row in rows)
        for field in FLOW_FIELDS
    }
    latest = rows[-1] if rows else {}
    totals.update(
        unique_subscribers=latest.get("unique_subscribers"),
        subscriber_date=latest.get("date"),
    )
    return totals


FIRST_FIELDS = tuple(c.name for c in FirstSubs.__table__.columns if c.name not in {"date", "pull_date"})


def summarize_first(rows):
    return {field: (float(sum((Decimal(str(row[field])) for row in rows), Decimal(0)))
            if field.endswith("amount") else sum(row[field] for row in rows)) if rows else None
            for field in FIRST_FIELDS}


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
            if current and previous and current_metrics[field] is not None and baseline is not None else None
        )
    result = await session.execute(select(FirstSubs).where(FirstSubs.date.between(previous_start, end_date)).order_by(FirstSubs.date))
    first_current, first_previous = [], []
    for record in result.scalars():
        row = {field: getattr(record, field) for field in FIRST_FIELDS}
        row.update(date=record.date.isoformat(), pull_date=record.pull_date.isoformat())
        if start_date <= record.date <= end_date:
            first_current.append(row)
        elif previous_start <= record.date <= previous_end:
            first_previous.append(row)
    first_metrics, first_baseline = summarize_first(first_current), summarize_first(first_previous)
    first_growth = {field: growth_percentage(first_metrics[field], first_baseline[field])
                    if first_current and first_previous else None for field in FIRST_FIELDS}
    return {
        "start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
        "previous_start_date": previous_start.isoformat(), "previous_end_date": previous_end.isoformat(),
        "metrics": current_metrics, "previous_metrics": previous_metrics, "growth_percentage": growth,
        "daily_rows": current, "days_with_data": len(current), "expected_days": days,
        "previous_days_with_data": len(previous),
        "first_daily_rows": first_current,
        "first_metrics": first_metrics, "first_previous_metrics": first_baseline,
        "first_growth_percentage": first_growth,
        "first_days_with_data": len(first_current), "first_previous_days_with_data": len(first_previous),
        "last_updated": max((row["pull_date"] for row in [*current, *first_current]), default=None),
    }
