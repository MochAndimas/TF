"""Aggregate daily ALL DEPO records without inferring user-level cohorts."""
from datetime import date
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import validate_date_range
from app.db.models.external_api import AllDepo, FirstDepo
from app.utils.period_comparison import previous_period_range, growth_percentage

ALL_FIELDS = tuple(c.name for c in AllDepo.__table__.columns if c.name not in {'date', 'pull_date'})
FIRST_FIELDS = tuple(c.name for c in FirstDepo.__table__.columns if c.name not in {'date', 'pull_date'})
FIELDS = (*ALL_FIELDS, *FIRST_FIELDS)


def summarize(rows: list[dict]) -> dict:
    totals = {}
    for field in FIELDS:
        values = [row[field] for row in rows]
        totals[field] = (None if any(v is None for v in values) else
            float(sum((Decimal(str(v)) for v in values), Decimal(0)))
            if field.endswith('_amount') else sum(values))
    for measure in ('amount', 'qty'):
        total, first = totals[f'total_deposit_{measure}'], totals[f'first_deposit_{measure}']
        totals[f'top_up_{measure}'] = total - first if total is not None and first is not None else None
    for prefix, name in [('total_deposit', 'average_deposit'), ('first_deposit', 'average_first_deposit'), ('top_up', 'average_top_up')]:
        amount, qty = totals[f'{prefix}_amount'], totals[f'{prefix}_qty']
        totals[name] = amount / qty if amount is not None and qty else None
    return totals


async def fetch_deposit_revenue_payload(session: AsyncSession, start_date: date, end_date: date) -> dict:
    validate_date_range(start_date, end_date)
    previous_start, previous_end = previous_period_range(start_date, end_date)
    by_date = {}
    for model, fields in ((AllDepo, ALL_FIELDS), (FirstDepo, FIRST_FIELDS)):
        result = await session.execute(select(model).where(model.date.between(previous_start, end_date)).order_by(model.date))
        for record in result.scalars():
            row = by_date.setdefault(record.date, {**dict.fromkeys(FIELDS), 'date': record.date.isoformat(), 'pull_date': record.pull_date.isoformat()})
            row.update({field: getattr(record, field) for field in fields})
            row['pull_date'] = max(row['pull_date'], record.pull_date.isoformat())
    current, previous = [], []
    for day, row in sorted(by_date.items()):
        if start_date <= day <= end_date:
            current.append(row)
        elif previous_start <= day <= previous_end:
            previous.append(row)
    totals, baseline = summarize(current), summarize(previous)
    growth = {key: growth_percentage(value, baseline[key])
              if current and previous and value is not None and baseline[key] is not None else None
              for key, value in totals.items()}
    return {
        'current_period': {'start_date': start_date.isoformat(), 'end_date': end_date.isoformat(), 'metrics': totals},
        'previous_period': {'start_date': previous_start.isoformat(), 'end_date': previous_end.isoformat(), 'metrics': baseline},
        'growth_percentage': growth, 'daily_rows': current,
        'days_with_data': len(current), 'expected_days': (end_date - start_date).days + 1,
        'previous_days_with_data': len(previous),
        'last_updated': max((row['pull_date'] for row in current), default=None),
    }
