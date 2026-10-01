"""Aggregate daily ALL DEPO records without inferring user-level cohorts."""
from datetime import date
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import validate_date_range
from app.db.models.external_api import AllDepo
from app.utils.period_comparison import previous_period_range, growth_percentage

FIELDS = tuple(column.name for column in AllDepo.__table__.columns if column.name not in {'date', 'pull_date'})


def summarize(rows: list[dict]) -> dict:
    totals = {field: float(sum((Decimal(str(row[field])) for row in rows), Decimal(0)))
              if field.endswith('_amount') else sum(row[field] for row in rows) for field in FIELDS}
    totals['top_up_amount'] = totals['total_deposit_amount'] - totals['first_deposit_amount']
    totals['top_up_qty'] = totals['total_deposit_qty'] - totals['first_deposit_qty']
    totals['average_deposit'] = (totals['total_deposit_amount'] / totals['total_deposit_qty']
                                 if totals['total_deposit_qty'] else None)
    totals['average_first_deposit'] = (totals['first_deposit_amount'] / totals['first_deposit_qty']
                                       if totals['first_deposit_qty'] else None)
    totals['average_top_up'] = (totals['top_up_amount'] / totals['top_up_qty']
                                if totals['top_up_qty'] else None)
    return totals


async def fetch_deposit_revenue_payload(session: AsyncSession, start_date: date, end_date: date) -> dict:
    validate_date_range(start_date, end_date)
    previous_start, previous_end = previous_period_range(start_date, end_date)
    result = await session.execute(select(AllDepo).where(AllDepo.date.between(previous_start, end_date)).order_by(AllDepo.date))
    current, previous = [], []
    for record in result.scalars():
        row = {field: getattr(record, field) for field in FIELDS}
        row.update(date=record.date.isoformat(), pull_date=record.pull_date.isoformat())
        if start_date <= record.date <= end_date:
            current.append(row)
        elif previous_start <= record.date <= previous_end:
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
