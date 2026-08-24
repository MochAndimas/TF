"""Analytics payload builder for All Regis source totals."""

from __future__ import annotations

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import RegisUtmDaily


async def fetch_regis_utm_daily_payload(
    session: AsyncSession, *, start_date: date, end_date: date
) -> dict[str, object]:
    """Return All Regis metrics, daily totals, and source breakdown."""
    rows = (
        await session.execute(
            select(
                RegisUtmDaily.date,
                RegisUtmDaily.source,
                func.sum(RegisUtmDaily.value).label("value"),
            )
            .where(RegisUtmDaily.date.between(start_date, end_date))
            .group_by(RegisUtmDaily.date, RegisUtmDaily.source)
            .order_by(RegisUtmDaily.date, RegisUtmDaily.source)
        )
    ).all()
    records = [{"date": row.date.isoformat(), "source": row.source, "value": int(row.value or 0)} for row in rows]
    daily: dict[str, int] = {}
    sources: dict[str, int] = {}
    for row in records:
        daily[row["date"]] = daily.get(row["date"], 0) + row["value"]
        sources[row["source"]] = sources.get(row["source"], 0) + row["value"]
    total = sum(daily.values())
    days = (end_date - start_date).days + 1
    peak_date, peak_value = max(daily.items(), key=lambda item: item[1], default=(None, 0))
    return {
        "metrics": {
            "total_register": total,
            "avg_daily_register": round(total / days, 2) if days else 0,
            "active_sources": len(sources),
            "peak_day": peak_date,
            "peak_day_register": peak_value,
        },
        "daily_rows": [{"date": key, "value": value} for key, value in sorted(daily.items())],
        "source_rows": [{"source": key, "value": value} for key, value in sorted(sources.items(), key=lambda item: item[1], reverse=True)],
        "details": records,
    }
