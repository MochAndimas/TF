"""Registration-cohort revenue metrics scoped to one social platform."""

from datetime import date, timedelta
from typing import Literal

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import DataSocmed
from app.utils.period_comparison import previous_period_range, growth_percentage


RevenueComparison = Literal["previous_period", "month_to_date", "previous_month"]


def revenue_previous_range(start_date: date, end_date: date, comparison: RevenueComparison):
    return previous_period_range(start_date, end_date, comparison)


async def fetch_socmed_revenue(
    session: AsyncSession, *, source: str, start_date: date, end_date: date,
    comparison: RevenueComparison = "previous_period",
) -> dict:
    if source not in {"instagram", "youtube", "tiktok", "facebook"}:
        raise ValueError("Unsupported social platform.")
    if start_date > end_date:
        raise ValueError("Start date cannot be after end date.")

    async def summarize(start, end):
        register, qty, amount = (await session.execute(select(
            func.count(func.distinct(DataSocmed.id)),
            func.count(func.distinct(case((DataSocmed.first_depo > 0, DataSocmed.id)))),
            func.coalesce(func.sum(DataSocmed.first_depo), 0),
        ).where(
            func.lower(func.trim(DataSocmed.utm_source)) == source,
            DataSocmed.tgl_regis.between(start, end),
        ))).one()
        return {
            "register": int(register),
            "first_deposit_qty": int(qty),
            "first_deposit": float(amount),
            "register_to_deposit": qty / register * 100 if register else 0.0,
        }

    async def daily_rows(start, end):
        result = await session.execute(
            select(
                DataSocmed.tgl_regis.label("date"),
                func.count(func.distinct(DataSocmed.id)).label("register"),
                func.count(
                    func.distinct(case((DataSocmed.first_depo > 0, DataSocmed.id)))
                ).label("first_deposit_qty"),
            )
            .where(
                func.lower(func.trim(DataSocmed.utm_source)) == source,
                DataSocmed.tgl_regis.between(start, end),
            )
            .group_by(DataSocmed.tgl_regis)
            .order_by(DataSocmed.tgl_regis)
        )
        rows = []
        for row in result:
            register = int(row.register)
            first_deposit_qty = int(row.first_deposit_qty)
            rows.append({
                "date": row.date.isoformat(),
                "register": register,
                "first_deposit_qty": first_deposit_qty,
                "register_to_deposit": (
                    first_deposit_qty / register * 100 if register else 0.0
                ),
            })
        return rows

    previous_start, previous_end = previous_period_range(start_date, end_date, comparison if comparison != "previous_period" else None)
    current = await summarize(start_date, end_date)
    previous = await summarize(previous_start, previous_end)
    growth = {key: growth_percentage(value, previous[key]) for key, value in current.items()}
    return {
        "current_period": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "metrics": current},
        "previous_period": {"start_date": previous_start.isoformat(), "end_date": previous_end.isoformat(), "metrics": previous},
        "growth_percentage": growth,
        "daily_rows": await daily_rows(start_date, end_date),
    }
