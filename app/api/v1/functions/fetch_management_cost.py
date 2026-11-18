"""Daily cost and revenue aggregates for Overview Management."""

from calendar import monthrange
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, localcontext

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import FacebookAds, GoogleAds, TikTokAds, ToolsFixedCost
from app.db.models.gpt_ads import GptAdsDaily
from app.utils.period_comparison import growth_percentage


async def fetch_management_cost(
    session: AsyncSession,
    current: tuple[date, date],
    previous: tuple[date, date],
    deposit_rows,
    subscription_rows,
) -> dict:
    start, end = previous[0], current[1]
    ads = defaultdict(Decimal)
    for model, cost in (
        (GoogleAds, GoogleAds.cost),
        (FacebookAds, FacebookAds.cost),
        (TikTokAds, TikTokAds.cost),
        (GptAdsDaily, GptAdsDaily.spend),
    ):
        query = select(model.date, func.sum(cost)).where(model.date.between(start, end))
        if model is GptAdsDaily:
            query = query.where(model.report_level == "campaign", model.currency == "IDR")
        for day, amount in await session.execute(query.group_by(model.date)):
            if amount is not None:
                ads[day] += Decimal(str(amount))

    tools = defaultdict(Decimal)
    for month, amount in await session.execute(
        select(ToolsFixedCost.date, ToolsFixedCost.amount_idr)
        .where(ToolsFixedCost.date.between(start.replace(day=1), end.replace(day=1)))
    ):
        tools[month] += Decimal(str(amount))

    revenue = defaultdict(Decimal)
    for row in (*deposit_rows, *subscription_rows):
        revenue[row["date"]] += Decimal(str(row["amount"]))

    def summarize(window):
        first, last = window
        rows = []
        cost_total = revenue_total = Decimal(0)
        missing_tools_months = set()
        for offset in range((last - first).days + 1):
            day = first + timedelta(days=offset)
            month = day.replace(day=1)
            if month not in tools:
                missing_tools_months.add(month.isoformat())
            amount = revenue[day]
            with localcontext() as context:
                context.prec = 34
                cost = ads[day] + tools.get(month, Decimal(0)) / monthrange(day.year, day.month)[1]
                cost_total += cost
                revenue_total += amount
            rows.append({"date": day.isoformat(), "cost": float(cost), "revenue_idr": float(amount)})
        return {
            "cost_to_revenue_cost": float(cost_total),
            "revenue": float(revenue_total),
            "cost_to_revenue_pct": float(cost_total / revenue_total * 100) if revenue_total else None,
        }, rows, sorted(missing_tools_months)

    metrics, rows, missing = summarize(current)
    baseline, _, previous_missing = summarize(previous)
    return {
        "current_period": {"metrics": metrics},
        "previous_period": {"start_date": previous[0].isoformat(), "end_date": previous[1].isoformat()},
        "growth_percentage": {
            key: growth_percentage(value, baseline[key])
            if value is not None and baseline[key] is not None else None
            for key, value in metrics.items()
        },
        "daily_rows": rows,
        "missing_tools_months": sorted(set(missing + previous_missing)),
    }
