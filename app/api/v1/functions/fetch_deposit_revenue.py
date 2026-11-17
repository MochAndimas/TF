"""Aggregate daily ALL DEPO records without inferring user-level cohorts."""
from collections import defaultdict
from datetime import date
from decimal import Decimal

from sqlalchemy import case, distinct, func, select, union
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import validate_date_range
from app.api.v1.functions.fetch_management_cost import fetch_management_cost
from app.db.models.adjust import AdjustRegister
from app.db.models.external_api import (
    AllDepo,
    AllSubscription,
    DailyRegister,
    DataDepo,
    DataDepoBa,
    DataSocmed,
    FirstDepo,
    FirstSubs,
)
from app.utils.overview.shared import USD_TO_IDR_RATE
from app.utils.period_comparison import previous_period_range, growth_percentage

ALL_FIELDS = tuple(c.name for c in AllDepo.__table__.columns if c.name not in {'date', 'pull_date'})
FIRST_FIELDS = tuple(c.name for c in FirstDepo.__table__.columns if c.name not in {'date', 'pull_date'})
FIELDS = (*ALL_FIELDS, *FIRST_FIELDS)


async def fetch_register_total(session: AsyncSession, start_date: date, end_date: date) -> dict:
    validate_date_range(start_date, end_date)
    previous_start, previous_end = previous_period_range(start_date, end_date)
    adjust_current_range = AdjustRegister.date.between(start_date, end_date)
    adjust_previous_range = AdjustRegister.date.between(previous_start, previous_end)
    adjust_source = func.nullif(func.lower(func.trim(AdjustRegister.source_name)), "")
    adjust_user = func.nullif(func.trim(AdjustRegister.tf_id), "")
    adjust_result = await session.execute(
        select(
            func.count(distinct(case((adjust_current_range, adjust_user)))),
            func.count(distinct(case((adjust_previous_range, adjust_user)))),
        ).where(adjust_current_range | adjust_previous_range, adjust_source != "organic")
    )
    adjust_current, adjust_previous = adjust_result.one()
    payload = {
        "previous_period": {"start_date": previous_start.isoformat(), "end_date": previous_end.isoformat()},
    }
    previous_totals = {}
    for total_key, growth_key, date_column, count_column in (
        ("total_register", "growth_percentage", FirstDepo.date, FirstDepo.register_qty),
        ("ads_register", "ads_growth_percentage", DailyRegister.date, DailyRegister.total_regis),
    ):
        current_range = date_column.between(start_date, end_date)
        previous_range = date_column.between(previous_start, previous_end)
        result = await session.execute(
            select(
                func.sum(case((current_range, count_column))),
                func.sum(case((previous_range, count_column))),
            ).where(current_range | previous_range)
        )
        current, previous = result.one()
        if total_key == "ads_register":
            if adjust_current:
                current = (current or 0) + adjust_current
            if adjust_previous:
                previous = (previous or 0) + adjust_previous
        payload[total_key] = current
        previous_totals[total_key] = previous
        payload[growth_key] = (
            growth_percentage(current, previous)
            if current is not None and previous is not None else None
        )

    current_range = DataSocmed.tgl_regis.between(start_date, end_date)
    previous_range = DataSocmed.tgl_regis.between(previous_start, previous_end)
    has_deposit = DataSocmed.first_depo > 0
    deposit_amount = func.coalesce(DataSocmed.first_depo, 0)
    result = await session.execute(
        select(
            func.count(distinct(case((current_range, DataSocmed.id)))),
            func.count(distinct(case((previous_range, DataSocmed.id)))),
            func.count(distinct(case((current_range & has_deposit, DataSocmed.id)))),
            func.count(distinct(case((previous_range & has_deposit, DataSocmed.id)))),
            func.sum(case((current_range, deposit_amount))),
            func.sum(case((previous_range, deposit_amount))),
        ).where(current_range | previous_range)
    )
    current, previous, current_deposit_qty, previous_deposit_qty, current_deposit_usd, previous_deposit_usd = result.one()
    payload["socmed_register"] = current or None
    payload["socmed_growth_percentage"] = growth_percentage(current, previous) if current and previous else None
    socmed_current = _usd_first_deposit_metrics(current_deposit_qty, current_deposit_usd)
    socmed_previous = _usd_first_deposit_metrics(previous_deposit_qty, previous_deposit_usd)
    payload["socmed_first_deposit"] = _deposit_with_growth(socmed_current, socmed_previous)
    socmed_daily = await session.execute(
        select(
            DataSocmed.tgl_regis,
            func.count(distinct(case((has_deposit, DataSocmed.id)))),
            func.sum(deposit_amount),
        )
        .where(current_range)
        .group_by(DataSocmed.tgl_regis)
    )
    socmed_by_day = {day: _usd_first_deposit_metrics(qty, usd) for day, qty, usd in socmed_daily}
    total_register = payload["total_register"]
    previous_total_register = previous_totals["total_register"]
    organic = total_register - (payload["ads_register"] or 0) - current if total_register is not None else None
    previous_organic = (
        previous_total_register - (previous_totals["ads_register"] or 0) - previous
        if previous_total_register is not None else None
    )
    payload["organic_register"] = organic
    payload["organic_growth_percentage"] = (
        growth_percentage(organic, previous_organic)
        if organic is not None and previous_organic is not None else None
    )
    current_range = FirstDepo.date.between(start_date, end_date)
    previous_range = FirstDepo.date.between(previous_start, previous_end)
    daily_deposits = await session.execute(
        select(FirstDepo.date, FirstDepo.register_qty, FirstDepo.first_deposit_qty, FirstDepo.first_deposit_amount)
        .where(current_range | previous_range)
        .order_by(FirstDepo.date)
    )
    current_days, previous_days = [], []
    for row in daily_deposits:
        (current_days if row.date >= start_date else previous_days).append(row)
    payload["daily_register"] = [
        {"date": row.date.isoformat(), "total_register": row.register_qty} for row in current_days
    ]
    total_current = _first_deposit_metrics(current_days)
    total_previous = _first_deposit_metrics(previous_days)
    payload["first_deposit"] = _deposit_with_growth(total_current, total_previous)
    payload["register_to_first_deposit"] = _deposit_with_growth(
        {"rate": _register_to_deposit_rate(total_current["qty"], total_register)},
        {"rate": _register_to_deposit_rate(total_previous["qty"], previous_total_register)},
    )

    ads_deposits = union(*(
        select(model.user_id, model.tanggal_regis, model.first_depo_date, model.first_depo)
        .where(
            model.tanggal_regis.between(start_date, end_date)
            | model.tanggal_regis.between(previous_start, previous_end),
            model.first_depo > 0,
        )
        for model in (DataDepo, DataDepoBa)
    ))
    current_ads, previous_ads = [], []
    for row in await session.execute(ads_deposits):
        (current_ads if row.tanggal_regis >= start_date else previous_ads).append(row)
    ads_current = _ads_first_deposit_metrics(current_ads)
    ads_previous = _ads_first_deposit_metrics(previous_ads)
    payload["ads_first_deposit"] = _deposit_with_growth(ads_current, ads_previous)
    payload["organic_first_deposit"] = _deposit_with_growth(
        _organic_first_deposit_metrics(total_current, ads_current, socmed_current),
        _organic_first_deposit_metrics(total_previous, ads_previous, socmed_previous),
    )
    ads_rows_by_day = defaultdict(list)
    for row in current_ads:
        ads_rows_by_day[row.tanggal_regis].append(row)
    ads_by_day = {day: _ads_first_deposit_metrics(rows) for day, rows in ads_rows_by_day.items()}
    total_by_day = {row.date: _first_deposit_metrics([row]) for row in current_days}
    organic_by_day = {
        day: _organic_first_deposit_metrics(total, ads_by_day.get(day, {}), socmed_by_day.get(day, {}))
        for day, total in total_by_day.items()
    }
    payload["daily_first_deposit"] = {
        category: [
            {"date": day.isoformat(), "amount": metrics["amount"], "qty": metrics["qty"]}
            for day, metrics in sorted(by_day.items())
        ]
        for category, by_day in (
            ("Total", total_by_day), ("Ads", ads_by_day), ("Socmed", socmed_by_day), ("Organic", organic_by_day)
        )
    }
    revenue_rows = {}
    for key, day, qty, amount in (
        ("first_subscription", FirstSubs.date, FirstSubs.first_subscription_qty, FirstSubs.first_subscription_amount),
        ("all_deposit", AllDepo.date, AllDepo.total_deposit_qty, AllDepo.total_deposit_amount),
        (
            "all_subscription",
            AllSubscription.date,
            AllSubscription.total_subscription_qty,
            AllSubscription.total_subscription_amount,
        ),
    ):
        rows = await session.execute(
            select(day.label("date"), qty.label("qty"), amount.label("amount"))
            .where(day.between(start_date, end_date) | day.between(previous_start, previous_end))
            .order_by(day)
        )
        current_rows, previous_rows = [], []
        for row in rows.mappings():
            (current_rows if row["date"] >= start_date else previous_rows).append(row)
        if key != "first_subscription":
            revenue_rows[key] = current_rows + previous_rows
        current_metrics = _summarize_daily_amount_qty(current_rows)
        previous_metrics = _summarize_daily_amount_qty(previous_rows)
        payload[key] = _deposit_with_growth(current_metrics, previous_metrics)
        if key == "all_deposit":
            payload["topup"] = _deposit_with_growth(
                _topup_metrics(current_metrics, total_current),
                _topup_metrics(previous_metrics, total_previous),
            )
            all_by_day = {row["date"]: row for row in current_rows}
            payload["daily_topup"] = [
                {"date": day.isoformat(), **_topup_metrics(all_by_day.get(day, {}), total_by_day.get(day, {}))}
                for day in sorted(all_by_day.keys() | total_by_day.keys())
            ]
        payload[f"daily_{key}"] = [
            {"date": row["date"].isoformat(), "amount": row["amount"], "qty": row["qty"]}
            for row in current_rows
        ]
    payload["cost_to_revenue"] = await fetch_management_cost(
        session, (start_date, end_date), (previous_start, previous_end),
        revenue_rows["all_deposit"], revenue_rows["all_subscription"],
    )
    return payload


def _topup_metrics(total: dict, first: dict) -> dict:
    amount = qty = None
    if total.get("amount") is not None and first.get("amount") is not None:
        amount = float(Decimal(str(total["amount"])) - Decimal(str(first["amount"])))
    if total.get("qty") is not None and first.get("qty") is not None:
        qty = total["qty"] - first["qty"]
    return {"amount": amount, "qty": qty, "average": amount / qty if amount is not None and qty else None}


def _register_to_deposit_rate(deposit_qty: int | None, register_qty: int | None) -> float | None:
    return deposit_qty / register_qty if register_qty and deposit_qty is not None else None


def _deposit_with_growth(current: dict, previous: dict) -> dict:
    return {
        "metrics": current,
        "growth_percentage": {
            key: growth_percentage(value, previous[key])
            if value is not None and previous[key] is not None else None
            for key, value in current.items()
        },
    }


def _ads_first_deposit_metrics(rows) -> dict:
    qty = len({row.user_id for row in rows})
    usd = sum(Decimal(str(row.first_depo)) for row in rows) if rows else None
    return _usd_first_deposit_metrics(qty, usd)


def _usd_first_deposit_metrics(qty: int, usd: float | Decimal | None) -> dict:
    if usd is None:
        return {"amount": None, "qty": None, "average": None}
    amount = float(Decimal(str(usd)) * Decimal(str(USD_TO_IDR_RATE)))
    return {"amount": amount, "qty": qty, "average": amount / qty if qty else None}


def _first_deposit_metrics(rows) -> dict:
    return _summarize_daily_amount_qty([
        {"qty": row.first_deposit_qty, "amount": row.first_deposit_amount} for row in rows
    ])


def _summarize_daily_amount_qty(rows) -> dict:
    qty = sum(row["qty"] for row in rows) if rows else None
    amount = float(sum(Decimal(str(row["amount"])) for row in rows)) if rows else None
    return {"amount": amount, "qty": qty, "average": amount / qty if qty else None}


def _organic_first_deposit_metrics(total: dict, ads: dict, socmed: dict) -> dict:
    if total["amount"] is None or total["qty"] is None:
        return {"amount": None, "qty": None, "average": None}
    amount = total["amount"] - (ads.get("amount") or 0) - (socmed.get("amount") or 0)
    qty = total["qty"] - (ads.get("qty") or 0) - (socmed.get("qty") or 0)
    return {"amount": amount, "qty": qty, "average": amount / qty if qty > 0 else None}


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
