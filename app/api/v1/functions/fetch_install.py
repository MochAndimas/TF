"""Google Play Console install analytics payload builders."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import PlayConsoleInstallMetrics


def _growth_percentage(current_value: float, previous_value: float) -> float:
    if previous_value == 0:
        return 100.0 if current_value else 0.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def _previous_period_range(start_date: date, end_date: date) -> tuple[date, date]:
    period_days = (end_date - start_date).days + 1
    previous_end = start_date - timedelta(days=1)
    previous_start = previous_end - timedelta(days=period_days - 1)
    return previous_start, previous_end


def _normal_filter(value: str | None) -> str | None:
    normalized = (value or "").strip()
    if not normalized or normalized.lower() == "all":
        return None
    return normalized


async def _read_rows(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    package_name: str | None = None,
    country: str | None = None,
) -> pd.DataFrame:
    filters = [PlayConsoleInstallMetrics.date.between(start_date, end_date)]
    if package_name:
        filters.append(PlayConsoleInstallMetrics.package_name == package_name)
    if country:
        filters.append(PlayConsoleInstallMetrics.country == country)

    query = (
        select(
            PlayConsoleInstallMetrics.date.label("date"),
            PlayConsoleInstallMetrics.package_name.label("package_name"),
            PlayConsoleInstallMetrics.country.label("country"),
            func.sum(PlayConsoleInstallMetrics.installers).label("installers"),
            func.sum(PlayConsoleInstallMetrics.uninstallers).label("uninstallers"),
            func.sum(PlayConsoleInstallMetrics.active_devices).label("active_devices"),
        )
        .where(*filters)
        .group_by(
            PlayConsoleInstallMetrics.date,
            PlayConsoleInstallMetrics.package_name,
            PlayConsoleInstallMetrics.country,
        )
        .order_by(PlayConsoleInstallMetrics.date, PlayConsoleInstallMetrics.package_name, PlayConsoleInstallMetrics.country)
    )
    rows = (await session.execute(query)).fetchall()
    if not rows:
        return pd.DataFrame(columns=["date", "package_name", "country", "installers", "uninstallers", "active_devices"])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in ("installers", "uninstallers", "active_devices"):
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ("package_name", "country"):
        df[column] = df[column].fillna("").astype(str)
    return df


async def _filter_options(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> dict[str, list[str]]:
    query = (
        select(
            PlayConsoleInstallMetrics.package_name.label("package_name"),
            PlayConsoleInstallMetrics.country.label("country"),
        )
        .where(PlayConsoleInstallMetrics.date.between(start_date, end_date))
        .group_by(PlayConsoleInstallMetrics.package_name, PlayConsoleInstallMetrics.country)
        .order_by(PlayConsoleInstallMetrics.package_name, PlayConsoleInstallMetrics.country)
    )
    rows = (await session.execute(query)).fetchall()
    packages = sorted({str(row.package_name) for row in rows if row.package_name})
    countries = sorted({str(row.country) for row in rows if row.country})
    return {"packages": packages, "countries": countries}


def _metric_summary(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
    previous_start: date,
    previous_end: date,
) -> dict[str, object]:
    metric_keys = ["installers", "uninstallers", "net_installs", "active_devices", "churn_rate"]

    def values(df: pd.DataFrame) -> dict[str, float]:
        installers = int(df["installers"].sum()) if not df.empty else 0
        uninstallers = int(df["uninstallers"].sum()) if not df.empty else 0
        active_devices = 0
        if not df.empty:
            daily_devices = (
                df.groupby("date", as_index=False)
                .agg(active_devices=("active_devices", "sum"))
                .sort_values("date")
            )
            active_devices = int(daily_devices.iloc[-1]["active_devices"]) if not daily_devices.empty else 0
        churn_rate = round((uninstallers / installers) * 100, 2) if installers else 0.0
        return {
            "installers": installers,
            "uninstallers": uninstallers,
            "net_installs": installers - uninstallers,
            "active_devices": active_devices,
            "churn_rate": churn_rate,
        }

    current = values(current_df)
    previous = values(previous_df)
    growth = {
        key: _growth_percentage(float(current[key]), float(previous[key]))
        for key in metric_keys
    }
    return {
        "current_period": {"from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "metrics": current},
        "previous_period": {"from_date": previous_start.isoformat(), "to_date": previous_end.isoformat(), "metrics": previous},
        "growth_percentage": growth,
    }


def _daily_rows(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    grouped = (
        df.groupby("date", as_index=False)
        .agg(
            installers=("installers", "sum"),
            uninstallers=("uninstallers", "sum"),
            active_devices=("active_devices", "sum"),
        )
        .sort_values("date")
    )
    grouped["net_installs"] = grouped["installers"] - grouped["uninstallers"]
    grouped["date"] = grouped["date"].astype(str)
    return grouped.to_dict(orient="records")


def _dimension_rows(df: pd.DataFrame, dimension: str) -> list[dict[str, object]]:
    if df.empty:
        return []
    grouped = (
        df.groupby(dimension, as_index=False)
        .agg(
            installers=("installers", "sum"),
            uninstallers=("uninstallers", "sum"),
            active_devices=("active_devices", "max"),
        )
        .sort_values("installers", ascending=False)
    )
    grouped["net_installs"] = grouped["installers"] - grouped["uninstallers"]
    total_installers = float(grouped["installers"].sum())
    grouped["share_pct"] = grouped["installers"].apply(
        lambda value: round((float(value) / total_installers) * 100, 2) if total_installers else 0.0
    )
    return grouped.to_dict(orient="records")


def _detail_rows(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    grouped = (
        df.groupby("date", as_index=False)
        .agg(
            installers=("installers", "sum"),
            uninstallers=("uninstallers", "sum"),
            active_devices=("active_devices", "sum"),
        )
        .sort_values("date")
    )
    grouped["net_installs"] = grouped["installers"] - grouped["uninstallers"]
    grouped["churn_rate"] = grouped.apply(
        lambda row: round((float(row["uninstallers"]) / float(row["installers"])) * 100, 2)
        if float(row["installers"])
        else 0.0,
        axis=1,
    )
    grouped["date"] = grouped["date"].astype(str)
    return grouped.to_dict(orient="records")


async def fetch_install_analytics_payload(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    package_name: str | None = None,
    country: str | None = None,
) -> dict[str, object]:
    """Build Google Play Console install analytics payload."""
    selected_package = _normal_filter(package_name)
    selected_country = _normal_filter(country)
    previous_start, previous_end = _previous_period_range(start_date, end_date)
    current_df = await _read_rows(
        session=session,
        start_date=start_date,
        end_date=end_date,
        package_name=selected_package,
        country=selected_country,
    )
    previous_df = await _read_rows(
        session=session,
        start_date=previous_start,
        end_date=previous_end,
        package_name=selected_package,
        country=selected_country,
    )
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "filters": {
            "package_name": selected_package or "all",
            "country": selected_country or "all",
            **await _filter_options(session=session, start_date=start_date, end_date=end_date),
        },
        "metrics": _metric_summary(
            current_df,
            previous_df,
            start_date=start_date,
            end_date=end_date,
            previous_start=previous_start,
            previous_end=previous_end,
        ),
        "daily_rows": _daily_rows(current_df),
        "package_rows": _dimension_rows(current_df, "package_name"),
        "country_rows": _dimension_rows(current_df, "country"),
        "details": _detail_rows(current_df),
    }
