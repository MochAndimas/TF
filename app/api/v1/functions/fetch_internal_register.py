"""Internal register analytics payload builders."""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Campaign, DailyRegister

SOURCE_OPTIONS = {
    "all": None,
    "google": "google_ads",
    "facebook": "facebook_ads",
    "tiktok": "tiktok_ads",
}


def _empty_register_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "campaign_id",
            "campaign_name",
            "ad_source",
            "ad_type",
            "total_regis",
        ]
    )


async def _read_internal_register_rows(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    source: str,
) -> pd.DataFrame:
    source_key = source.strip().lower()
    if source_key not in SOURCE_OPTIONS:
        supported = ", ".join(sorted(SOURCE_OPTIONS))
        raise ValueError(f"Unsupported register source '{source}'. Supported sources: {supported}.")

    query = (
        select(
            DailyRegister.date.label("date"),
            DailyRegister.campaign_id.label("campaign_id"),
            Campaign.campaign_name.label("campaign_name"),
            Campaign.ad_source.label("ad_source"),
            Campaign.ad_type.label("ad_type"),
            func.sum(DailyRegister.total_regis).label("total_regis"),
        )
        .join(DailyRegister.campaign)
        .where(DailyRegister.date.between(start_date, end_date))
        .group_by(
            DailyRegister.date,
            DailyRegister.campaign_id,
            Campaign.campaign_name,
            Campaign.ad_source,
            Campaign.ad_type,
        )
    )
    source_value = SOURCE_OPTIONS[source_key]
    if source_value:
        query = query.where(Campaign.ad_source == source_value)

    result = await session.execute(query)
    rows = result.fetchall()
    if not rows:
        return _empty_register_frame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["campaign_id"] = df["campaign_id"].astype(str)
    df["campaign_name"] = df["campaign_name"].fillna("Unknown Campaign")
    df["ad_source"] = df["ad_source"].fillna("unknown")
    df["ad_type"] = df["ad_type"].fillna("unknown")
    df["total_regis"] = pd.to_numeric(df["total_regis"], errors="coerce").fillna(0).astype(int)
    return df


def _serialize_figure(figure: go.Figure) -> dict[str, object]:
    return json.loads(json.dumps(figure, cls=plotly.utils.PlotlyJSONEncoder))


def _date_labels(series: pd.Series) -> list[str]:
    return pd.to_datetime(series).dt.strftime("%b %d\n%Y").tolist()


async def _figure_payload(figure: go.Figure, rows: pd.DataFrame) -> dict[str, object]:
    serializable = rows.copy()
    if "date" in serializable.columns:
        serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
    return {
        "rows": await asyncio.to_thread(lambda: serializable.to_dict(orient="records")),
        "figure": _serialize_figure(figure),
    }


def _empty_figure(title: str, message: str = "No register data for selected date range") -> go.Figure:
    figure = go.Figure()
    figure.update_layout(
        title=title,
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
            }
        ],
    )
    return figure


async def _daily_trend_chart(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return await _figure_payload(_empty_figure("Daily Internal Register"), pd.DataFrame())

    daily = df.groupby("date", as_index=False)["total_regis"].sum().sort_values("date")
    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=_date_labels(daily["date"]),
            y=daily["total_regis"],
            name="Register",
            text=[f"{int(value):,}" for value in daily["total_regis"]],
            textposition="auto",
            hovertemplate="<b>%{x}</b><br>Register: %{y:,}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=_date_labels(daily["date"]),
            y=daily["total_regis"].rolling(window=7, min_periods=1).mean(),
            name="7D Avg",
            mode="lines+markers",
            hovertemplate="<b>%{x}</b><br>7D Avg: %{y:,.1f}<extra></extra>",
        )
    )
    figure.update_layout(title="Daily Internal Register", xaxis_title="Date", yaxis_title="Register", xaxis=dict(type="category"), legend=dict(orientation="h", y=1.12, x=0))
    return await _figure_payload(figure, daily)


async def _source_mix_chart(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return await _figure_payload(_empty_figure("Register by Source"), pd.DataFrame())

    daily_source = df.groupby(["date", "ad_source"], as_index=False)["total_regis"].sum().sort_values(["date", "ad_source"])
    pivot = daily_source.pivot_table(index="date", columns="ad_source", values="total_regis", aggfunc="sum", fill_value=0).reset_index()
    figure = go.Figure()
    date_labels = _date_labels(pivot["date"])
    source_order = ["google_ads", "facebook_ads", "tiktok_ads", "unknown"]
    for source in [item for item in source_order if item in pivot.columns] + [item for item in pivot.columns if item not in {"date", *source_order}]:
        figure.add_trace(
            go.Bar(
                x=date_labels,
                y=pivot[source],
                name=source.replace("_", " ").title(),
                hovertemplate="<b>%{x}</b><br>Register: %{y:,}<extra></extra>",
            )
        )
    figure.update_layout(title="Register by Source", xaxis_title="Date", yaxis_title="Register", barmode="stack", xaxis=dict(type="category"), legend=dict(orientation="h", y=1.12, x=0))
    return await _figure_payload(figure, pivot)


async def _cumulative_trend_chart(df: pd.DataFrame, top_n: int = 8) -> dict[str, object]:
    if df.empty:
        return await _figure_payload(_empty_figure("Cumulative Register by Campaign"), pd.DataFrame())

    top_campaigns = (
        df.groupby(["campaign_id", "campaign_name"], as_index=False)["total_regis"]
        .sum()
        .sort_values("total_regis", ascending=False)
        .head(top_n)
    )
    daily = (
        df.loc[df["campaign_id"].isin(top_campaigns["campaign_id"])]
        .groupby(["date", "campaign_id", "campaign_name"], as_index=False)["total_regis"]
        .sum()
        .sort_values(["campaign_name", "date"])
    )
    daily["cumulative_register"] = daily.groupby("campaign_id")["total_regis"].cumsum()
    figure = go.Figure()
    ordered_campaigns = top_campaigns.sort_values("total_regis", ascending=False)
    for _, campaign in ordered_campaigns.iterrows():
        subset = daily.loc[daily["campaign_id"] == campaign["campaign_id"]].sort_values("date")
        if subset.empty:
            continue
        campaign_name = str(campaign["campaign_name"])
        short_name = campaign_name if len(campaign_name) <= 34 else f"{campaign_name[:31]}..."
        figure.add_trace(
            go.Scatter(
                x=_date_labels(subset["date"]),
                y=subset["cumulative_register"],
                mode="lines+markers",
                name=short_name,
                customdata=subset[["campaign_id", "total_regis"]].to_numpy(),
                hovertemplate="<b>%{fullData.name}</b><br>ID: %{customdata[0]}<br><b>%{x}</b><br>Daily: %{customdata[1]:,}<br>Cumulative: %{y:,}<extra></extra>",
            )
        )
    figure.update_layout(
        title=f"Cumulative Register by Campaign (Top {top_n})",
        xaxis_title="Date",
        yaxis_title="Register",
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.18, x=0),
    )
    return await _figure_payload(figure, daily)


async def _campaign_heatmap_chart(df: pd.DataFrame, top_n: int = 12) -> dict[str, object]:
    if df.empty:
        return await _figure_payload(_empty_figure("Daily Register Heatmap"), pd.DataFrame())

    top_campaigns = (
        df.groupby(["campaign_id", "campaign_name"], as_index=False)["total_regis"]
        .sum()
        .sort_values("total_regis", ascending=False)
        .head(top_n)
    )
    selected = df.loc[df["campaign_id"].isin(top_campaigns["campaign_id"])].copy()
    selected["campaign_label"] = selected["campaign_name"].astype(str).apply(lambda value: value if len(value) <= 42 else f"{value[:39]}...")
    heatmap = selected.pivot_table(
        index="campaign_label",
        columns="date",
        values="total_regis",
        aggfunc="sum",
        fill_value=0,
    )
    ordered_labels = (
        selected.groupby("campaign_label")["total_regis"]
        .sum()
        .sort_values(ascending=True)
        .index
        .tolist()
    )
    heatmap = heatmap.reindex(ordered_labels)
    date_labels = pd.to_datetime(heatmap.columns).strftime("%b %d\n%Y").tolist()
    figure = go.Figure(
        data=[
            go.Heatmap(
                x=date_labels,
                y=heatmap.index.tolist(),
                z=heatmap.to_numpy(),
                colorscale="Blues",
                colorbar=dict(title="Register"),
                hovertemplate="<b>%{y}</b><br><b>%{x}</b><br>Register: %{z:,}<extra></extra>",
            )
        ]
    )
    figure.update_layout(
        title=f"Daily Register Heatmap (Top {top_n} Campaigns)",
        xaxis_title="Date",
        yaxis_title="",
        xaxis=dict(type="category"),
    )
    rows = selected[["date", "campaign_id", "campaign_name", "total_regis"]].sort_values(["date", "campaign_name"])
    return await _figure_payload(figure, rows)


async def _type_mix_chart(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return await _figure_payload(_empty_figure("Register by Campaign Type"), pd.DataFrame())

    type_df = df.groupby("ad_type", as_index=False)["total_regis"].sum().sort_values("total_regis", ascending=False)
    figure = go.Figure(
        data=[
            go.Pie(
                labels=type_df["ad_type"].str.replace("_", " ").str.title(),
                values=type_df["total_regis"],
                hole=0.48,
                hovertemplate="<b>%{label}</b><br>Register: %{value:,}<br>Share: %{percent}<extra></extra>",
            )
        ]
    )
    figure.update_layout(title="Register by Campaign Type", legend=dict(orientation="h", y=-0.05, x=0))
    return await _figure_payload(figure, type_df)


async def _top_campaign_chart(df: pd.DataFrame, top_n: int = 15) -> dict[str, object]:
    if df.empty:
        return await _figure_payload(_empty_figure(f"Top {top_n} Campaigns by Register"), pd.DataFrame())

    campaign_df = (
        df.groupby(["campaign_id", "campaign_name", "ad_source", "ad_type"], as_index=False)["total_regis"]
        .sum()
        .sort_values("total_regis", ascending=False)
        .head(top_n)
        .sort_values("total_regis", ascending=True)
    )
    campaign_df["short_name"] = campaign_df["campaign_name"].astype(str).apply(lambda value: value if len(value) <= 42 else f"{value[:39]}...")
    figure = go.Figure(
        data=[
            go.Bar(
                x=campaign_df["total_regis"],
                y=campaign_df["short_name"],
                orientation="h",
                text=[f"{int(value):,}" for value in campaign_df["total_regis"]],
                textposition="auto",
                customdata=campaign_df[["campaign_id", "ad_source", "ad_type"]].to_numpy(),
                hovertemplate="<b>%{y}</b><br>ID: %{customdata[0]}<br>Source: %{customdata[1]}<br>Type: %{customdata[2]}<br>Register: %{x:,}<extra></extra>",
            )
        ]
    )
    figure.update_layout(title=f"Top {top_n} Campaigns by Register", xaxis_title="Register", yaxis_title="")
    return await _figure_payload(figure, campaign_df.drop(columns=["short_name"]))


def _single_period_metrics(df: pd.DataFrame, start_date: date, end_date: date) -> dict[str, object]:
    if df.empty:
        return {
            "total_register": 0,
            "avg_daily_register": 0.0,
            "active_campaigns": 0,
            "active_sources": 0,
            "peak_day": None,
            "peak_day_register": 0,
        }

    total = int(df["total_regis"].sum())
    days = max((end_date - start_date).days + 1, 1)
    daily = df.groupby("date", as_index=False)["total_regis"].sum().sort_values("total_regis", ascending=False)
    peak = daily.iloc[0]
    return {
        "total_register": total,
        "avg_daily_register": round(total / days, 2),
        "active_campaigns": int(df["campaign_id"].nunique()),
        "active_sources": int(df["ad_source"].nunique()),
        "peak_day": peak["date"].isoformat(),
        "peak_day_register": int(peak["total_regis"]),
    }


def _growth_percentage(current_value: float, previous_value: float) -> float:
    if previous_value == 0:
        return 0.0 if current_value == 0 else 100.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def _metric_payload(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
    previous_start: date,
    previous_end: date,
) -> dict[str, object]:
    current_metrics = _single_period_metrics(current_df, start_date, end_date)
    previous_metrics = _single_period_metrics(previous_df, previous_start, previous_end)
    growth_keys = (
        "total_register",
        "avg_daily_register",
        "active_campaigns",
        "active_sources",
        "peak_day_register",
    )
    growth = {
        key: _growth_percentage(float(current_metrics.get(key) or 0), float(previous_metrics.get(key) or 0))
        for key in growth_keys
    }
    return {
        "current_period": {
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "metrics": current_metrics,
        },
        "previous_period": {
            "from_date": previous_start.isoformat(),
            "to_date": previous_end.isoformat(),
            "metrics": previous_metrics,
        },
        "growth_percentage": growth,
    }


async def _details_table(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    total = float(df["total_regis"].sum()) or 1.0
    grouped = (
        df.groupby(["campaign_id", "campaign_name", "ad_source", "ad_type"], as_index=False)
        .agg(total_register=("total_regis", "sum"), active_days=("date", "nunique"), first_date=("date", "min"), last_date=("date", "max"))
        .sort_values("total_register", ascending=False)
    )
    grouped["avg_daily_register"] = grouped.apply(lambda row: round(float(row["total_register"]) / float(row["active_days"]), 2) if float(row["active_days"]) else 0.0, axis=1)
    grouped["share_pct"] = grouped["total_register"].apply(lambda value: round((float(value) / total) * 100, 2))
    grouped["first_date"] = pd.to_datetime(grouped["first_date"]).dt.date.astype(str)
    grouped["last_date"] = pd.to_datetime(grouped["last_date"]).dt.date.astype(str)
    return await asyncio.to_thread(lambda: grouped.to_dict(orient="records"))


async def fetch_internal_register_payload(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    source: str = "all",
) -> dict[str, object]:
    """Build complete internal-register analytics payload."""
    period_days = (end_date - start_date).days + 1
    previous_end = start_date - timedelta(days=1)
    previous_start = previous_end - timedelta(days=period_days - 1)
    df = await _read_internal_register_rows(
        session=session,
        start_date=start_date,
        end_date=end_date,
        source=source,
    )
    previous_df = await _read_internal_register_rows(
        session=session,
        start_date=previous_start,
        end_date=previous_end,
        source=source,
    )
    daily_chart, cumulative_chart, source_chart, type_chart, top_campaign_chart, heatmap_chart, details = await asyncio.gather(
        _daily_trend_chart(df),
        _cumulative_trend_chart(df),
        _source_mix_chart(df),
        _type_mix_chart(df),
        _top_campaign_chart(df),
        _campaign_heatmap_chart(df),
        _details_table(df),
    )
    return {
        "source": source.strip().lower(),
        "from_date": start_date.isoformat(),
        "to_date": end_date.isoformat(),
        "metrics": _metric_payload(
            df,
            previous_df,
            start_date=start_date,
            end_date=end_date,
            previous_start=previous_start,
            previous_end=previous_end,
        ),
        "charts": {
            "daily_trend": daily_chart,
            "cumulative_trend": cumulative_chart,
            "source_mix": source_chart,
            "type_mix": type_chart,
            "top_campaigns": top_campaign_chart,
            "campaign_heatmap": heatmap_chart,
        },
        "details": details,
    }
