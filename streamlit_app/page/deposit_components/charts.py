"""Chart builders for the deposit page."""

from __future__ import annotations

import datetime as dt
import math

import pandas as pd
import plotly.graph_objects as go

from streamlit_app.page.deposit_components.formatting import currency_label, currency_multiplier, format_amount_full


def _format_heatmap_tick(value: float, currency_unit: str) -> str:
    abs_value = abs(float(value or 0))
    if currency_unit == "IDR":
        if abs_value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.1f}B"
        if abs_value >= 1_000_000:
            return f"{value / 1_000_000:.1f}M"
        if abs_value >= 1_000:
            return f"{value / 1_000:.0f}K"
        return f"{value:.0f}"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.0f}k"
    return f"{value:.0f}"


def extract_daily_metric_values(report: dict[str, object], metric_key: str, timeline: list[str]) -> dict[str, dict[str, float]]:
    daily_metrics = {
        str(row.get("date")): row
        for row in report.get("daily_metrics", [])
        if isinstance(row, dict)
    }
    values: dict[str, dict[str, float]] = {}
    for day in timeline:
        metric_values = daily_metrics.get(day, {}).get(metric_key, {})
        values[day] = {
            "new": float(metric_values.get("new", 0) or 0),
            "existing": float(metric_values.get("existing", 0) or 0),
        }
    return values


def build_daily_deposit_amount_figure(
    report: dict[str, object],
    currency_unit: str,
    deposit_label: str = "First Deposit",
) -> go.Figure:
    timeline = report.get("timeline", [])
    if not timeline or not report.get("daily_metrics"):
        figure = go.Figure()
        figure.update_layout(title=f"Daily {deposit_label} Amount: New vs Existing", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    amount_map = extract_daily_metric_values(report, "depo_amount", timeline)
    date_labels = [dt.date.fromisoformat(day).strftime("%b %d\n%Y") for day in timeline]
    multiplier = currency_multiplier(currency_unit)
    currency_symbol = currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"
    figure = go.Figure()
    figure.add_trace(go.Bar(x=date_labels, y=[amount_map[day]["new"] * multiplier for day in timeline], name="New User", marker_color="#6176ff", hovertemplate=f"<b>%{{x}}</b><br>New: {currency_symbol} %{{y:,{decimals}}}<extra></extra>"))
    figure.add_trace(go.Bar(x=date_labels, y=[amount_map[day]["existing"] * multiplier for day in timeline], name="Existing User", marker_color="#ff7a59", hovertemplate=f"<b>%{{x}}</b><br>Existing: {currency_symbol} %{{y:,{decimals}}}<extra></extra>"))
    figure.update_layout(title=f"Daily {deposit_label} Amount: New vs Existing", barmode="stack", xaxis=dict(type="category"), yaxis=dict(title=f"{deposit_label} Amount ({currency_symbol})"), legend=dict(orientation="h", y=1.08, x=0), margin=dict(l=24, r=24, t=60, b=24))
    return figure


def build_daily_deposit_qty_aov_figure(
    report: dict[str, object],
    currency_unit: str,
    deposit_label: str = "First Deposit",
) -> go.Figure:
    timeline = report.get("timeline", [])
    if not timeline or not report.get("daily_metrics"):
        figure = go.Figure()
        figure.update_layout(title=f"Daily {deposit_label} Qty + AOV", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    qty_map = extract_daily_metric_values(report, "qty", timeline)
    aov_map = extract_daily_metric_values(report, "aov", timeline)
    date_labels = [dt.date.fromisoformat(day).strftime("%b %d\n%Y") for day in timeline]
    multiplier = currency_multiplier(currency_unit)
    currency_symbol = currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"
    figure = go.Figure()
    figure.add_trace(go.Bar(x=date_labels, y=[qty_map[day]["new"] for day in timeline], name="New Qty", marker_color="#6176ff", offsetgroup="new_qty", hovertemplate="<b>%{x}</b><br>New Qty: %{y:,.0f}<extra></extra>"))
    figure.add_trace(go.Bar(x=date_labels, y=[qty_map[day]["existing"] for day in timeline], name="Existing Qty", marker_color="#8ea0ff", offsetgroup="existing_qty", hovertemplate="<b>%{x}</b><br>Existing Qty: %{y:,.0f}<extra></extra>"))
    figure.add_trace(go.Scatter(x=date_labels, y=[aov_map[day]["new"] * multiplier for day in timeline], mode="lines+markers", name="New AOV", yaxis="y2", line=dict(color="#22c55e", width=2), hovertemplate=f"<b>%{{x}}</b><br>New AOV: {currency_symbol} %{{y:,{decimals}}}<extra></extra>"))
    figure.add_trace(go.Scatter(x=date_labels, y=[aov_map[day]["existing"] * multiplier for day in timeline], mode="lines+markers", name="Existing AOV", yaxis="y2", line=dict(color="#f97316", width=2, dash="dot"), hovertemplate=f"<b>%{{x}}</b><br>Existing AOV: {currency_symbol} %{{y:,{decimals}}}<extra></extra>"))
    figure.update_layout(title=f"Daily {deposit_label} Qty + AOV", xaxis=dict(type="category"), yaxis=dict(title=f"{deposit_label} Qty"), yaxis2=dict(title=f"AOV ({currency_symbol})", overlaying="y", side="right"), legend=dict(orientation="h", y=1.12, x=0), margin=dict(l=24, r=24, t=68, b=24))
    return figure


def build_top_campaign_deposit_figure(
    report: dict[str, object],
    currency_unit: str,
    top_n: int = 10,
    deposit_label: str = "First Deposit",
) -> go.Figure:
    campaign_totals = report.get("campaign_totals", [])
    if not campaign_totals:
        figure = go.Figure()
        figure.update_layout(title=f"Top Campaign by {deposit_label} Amount", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    rows: list[dict[str, object]] = []
    multiplier = currency_multiplier(currency_unit)
    currency_symbol = currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"
    for campaign in campaign_totals:
        if not isinstance(campaign, dict):
            continue
        amount_map = campaign.get("depo_amount", {})
        new_total = float(amount_map.get("new", 0) or 0)
        existing_total = float(amount_map.get("existing", 0) or 0)
        total = new_total + existing_total
        if total <= 0:
            continue
        label = str(campaign.get("campaign_name") or "Unknown Campaign").strip()
        rows.append({"label": label if len(label) <= 44 else f"{label[:41]}...", "new_total": new_total * multiplier, "existing_total": existing_total * multiplier, "total": total * multiplier})
    if not rows:
        figure = go.Figure()
        figure.update_layout(title=f"Top Campaign by {deposit_label} Amount", annotations=[{"text": f"No positive {deposit_label.lower()} data", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    ranked = pd.DataFrame(rows).sort_values("total", ascending=False).head(top_n).sort_values("total", ascending=True)
    figure = go.Figure()
    figure.add_trace(go.Bar(x=ranked["new_total"].tolist(), y=ranked["label"].tolist(), orientation="h", name="New User", marker_color="#6176ff", hovertemplate=f"<b>%{{y}}</b><br>New: {currency_symbol} %{{x:,{decimals}}}<extra></extra>"))
    figure.add_trace(go.Bar(x=ranked["existing_total"].tolist(), y=ranked["label"].tolist(), orientation="h", name="Existing User", marker_color="#ff7a59", hovertemplate=f"<b>%{{y}}</b><br>Existing: {currency_symbol} %{{x:,{decimals}}}<extra></extra>"))
    figure.update_layout(title=f"Top Campaign by {deposit_label} Amount", barmode="stack", xaxis=dict(title=f"{deposit_label} Amount ({currency_symbol})"), yaxis=dict(title="Campaign"), legend=dict(orientation="h", y=1.08, x=0), margin=dict(l=24, r=24, t=60, b=24))
    return figure


def build_campaign_deposit_amount_heatmap_figure(
    report: dict[str, object],
    currency_unit: str,
    top_n: int = 12,
    deposit_label: str = "First Deposit",
) -> go.Figure:
    timeline = [str(day) for day in report.get("timeline", [])]
    rows = [row for row in report.get("campaign_daily_metrics", []) if isinstance(row, dict)]
    if not timeline or not rows:
        figure = go.Figure()
        figure.update_layout(title=f"Daily {deposit_label} Amount Heatmap (Top {top_n} Campaigns)", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure

    dataframe = pd.DataFrame(rows)
    required_columns = {"date", "campaign_id", "campaign_name", "depo_amount"}
    if dataframe.empty or not required_columns.issubset(dataframe.columns):
        figure = go.Figure()
        figure.update_layout(title=f"Daily {deposit_label} Amount Heatmap (Top {top_n} Campaigns)", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure

    multiplier = currency_multiplier(currency_unit)
    currency_symbol = currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"
    dataframe["date"] = dataframe["date"].astype(str)
    dataframe["campaign_id"] = dataframe["campaign_id"].astype(str)
    dataframe["campaign_name"] = dataframe["campaign_name"].fillna("Unknown Campaign").astype(str)
    dataframe["depo_amount"] = pd.to_numeric(dataframe["depo_amount"], errors="coerce").fillna(0.0) * multiplier
    top_campaigns = (
        dataframe.groupby(["campaign_id", "campaign_name"], as_index=False)["depo_amount"]
        .sum()
        .sort_values("depo_amount", ascending=False)
        .head(top_n)
    )
    selected = dataframe.loc[dataframe["campaign_id"].isin(top_campaigns["campaign_id"])].copy()
    if selected.empty:
        figure = go.Figure()
        figure.update_layout(title=f"Daily {deposit_label} Amount Heatmap (Top {top_n} Campaigns)", annotations=[{"text": "No positive first deposit amount data", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure

    selected["campaign_label"] = selected["campaign_name"].apply(lambda value: value if len(value) <= 42 else f"{value[:39]}...")
    heatmap = selected.pivot_table(index="campaign_label", columns="date", values="depo_amount", aggfunc="sum", fill_value=0)
    heatmap = heatmap.reindex(columns=timeline, fill_value=0)
    ordered_labels = (
        selected.groupby("campaign_label")["depo_amount"]
        .sum()
        .sort_values(ascending=True)
        .index
        .tolist()
    )
    heatmap = heatmap.reindex(ordered_labels)
    date_labels = [dt.date.fromisoformat(day).strftime("%b %d\n%Y") for day in timeline]
    amount_values = heatmap.to_numpy()
    color_values = [[math.log1p(float(value or 0)) for value in row] for row in amount_values]
    max_amount = float(amount_values.max() or 0) if amount_values.size else 0.0
    tick_amounts = [0.0]
    if max_amount > 0:
        tick_amounts.extend([max_amount * ratio for ratio in (0.25, 0.5, 0.75, 1.0)])
    figure = go.Figure(
        data=[
            go.Heatmap(
                x=date_labels,
                y=heatmap.index.tolist(),
                z=color_values,
                customdata=amount_values,
                colorscale=[
                    [0.0, "#0f172a"],
                    [0.08, "#1e40af"],
                    [0.24, "#2563eb"],
                    [0.48, "#22c55e"],
                    [0.72, "#facc15"],
                    [1.0, "#f97316"],
                ],
                colorbar=dict(
                    title=f"{deposit_label} Amount",
                    tickvals=[math.log1p(value) for value in tick_amounts],
                    ticktext=[_format_heatmap_tick(value, currency_unit) for value in tick_amounts],
                ),
                hovertemplate=f"<b>%{{y}}</b><br><b>%{{x}}</b><br>{deposit_label} Amount: {currency_symbol} %{{customdata:,{decimals}}}<extra></extra>",
            )
        ]
    )
    figure.update_layout(title=f"Daily {deposit_label} Amount Heatmap (Top {top_n} Campaigns)", xaxis_title="Date", yaxis_title="", xaxis=dict(type="category"), margin=dict(l=24, r=24, t=60, b=24))
    return figure


def build_deposit_method_pie_figure(
    report: dict[str, object],
    currency_unit: str,
) -> go.Figure:
    rows = [row for row in report.get("deposit_method_summary", []) if isinstance(row, dict)]
    rows = [row for row in rows if float(row.get("deposit_qty", 0) or 0) > 0]
    if not rows:
        figure = go.Figure()
        figure.update_layout(title="Deposit Method Share", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure

    multiplier = currency_multiplier(currency_unit)
    labels = [str(row.get("method", "-")) for row in rows]
    quantities = [float(row.get("deposit_qty", 0) or 0) for row in rows]
    hovertext = []
    for row in rows:
        deposit_amount = float(row.get("deposit_amount", 0) or 0)
        average_deposit = float(row.get("average_deposit", 0) or 0)
        hovertext.append(
            "<br>".join(
                [
                    f"<b>{row.get('method', '-')}</b>",
                    f"Deposit Qty: {int(row.get('deposit_qty', 0) or 0):,}",
                    f"Deposit Amount: {format_amount_full(deposit_amount, currency_unit=currency_unit)}",
                    f"Average Deposit: {format_amount_full(average_deposit, currency_unit=currency_unit)}",
                ]
            )
        )
    figure = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=quantities,
                hole=0.42,
                marker=dict(colors=["#6176ff", "#22c55e"]),
                customdata=[[float(row.get("deposit_amount", 0) or 0) * multiplier] for row in rows],
                hovertext=hovertext,
                textinfo="label+percent",
                hovertemplate="%{hovertext}<extra></extra>",
            )
        ]
    )
    figure.update_layout(title="Deposit Method Share", margin=dict(l=24, r=24, t=60, b=24), legend=dict(orientation="h", y=1.08, x=0))
    return figure
