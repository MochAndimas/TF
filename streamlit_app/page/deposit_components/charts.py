"""Chart builders for the deposit page."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import plotly.graph_objects as go

from streamlit_app.page.deposit_components.formatting import currency_label, currency_multiplier


def extract_section_metric_values(section: dict[str, object], metric_key: str, timeline: list[str]) -> dict[str, dict[str, float]]:
    for row in section.get("rows", []):
        if str(row.get("key")) == metric_key:
            values = row.get("values", {})
            return {day: {"new": float(values.get(day, {}).get("new", 0) or 0), "existing": float(values.get(day, {}).get("existing", 0) or 0)} for day in timeline}
    return {day: {"new": 0.0, "existing": 0.0} for day in timeline}


def build_daily_deposit_amount_figure(report: dict[str, object], currency_unit: str) -> go.Figure:
    timeline = report.get("timeline", [])
    total_section = next((section for section in report.get("sections", []) if str(section.get("campaign_id")) == "TOTAL"), None)
    if not timeline or total_section is None:
        figure = go.Figure()
        figure.update_layout(title="Daily Deposit Amount: New vs Existing", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    amount_map = extract_section_metric_values(total_section, "depo_amount", timeline)
    date_labels = [dt.date.fromisoformat(day).strftime("%b %d\n%Y") for day in timeline]
    multiplier = currency_multiplier(currency_unit)
    currency_symbol = currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"
    figure = go.Figure()
    figure.add_trace(go.Bar(x=date_labels, y=[amount_map[day]["new"] * multiplier for day in timeline], name="New User", marker_color="#6176ff", hovertemplate=f"<b>%{{x}}</b><br>New: {currency_symbol} %{{y:,{decimals}}}<extra></extra>"))
    figure.add_trace(go.Bar(x=date_labels, y=[amount_map[day]["existing"] * multiplier for day in timeline], name="Existing User", marker_color="#ff7a59", hovertemplate=f"<b>%{{x}}</b><br>Existing: {currency_symbol} %{{y:,{decimals}}}<extra></extra>"))
    figure.update_layout(title="Daily Deposit Amount: New vs Existing", barmode="stack", xaxis=dict(type="category"), yaxis=dict(title=f"Deposit Amount ({currency_symbol})"), legend=dict(orientation="h", y=1.08, x=0), margin=dict(l=24, r=24, t=60, b=24))
    return figure


def build_daily_deposit_qty_aov_figure(report: dict[str, object], currency_unit: str) -> go.Figure:
    timeline = report.get("timeline", [])
    total_section = next((section for section in report.get("sections", []) if str(section.get("campaign_id")) == "TOTAL"), None)
    if not timeline or total_section is None:
        figure = go.Figure()
        figure.update_layout(title="Daily Deposit Qty + AOV", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    qty_map = extract_section_metric_values(total_section, "qty", timeline)
    aov_map = extract_section_metric_values(total_section, "aov", timeline)
    date_labels = [dt.date.fromisoformat(day).strftime("%b %d\n%Y") for day in timeline]
    multiplier = currency_multiplier(currency_unit)
    currency_symbol = currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"
    figure = go.Figure()
    figure.add_trace(go.Bar(x=date_labels, y=[qty_map[day]["new"] for day in timeline], name="New Qty", marker_color="#6176ff", offsetgroup="new_qty", hovertemplate="<b>%{x}</b><br>New Qty: %{y:,.0f}<extra></extra>"))
    figure.add_trace(go.Bar(x=date_labels, y=[qty_map[day]["existing"] for day in timeline], name="Existing Qty", marker_color="#8ea0ff", offsetgroup="existing_qty", hovertemplate="<b>%{x}</b><br>Existing Qty: %{y:,.0f}<extra></extra>"))
    figure.add_trace(go.Scatter(x=date_labels, y=[aov_map[day]["new"] * multiplier for day in timeline], mode="lines+markers", name="New AOV", yaxis="y2", line=dict(color="#22c55e", width=2), hovertemplate=f"<b>%{{x}}</b><br>New AOV: {currency_symbol} %{{y:,{decimals}}}<extra></extra>"))
    figure.add_trace(go.Scatter(x=date_labels, y=[aov_map[day]["existing"] * multiplier for day in timeline], mode="lines+markers", name="Existing AOV", yaxis="y2", line=dict(color="#f97316", width=2, dash="dot"), hovertemplate=f"<b>%{{x}}</b><br>Existing AOV: {currency_symbol} %{{y:,{decimals}}}<extra></extra>"))
    figure.update_layout(title="Daily Deposit Qty + AOV", xaxis=dict(type="category"), yaxis=dict(title="Deposit Qty"), yaxis2=dict(title=f"AOV ({currency_symbol})", overlaying="y", side="right"), legend=dict(orientation="h", y=1.12, x=0), margin=dict(l=24, r=24, t=68, b=24))
    return figure


def build_top_campaign_deposit_figure(report: dict[str, object], currency_unit: str, top_n: int = 10) -> go.Figure:
    timeline = report.get("timeline", [])
    campaign_sections = [section for section in report.get("sections", []) if str(section.get("campaign_id")) != "TOTAL"]
    if not timeline or not campaign_sections:
        figure = go.Figure()
        figure.update_layout(title="Top Campaign by Deposit Amount", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    rows: list[dict[str, object]] = []
    multiplier = currency_multiplier(currency_unit)
    currency_symbol = currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"
    for section in campaign_sections:
        amount_map = extract_section_metric_values(section, "depo_amount", timeline)
        new_total = sum(amount_map[day]["new"] for day in timeline)
        existing_total = sum(amount_map[day]["existing"] for day in timeline)
        total = new_total + existing_total
        if total <= 0:
            continue
        label = str(section.get("title") or section.get("campaign_name") or "Unknown Campaign").strip()
        rows.append({"label": label if len(label) <= 44 else f"{label[:41]}...", "new_total": new_total * multiplier, "existing_total": existing_total * multiplier, "total": total * multiplier})
    if not rows:
        figure = go.Figure()
        figure.update_layout(title="Top Campaign by Deposit Amount", annotations=[{"text": "No positive deposit data", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    ranked = pd.DataFrame(rows).sort_values("total", ascending=False).head(top_n).sort_values("total", ascending=True)
    figure = go.Figure()
    figure.add_trace(go.Bar(x=ranked["new_total"].tolist(), y=ranked["label"].tolist(), orientation="h", name="New User", marker_color="#6176ff", hovertemplate=f"<b>%{{y}}</b><br>New: {currency_symbol} %{{x:,{decimals}}}<extra></extra>"))
    figure.add_trace(go.Bar(x=ranked["existing_total"].tolist(), y=ranked["label"].tolist(), orientation="h", name="Existing User", marker_color="#ff7a59", hovertemplate=f"<b>%{{y}}</b><br>Existing: {currency_symbol} %{{x:,{decimals}}}<extra></extra>"))
    figure.update_layout(title="Top Campaign by Deposit Amount", barmode="stack", xaxis=dict(title=f"Deposit Amount ({currency_symbol})"), yaxis=dict(title="Campaign"), legend=dict(orientation="h", y=1.08, x=0), margin=dict(l=24, r=24, t=60, b=24))
    return figure
