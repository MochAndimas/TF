"""Chart builders for custom Overview page visuals."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from streamlit_app.page.overview_components.formatting import convert_idr_to_usd


def build_cost_vs_deposit_figure(rows: list[dict], currency_unit: str) -> go.Figure:
    daily_df = pd.DataFrame(rows or [])
    if daily_df.empty:
        figure = go.Figure()
        figure.update_layout(title="Cost vs Deposit Per Hari", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
    daily_df["cost"] = pd.to_numeric(daily_df.get("cost", 0), errors="coerce").fillna(0.0)
    daily_df["first_deposit_idr"] = pd.to_numeric(daily_df.get("first_deposit_idr", 0), errors="coerce").fillna(0.0)
    cost_values = daily_df["cost"].tolist()
    deposit_values = daily_df["first_deposit_idr"].tolist()
    if currency_unit == "USD":
        cost_values = [convert_idr_to_usd(value) for value in cost_values]
        deposit_values = [convert_idr_to_usd(value) for value in deposit_values]
        cost_hover = "<b>%{x}</b><br>Cost: $ %{y:,.2f}<extra></extra>"
        deposit_hover = "<b>%{x}</b><br>Deposit: $ %{y:,.2f}<extra></extra>"
        cost_name = "Cost (USD)"
        deposit_name = "Deposit (USD)"
    else:
        cost_hover = "<b>%{x}</b><br>Cost: Rp %{y:,.0f}<extra></extra>"
        deposit_hover = "<b>%{x}</b><br>Deposit: Rp %{y:,.0f}<extra></extra>"
        cost_name = "Cost (IDR)"
        deposit_name = "Deposit (IDR)"
    figure = go.Figure()
    date_labels = daily_df["date"].dt.strftime("%b %d\n%Y").tolist()
    figure.add_trace(go.Bar(x=date_labels, y=cost_values, name=cost_name, marker_color="#6176ff", yaxis="y", offsetgroup="cost", hovertemplate=cost_hover))
    figure.add_trace(go.Bar(x=date_labels, y=deposit_values, name=deposit_name, marker_color="#13c39c", yaxis="y2", offsetgroup="deposit", hovertemplate=deposit_hover))
    figure.update_layout(title="Cost vs Deposit Per Hari", barmode="group", xaxis=dict(type="category"), yaxis=dict(title=f"Cost ({currency_unit})"), yaxis2=dict(title=f"Deposit ({currency_unit})", overlaying="y", side="right", showgrid=False), legend=dict(orientation="h", y=1.1, x=0))
    return figure


def build_cost_to_deposit_ratio_figure(rows: list[dict]) -> go.Figure:
    daily_df = pd.DataFrame(rows or [])
    if daily_df.empty:
        figure = go.Figure()
        figure.update_layout(title="Cost To Deposit (%) Per Hari", annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        return figure
    daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
    daily_df["cost_to_revenue_pct"] = pd.to_numeric(daily_df.get("cost_to_revenue_pct", 0), errors="coerce").fillna(0.0)
    figure = go.Figure()
    figure.add_trace(go.Scatter(x=daily_df["date"].dt.strftime("%b %d\n%Y").tolist(), y=daily_df["cost_to_revenue_pct"].tolist(), mode="lines+markers", name="Cost To Deposit", line=dict(color="#ff6248", width=2), hovertemplate="<b>%{x}</b><br>Cost To Deposit: %{y:.2f}%<extra></extra>"))
    figure.update_layout(title="Cost To Deposit (%) Per Hari", xaxis=dict(type="category"), yaxis=dict(title="Percent", ticksuffix="%"), legend=dict(orientation="h", y=1.1, x=0))
    return figure
