"""Shared revenue cards for social media pages."""

from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from streamlit_app.functions.metrics import _render_metric_with_growth, _campaign_format_growth
from streamlit_app.page.campaign_components.common import set_transparent_chart_background


def revenue_comparison_for_period(period: str | None) -> str:
    return {"This Month": "month_to_date", "Last Month": "previous_month"}.get(period, "previous_period")


def _daily_revenue_frame(metrics: dict) -> pd.DataFrame:
    rows = metrics.get("daily_rows", [])
    period = metrics.get("current_period", {})
    start_date = period.get("start_date")
    end_date = period.get("end_date")
    if not start_date or not end_date:
        return pd.DataFrame(columns=["date", "register", "first_deposit_qty", "register_to_deposit"])

    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = pd.DataFrame(columns=["date", "register", "first_deposit_qty"])
    else:
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.set_index("date")
    frame = frame.reindex(pd.date_range(start_date, end_date), fill_value=0).rename_axis("date").reset_index()
    for column in ("register", "first_deposit_qty"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)
    frame["register_to_deposit"] = (
        frame["first_deposit_qty"].div(frame["register"].replace(0, float("nan"))).mul(100).fillna(0)
    )
    return frame


def _registration_deposit_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    for column, label, color in (
        ("register", "Register", "#6875f5"),
        ("first_deposit_qty", "First Deposit Qty", "#22b895"),
    ):
        figure.add_trace(go.Scatter(
            x=frame["date"], y=frame[column], name=label, mode="lines+markers",
            line={"color": color, "width": 2},
            hovertemplate="%{x|%d %b %Y}<br>%{y:,.0f}<extra>%{fullData.name}</extra>",
        ))
    figure.update_layout(
        title="Daily Register & First Deposit",
        height=370,
        hovermode="x unified",
        margin={"l": 20, "r": 20, "t": 60, "b": 105},
        legend={"orientation": "h", "x": 0, "y": -0.3},
        xaxis={"tickformat": "%d %b", "automargin": True},
        yaxis={"title": "Count", "rangemode": "tozero"},
    )
    return set_transparent_chart_background(figure)


def _conversion_figure(frame: pd.DataFrame) -> go.Figure:
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    figure.add_trace(go.Bar(
        x=frame["date"], y=frame["register"], name="Register",
        marker_color="#6875f5",
        hovertemplate="%{x|%d %b %Y}<br>%{y:,.0f}<extra>%{fullData.name}</extra>",
    ), secondary_y=False)
    figure.add_trace(go.Bar(
        x=frame["date"], y=frame["first_deposit_qty"], name="First Deposit Qty",
        marker_color="#22b895",
        hovertemplate="%{x|%d %b %Y}<br>%{y:,.0f}<extra>%{fullData.name}</extra>",
    ), secondary_y=False)
    figure.add_trace(go.Scatter(
        x=frame["date"], y=frame["register_to_deposit"], name="Register to Deposit %",
        mode="lines+markers", line={"color": "#e6ae54", "width": 3},
        hovertemplate="%{x|%d %b %Y}<br>%{y:.2f}%<extra>%{fullData.name}</extra>",
    ), secondary_y=True)
    figure.update_layout(
        title="Daily Register to Deposit Conversion",
        height=370,
        barmode="group",
        hovermode="x unified",
        margin={"l": 20, "r": 20, "t": 60, "b": 105},
        legend={"orientation": "h", "x": 0, "y": -0.3},
        xaxis={"tickformat": "%d %b", "automargin": True},
    )
    figure.update_yaxes(title_text="Count", rangemode="tozero", secondary_y=False)
    figure.update_yaxes(title_text="Conversion (%)", rangemode="tozero", ticksuffix="%", secondary_y=True)
    return set_transparent_chart_background(figure)


def render_revenue(metrics: dict | None) -> None:
    st.markdown("## Revenue")
    if metrics is None:
        st.info("Revenue data is unavailable. Please reload the page.")
        return
    current = metrics.get("current_period", {}).get("metrics", {})
    growth = metrics.get("growth_percentage", {})
    previous = metrics.get("previous_period", {})
    comparison_label = "from last period"
    if previous.get("start_date") and previous.get("end_date"):
        start = date.fromisoformat(previous["start_date"])
        end = date.fromisoformat(previous["end_date"])
        comparison_label = f"vs {start:%d %b %Y} – {end:%d %b %Y}"
    specs = [
        ("Register", "register", f'{current.get("register", 0):,}'),
        ("First Deposit Qty", "first_deposit_qty", f'{current.get("first_deposit_qty", 0):,}'),
        ("First Deposit", "first_deposit", f'$ {current.get("first_deposit", 0):,.2f}'),
        ("Register to Deposit %", "register_to_deposit", f'{current.get("register_to_deposit", 0):.2f}%'),
    ]
    for column, (label, key, value) in zip(st.columns(4, gap="small"), specs):
        with column:
            with st.container(border=True):
                change = growth.get(key, 0.0)
                _render_metric_with_growth(st, label, value, delta=_campaign_format_growth(change).replace("from last period", comparison_label),
                          delta_color="off" if change == 0 else "normal")

    daily_frame = _daily_revenue_frame(metrics)
    for column, figure in zip(
        st.columns(2, gap="small"),
        [_registration_deposit_figure(daily_frame), _conversion_figure(daily_frame)],
    ):
        with column, st.container(border=True):
            st.plotly_chart(figure, width="stretch")
