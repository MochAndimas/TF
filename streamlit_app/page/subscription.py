"""Subscription revenue analytics using the shared analytics visual style."""
from __future__ import annotations

from streamlit_app.functions.comparison import select_comparison, comparison_cache_key

import datetime as dt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.functions.api import fetch_api_result
from streamlit_app.functions.metrics import _campaign_format_growth
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.page.campaign_components.common import PAGE_STYLE, set_transparent_chart_background

LABELS = {
    "total_subscription_amount": "Total Subscription Revenue",
    "new_subscription_amount": "New Subscription Revenue",
    "total_subscription_qty": "Total Subscription Qty",
    "new_subscription_qty": "New Subscription Qty",
    "total_subscribers": "Total Subscribers",
    "new_subscribers": "New Subscribers",
}
COLORS = ("#636EFA", "#00CC96")


def render_filters():
    presets = campaign_preset_ranges(dt.date.today())
    st.session_state.setdefault("subscription_period", "This Month")
    st.session_state.setdefault("subscription_date_range", presets["This Month"])
    with st.container(border=True):
        selected = st.selectbox("Periods", list(presets), key="subscription_period")
        select_comparison(selected)
        if selected == "Custom Range":
            dates = st.date_input("Select Date Range", key="subscription_date_range")
            if not isinstance(dates, tuple) or len(dates) != 2:
                st.warning("Please select a valid date range.")
                return None
        else:
            dates = presets[selected]
            st.session_state["subscription_date_range"] = dates
    if dates[0] > dates[1]:
        st.warning("Start date cannot be after end date.")
        return None
    return dates


def build_daily_figure(frame, fields, title, *, currency=False):
    figure = go.Figure()
    for field, color in zip(fields, COLORS):
        figure.add_trace(go.Scatter(
            x=frame["date"], y=frame[field], name=LABELS[field],
            mode="lines+markers", line={"color": color}, connectgaps=False,
            hovertemplate=("Rp %{y:,.2f}" if currency else "%{y:,.0f}") + "<extra>%{fullData.name}</extra>",
        ))
    figure.update_layout(
        title=title, height=380, hovermode="x unified",
        margin={"l": 20, "r": 20, "t": 60, "b": 65},
        legend={"orientation": "h", "y": -0.22, "x": 0},
        xaxis={"title": None},
        yaxis={"title": "Revenue (IDR)" if currency else "Count", "rangemode": "tozero"},
    )
    return set_transparent_chart_background(figure)


def render_report(data):
    rows = data.get("daily_rows", [])
    if not rows:
        st.info("No subscription data for the selected period. Update All Subscription from the Update Data page.")
        return
    metrics = data["metrics"]
    growth = data.get("growth_percentage", {})
    st.markdown('<div class="metric-section-title">Subscription Summary</div>', unsafe_allow_html=True)
    for fields in (list(LABELS)[:2], list(LABELS)[2:]):
        for column, field in zip(st.columns(len(fields), gap="small"), fields):
            with column, st.container(border=True):
                value = metrics.get(field)
                amount = field.endswith("amount")
                display = "—" if value is None else (f"Rp {value:,.0f}" if amount else f"{value:,.0f}")
                latest = field.endswith("subscribers")
                delta = growth.get(field)
                st.metric(
                    LABELS[field] + (" (Latest Day)" if latest else ""), display,
                    delta=_campaign_format_growth(delta, {"previous_period": {"start_date": data.get("previous_start_date"), "end_date": data.get("previous_end_date")}}) if delta is not None else None,
                )
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"])
    # Preserve missing dates as gaps instead of inventing zero activity.
    daily = frame.set_index("date").reindex(pd.date_range(data["start_date"], data["end_date"])).rename_axis("date").reset_index()
    st.markdown('<div class="metric-section-title">Subscription Trends</div>', unsafe_allow_html=True)
    specs = [
        (("total_subscription_amount", "new_subscription_amount"), "Daily Subscription Revenue", True),
        (("total_subscription_qty", "new_subscription_qty"), "Daily Subscription Quantity", False),
    ]
    for column, (fields, title, currency) in zip(st.columns(2, gap="small"), specs):
        with column, st.container(border=True):
            st.plotly_chart(build_daily_figure(daily, fields, title, currency=currency), width="stretch")
    with st.container(border=True):
        st.plotly_chart(build_daily_figure(daily, ("total_subscribers", "new_subscribers"), "Daily Subscribers"), width="stretch")
    st.markdown('<div class="metric-section-title">Subscription Daily Details</div>', unsafe_allow_html=True)
    details = frame[["date", *LABELS]].rename(columns={"date": "Date", **LABELS})
    details["Date"] = details["Date"].dt.date
    with st.container(border=True):
        styled_details = details.sort_values("Date", ascending=False).style.format({
            label: "Rp {:,.2f}" for field, label in LABELS.items() if field.endswith("amount")
        })
        st.dataframe(styled_details, hide_index=True, width="stretch", column_config={
            "Date": st.column_config.DateColumn(format="DD MMM YYYY"),
            **{label: st.column_config.NumberColumn(format="%d") for field, label in LABELS.items() if not field.endswith("amount")},
        })


async def show_subscription_page(host: str) -> None:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Subscription</div>', unsafe_allow_html=True)
    dates = render_filters()
    if dates is None:
        return
    start_date, end_date = dates
    with st.spinner("Fetching subscription analytics..."):
        result = await fetch_api_result(
            st=st, host=host, uri="subscription/analytics",
            params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )
    if not result.ok:
        st.error(result.message or "Failed to fetch subscription analytics.")
        return
    render_report(result.data)
