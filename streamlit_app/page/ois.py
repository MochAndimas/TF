"""Partnership / OIS: acquisition, engagement and campaign exploration."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from streamlit_app.functions.api import fetch_api_result
from streamlit_app.functions.comparison import select_comparison
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.functions.metrics import _render_metric_with_growth, _campaign_format_growth, _campaign_format_number
from streamlit_app.page.campaign_components.common import PAGE_STYLE
from streamlit_app.page.ois_components.analysis import LABELS, frame, totals
from streamlit_app.page.ois_components import charts
from streamlit_app.page.ois_components.registrations import show_register_revenue


def render_period_filter():
    """Use the same presets and comparison policy as the other analytics pages."""
    today = datetime.now(ZoneInfo("Asia/Jakarta")).date()
    presets = campaign_preset_ranges(today)
    st.session_state.setdefault("ois_dates", presets["This Month"])
    if st.session_state.get("ois_period") not in presets:
        st.session_state["ois_period"] = "This Month"
    with st.container(border=True):
        period = st.selectbox("Periods", list(presets), key="ois_period")
        select_comparison(period)
        if period == "Custom Range":
            dates = st.date_input("Select Date Range", key="ois_dates", max_value=today)
        else:
            dates = presets[period]
            st.session_state["ois_dates"] = dates
        if len(dates) != 2 or dates[0] > dates[1] or (dates[1] - dates[0]).days >= 366:
            st.warning("Select a complete date range of at most 366 days.")
            return None
    return dates


def render_chart(figure, key):
    """Give every chart the standard analytics panel and spacing."""
    with st.container(border=True):
        st.plotly_chart(figure, width="stretch", key=key)


def render_table(data):
    with st.container(border=True):
        st.dataframe(data.rename(columns=LABELS).style.format(precision=2, thousands=",", na_rep="—"),
                     hide_index=True, width="stretch")


async def show_ois_page(host: str) -> None:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">OIS</div>', unsafe_allow_html=True)
    dates = render_period_filter()
    if dates is None:
        return
    new_users = await show_performance(host, dates)
    st.divider()
    st.markdown("## Register & Revenue")
    await show_register_revenue(host, dates, new_users=new_users)


async def show_performance(host: str, dates) -> float | None:
    with st.spinner("Loading OIS analytics..."):
        result = await fetch_api_result(st=st, host=host, uri="ois/analytics", params={
            "start_date": dates[0].isoformat(), "end_date": dates[1].isoformat()})
    if not result.ok:
        st.error(result.message or "Unable to load OIS analytics.")
        return
    data = result.data
    current, previous = frame(data["current_rows"]), frame(data["previous_rows"])
    if current.empty:
        st.info("No OIS records for this period. Sync GA4 User Acquisition (OIS) in Update Data or choose another period.")
        return
    combined = pd.concat([current, previous], ignore_index=True)
    with st.container(border=True):
        selections = {}
        fields = ["first_user_source", "platform", "first_user_medium", "first_user_campaign_name"]
        for column, field in zip(st.columns(4, gap="small"), fields):
            options = sorted(combined[field].unique().tolist())
            key = f"ois_filter_{field}"
            # Keep selections valid when the reporting period changes.
            selected = [value for value in st.session_state.get(key, []) if value in options]
            st.session_state[key] = selected
            selections[field] = column.multiselect(
                LABELS[field], options, key=key, placeholder="All")
    for field, selected in selections.items():
        if selected:
            current = current[current[field].isin(selected)]
            previous = previous[previous[field].isin(selected)]
    if current.empty:
        st.info("No records match these filters.")
        return
    st.markdown("## OIS Overview")
    now, before = totals(current), totals(previous)
    summary = {"previous_period": {"start_date": data["previous_start"], "end_date": data["previous_end"]}}
    specs = ["new_users", "active_users", "engaged_sessions"]
    for column, field in zip(st.columns(3, gap="small"), specs):
        change = (now[field] / before[field] - 1) * 100 if before[field] else None
        with column, st.container(border=True):
            _render_metric_with_growth(st, LABELS[field], _campaign_format_number(now[field]),
                                       delta=_campaign_format_growth(change, summary))
    st.markdown("## Acquisition Performance")
    with st.container(border=True):
        field = st.selectbox("Performance Metric", ["new_users", "engaged_sessions"],
                             format_func=LABELS.get, key="ois_acquisition_metric")
    render_chart(charts.trend(current, field, *dates), "ois_trend")
    left, middle, right = st.columns(3, gap="small")
    with left:
        render_chart(charts.mix(current, field), "ois_mix")
    with middle:
        render_chart(charts.contribution(current, "first_user_campaign_name",
                     field, "Top Campaigns · up to 12"), "ois_campaign")
    with right:
        render_chart(charts.contribution(current, "first_user_medium",
                     field, "Medium Contribution"), "ois_medium")
    st.markdown("## Source Performance")
    source_metrics = ["new_users", "active_users", "total_users", "engaged_sessions"]
    source_summary = pd.DataFrame([{
        "first_user_source": "OIS",
        **{metric: float(current[metric].sum()) for metric in source_metrics},
    }])
    render_table(source_summary)
    with st.expander("Daily Traffic Details"):
        columns = ["date", "platform", "first_user_source", "first_user_medium",
                   "first_user_campaign_name", "new_users", "active_users", "total_users",
                   "engaged_sessions"]
        display = current.sort_values(["date", "first_user_source"], ascending=[False, True])[columns]
        labels = {**LABELS, "active_users": "Daily active users", "total_users": "Daily total users"}
        st.dataframe(display.rename(columns=labels), hide_index=True, width="stretch", column_config={
            "Date": st.column_config.DatetimeColumn(format="DD MMM YYYY")})
    return now["new_users"]
