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
from streamlit_app.page.ois_components.analysis import LABELS, aggregate, frame, totals
from streamlit_app.page.ois_components import charts


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
    view = st.segmented_control(
        "OIS view", ["Performance", "Register & revenue"], default="Performance", key="ois_view")
    if view == "Register & revenue":
        from streamlit_app.page.ois_components.registrations import show_register_revenue
        await show_register_revenue(host, dates)
        return
    await show_performance(host, dates)


async def show_performance(host: str, dates) -> None:
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
    specs = ["new_users", "active_users", "engaged_sessions",
             "event_count", "key_events", "user_engagement_duration"]
    for row in (specs[:3], specs[3:]):
        for column, field in zip(st.columns(3, gap="small"), row):
            change = (now[field] / before[field] - 1) * 100 if before[field] else None
            with column, st.container(border=True):
                _render_metric_with_growth(st, LABELS[field], _campaign_format_number(now[field]),
                                           delta=_campaign_format_growth(change, summary))
    st.markdown("## Acquisition Performance")
    with st.container(border=True):
        field = st.selectbox("Performance Metric", ["new_users", "engaged_sessions", "event_count", "key_events"],
                             format_func=LABELS.get, key="ois_acquisition_metric")
    render_chart(charts.trend(current, field, *dates), "ois_trend")
    left, right = st.columns(2, gap="small")
    with left:
        render_chart(charts.mix(current, field), "ois_mix")
    with right:
        render_chart(charts.contribution(current, "platform", field,
                     "Platform Contribution"), "ois_platform_chart")
    render_chart(charts.contribution(current, "first_user_campaign_name",
                 field, "Top Campaigns · up to 12"), "ois_campaign")
    left, right = st.columns(2, gap="small")
    with left:
        render_chart(charts.contribution(current, "first_user_medium",
                     field, "Medium Contribution"), "ois_medium")
    with right:
        render_chart(charts.heatmap(current, field), "ois_weekday")
    st.markdown("## Source Performance")
    render_table(aggregate(current, ["first_user_source"]))
    st.markdown("## Engagement & Activity")
    with st.container(border=True):
        field = st.selectbox("Activity metric", ["active_users", "total_users", "user_engagement_duration",
                                                 "engagement_seconds_per_active_user_day", "sessions_per_active_user_day"],
                             format_func=LABELS.get, key="ois_engagement_metric")
    left, right = st.columns(2, gap="small")
    with left:
        render_chart(charts.trend(current, field, *dates), "ois_engagement_trend")
    with right:
        render_chart(charts.quality(current), "ois_quality")
    st.markdown("## Performance Breakdown")
    with st.container(border=True):
        dimension = st.selectbox("Group by", ["first_user_campaign_name", "platform", "first_user_medium", "first_user_source"],
                                 format_func=LABELS.get, key="ois_grouping")
        grouped = aggregate(current, [dimension]).sort_values("new_users", ascending=False)
    render_table(grouped)
    st.markdown("## Daily Details")
    with st.container(border=True):
        display = current.sort_values(["date", "first_user_source"], ascending=[False, True]).copy()
        display["average_engagement_seconds"] = display.user_engagement_duration / \
            display.active_users.replace(0, float("nan"))
        display["engaged_sessions_per_active_user"] = display.engaged_sessions / \
            display.active_users.replace(0, float("nan"))
        labels = {**LABELS, "active_users": "Daily active users", "total_users": "Daily total users",
                  "average_engagement_seconds": "Avg engagement / active user (s)",
                  "engaged_sessions_per_active_user": "Engaged sessions / active user"}
        st.dataframe(display.rename(columns=labels), hide_index=True, width="stretch", column_config={
            "User key event rate": st.column_config.NumberColumn(format="percent"),
            "Date": st.column_config.DatetimeColumn(format="DD MMM YYYY")})
