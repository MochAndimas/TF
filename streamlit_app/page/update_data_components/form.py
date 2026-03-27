"""Form helpers for the update-data page."""

from __future__ import annotations

import datetime as dt

import streamlit as st

from streamlit_app.functions.dates import get_date_range

DATA_SOURCE_OPTIONS = {
    "Unique Campaign": "unique_campaign",
    "Google Ads (API)": "google_ads",
    "Facebook Ads (API)": "facebook_ads",
    "TikTok Ads (GSheet)": "tiktok_ads",
    "GA4 Daily Users (App/Web)": "ga4_daily_metrics",
    "First Deposit (API)": "first_deposit",
}


def date_presets(today: dt.date) -> dict[str, tuple[dt.date, dt.date]]:
    """Build the preset date windows shown in the update form."""
    yesterday = today - dt.timedelta(days=1)
    this_month_start = today.replace(day=1)
    last_month_start = (this_month_start - dt.timedelta(days=1)).replace(day=1)
    last_month_end = this_month_start - dt.timedelta(days=1)
    return {
        "Yesterday": (yesterday, yesterday),
        "Last 7 Days": (today - dt.timedelta(days=7), yesterday),
        "This Month": (this_month_start, today),
        "Last Month": (last_month_start, last_month_end),
        "Custom Range": None,
    }


def resolve_date_input(mode: str, preset_key: str, presets: dict[str, tuple[dt.date, dt.date]]) -> tuple[dt.date, dt.date]:
    """Resolve the effective date range for an ETL update request."""
    if mode == "auto":
        yesterday = dt.date.today() - dt.timedelta(days=1)
        return yesterday, yesterday
    if preset_key != "Custom Range":
        return presets[preset_key]

    selected_range = st.date_input(
        "Select Date Range",
        value=get_date_range(days=7, period="days"),
        min_value=dt.date(2022, 1, 1),
        max_value=get_date_range(days=2, period="days")[1],
        key="update_date_range",
    )
    if not isinstance(selected_range, tuple) or len(selected_range) != 2:
        raise ValueError("Please select a start and end date.")
    from_date, to_date = selected_range
    if from_date > to_date:
        raise ValueError("Start date cannot be after end date.")
    return from_date, to_date


def render_update_form() -> dict[str, object]:
    """Render the update-data form and return normalized selections."""
    presets = date_presets(dt.date.today())
    from_date = to_date = None
    with st.container(border=True):
        left_col, right_col = st.columns(2)
        with left_col:
            source_label = st.selectbox("Data Source", options=list(DATA_SOURCE_OPTIONS.keys()), index=None, placeholder="Select a data source", key="update_data_source")
            mode = st.radio("Update Mode", options=["manual", "auto"], horizontal=True, key="update_mode")
        with right_col:
            preset_key = st.selectbox("Date Preset", options=list(presets.keys()), index=0 if mode == "auto" else None, disabled=(mode == "auto"), placeholder="Select date range preset", key="update_period")
            if mode == "auto":
                auto_date = dt.date.today() - dt.timedelta(days=1)
                st.info(f"Auto mode uses date: `{auto_date.isoformat()}`")
            elif preset_key:
                try:
                    from_date, to_date = resolve_date_input(mode, preset_key, presets)
                except ValueError as error:
                    st.warning(str(error))
        submitted = st.button("Run Update", type="primary", width="stretch", key="update_submit")
    return {"submitted": submitted, "source_label": source_label, "mode": mode, "preset_key": preset_key, "presets": presets, "from_date": from_date, "to_date": to_date}
