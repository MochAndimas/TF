"""Form helpers for the update-data page."""

from __future__ import annotations

import datetime as dt

import streamlit as st

from streamlit_app.functions.dates import get_date_range

ALL_DATA_SOURCES_LABEL = "All Data Sources"

DATA_SOURCE_OPTIONS = {
    ALL_DATA_SOURCES_LABEL: "__all__",
    "Unique Campaign": "unique_campaign",
    "Google Ads (API)": "google_ads",
    "Facebook Ads (API)": "facebook_ads",
    "TikTok Ads (GSheet)": "tiktok_ads",
    "GA4 Daily Users (App/Web)": "ga4_daily_metrics",
    "Instagram Insights (API)": "instagram_insights",
    "Instagram Media Insights (Post/Reels)": "instagram_media_insights",
    "TikTok Insights (API)": "tiktok_insights",
    "TikTok Media Insights (Video)": "tiktok_media_insights",
    "YouTube Daily Insight (API)": "youtube_daily_insight",
    "YouTube Media Insight (Video/Short/Live)": "youtube_media_insight",
    "Facebook Page Insights (API)": "facebook_page_insights",
    "Facebook Page Media Insights (API)": "facebook_page_media_insights",
    "Daily Register (GSheet)": "daily_register",
    "Regis UTM Daily (All Regis GSheet)": "regis_utm_daily",
    "First Deposit (GSheet)": "first_deposit",
    "First Deposit BA (GSheet)": "first_deposit_ba",
    "MS Deposit (GSheet)": "ms_deposit",
    "Google Play Console Install Metrics": "play_console_install_metrics",
    "Apple App Store Request Report (ONE_TIME_SNAPSHOT)": "apple_report_request",
    "Apple App Store Install Metrics (ONGOING)": "apple_install",
}

ALL_DATA_SOURCE_VALUES = [
    "google_ads",
    "facebook_ads",
    "tiktok_ads",
    "unique_campaign",
    "ga4_daily_metrics",
    "instagram_insights",
    "instagram_media_insights",
    "tiktok_insights",
    "tiktok_media_insights",
    "youtube_daily_insight",
    "youtube_media_insight",
    "facebook_page_insights",
    "facebook_page_media_insights",
    "daily_register",
    "regis_utm_daily",
    "first_deposit",
    "first_deposit_ba",
    "ms_deposit",
    "play_console_install_metrics",
    "apple_install",
]


def data_source_options(*, snapshot_completed: bool = False) -> dict[str, str]:
    """Build source options for the update form."""
    options = dict(DATA_SOURCE_OPTIONS)
    options["Apple App Store Install Metrics (ONE_TIME_SNAPSHOT)"] = (
        "apple_install_snapshot"
    )
    return options


def date_presets(today: dt.date) -> dict[str, tuple[dt.date, dt.date]]:
    """Build the preset date windows shown in the update form."""
    yesterday = today - dt.timedelta(days=1)
    this_month_start = today.replace(day=1)
    last_month_start = (this_month_start - dt.timedelta(days=1)).replace(day=1)
    last_month_end = this_month_start - dt.timedelta(days=1)
    return {
        "Yesterday": (yesterday, yesterday),
        "Last 7 Days": (today - dt.timedelta(days=7), yesterday),
        "This Month": (this_month_start, yesterday),
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
        min_value=dt.date(2020, 1, 1),
        max_value=get_date_range(days=2, period="days")[1],
        key="update_date_range",
    )
    if not isinstance(selected_range, tuple) or len(selected_range) != 2:
        raise ValueError("Please select a start and end date.")
    from_date, to_date = selected_range
    if from_date > to_date:
        raise ValueError("Start date cannot be after end date.")
    return from_date, to_date


def render_update_form(*, snapshot_completed: bool = False) -> dict[str, object]:
    """Render the update-data form and return normalized selections."""
    presets = date_presets(dt.date.today())
    source_options = data_source_options(snapshot_completed=snapshot_completed)
    from_date = to_date = None
    with st.container(border=True):
        left_col, right_col = st.columns(2)
        with left_col:
            source_label = st.selectbox("Data Source", options=list(source_options.keys()), index=None, placeholder="Select a data source", key="update_data_source")
            selected_source = source_options.get(source_label or "")
            snapshot_selected = selected_source == "apple_install_snapshot"
            report_request_selected = selected_source == "apple_report_request"
            mode = st.radio(
                "Update Mode",
                options=["manual"] if snapshot_selected or report_request_selected else ["manual", "auto"],
                horizontal=True,
                key=(
                    "update_mode_apple_snapshot"
                    if snapshot_selected
                    else "update_mode_apple_report_request"
                    if report_request_selected
                    else "update_mode"
                ),
            )
        with right_col:
            preset_key = st.selectbox(
                "Date Preset",
                options=list(presets.keys()),
                index=0 if mode == "auto" else None,
                disabled=(mode == "auto" or report_request_selected),
                placeholder="Select date range preset",
                key="update_period",
            )
            if report_request_selected:
                from_date = to_date = dt.date.today()
                st.info("This only requests the Apple one-time analytics report. Reports usually need 24-48 hours before they can be loaded.")
            elif mode == "auto":
                lag_days = (
                    5
                    if source_label == "Apple App Store Install Metrics (ONGOING)"
                    else 1
                )
                auto_date = dt.date.today() - dt.timedelta(days=lag_days)
                suffix = " (Apple completeness window)" if lag_days == 5 else ""
                st.info(f"Auto mode uses date: `{auto_date.isoformat()}`{suffix}")
            elif preset_key:
                try:
                    from_date, to_date = resolve_date_input(mode, preset_key, presets)
                except ValueError as error:
                    st.warning(str(error))
        submitted = st.button("Run Update", type="primary", width="stretch", key="update_submit")
    return {
        "submitted": submitted,
        "source_label": source_label,
        "source_options": source_options,
        "mode": mode,
        "preset_key": preset_key,
        "presets": presets,
        "from_date": from_date,
        "to_date": to_date,
    }
