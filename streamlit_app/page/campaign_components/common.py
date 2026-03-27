"""Shared helpers for campaign-oriented Streamlit pages."""

from __future__ import annotations

import datetime as dt

import streamlit as st

from streamlit_app.functions.dates import campaign_preset_ranges

PAGE_STYLE = """
<style>
.campaign-title {
    text-align: center;
    font-size: 3rem;
    font-weight: 800;
    margin-bottom: 1rem;
}
.metric-section-title {
    text-align: center;
    font-size: 2.6rem;
    font-weight: 800;
    margin: 1.6rem 0 1.1rem 0;
}
</style>
"""


def set_transparent_chart_background(figure):
    """Apply transparent background for non-table Plotly figures."""
    if not figure.data:
        return figure
    if any(getattr(trace, "type", "") == "table" for trace in figure.data):
        return figure
    figure.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return figure


def render_campaign_page_filters(
    *,
    page_title: str,
    period_key: str,
    date_range_key: str,
    source_key: str,
    source_label: str = "Performance Source",
) -> tuple[dt.date, dt.date, str]:
    """Render the shared date/source filter row used by campaign pages."""
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown(f'<div class="campaign-title">{page_title}</div>', unsafe_allow_html=True)

    source_options = {
        "Google Ads": "google",
        "Facebook Ads": "facebook",
        "TikTok Ads": "tiktok",
    }
    presets = campaign_preset_ranges(dt.date.today())
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]

    with st.container(border=True):
        filter_col, source_col = st.columns([2, 2], gap="small")
        with filter_col:
            selected_period = st.selectbox("Periods", options=list(presets.keys()), index=0, key=period_key)
            if selected_period == "Custom Range":
                selected = st.date_input("Select Date Range", key=date_range_key)
                if not isinstance(selected, tuple) or len(selected) != 2:
                    st.warning("Please select a valid date range.")
                    return None, None, None
                start_date, end_date = selected
            else:
                start_date, end_date = presets[selected_period]
                if st.session_state.get(date_range_key) != (start_date, end_date):
                    st.session_state[date_range_key] = (start_date, end_date)
        with source_col:
            selected_source = st.selectbox(source_label, options=list(source_options.keys()), index=0, key=source_key)

    return start_date, end_date, selected_source
