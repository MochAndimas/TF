import datetime as dt

import streamlit as st

from streamlit_app.functions.utils import (
    campaign_figure_from_payload,
    campaign_preset_ranges,
    fetch_data,
    get_user,
    render_campaign_metric_cards,
)

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


async def show_user_acquisition_page(host: str) -> None:
    """Render campaign ads dashboard page."""
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">User Acquisition</div>', unsafe_allow_html=True)

    source_options = {
        "Google Ads": "google",
        "Facebook Ads": "facebook",
        "TikTok Ads": "tiktok",
    }
    presets = campaign_preset_ranges(dt.date.today())
    with st.container(border=True):
        with st.form("campaign_ads_filters", border=False):
            filter_col, source_col = st.columns([2, 2], gap="small")
            with filter_col:
                period_key = st.selectbox(
                    "Periods",
                    options=list(presets.keys()),
                    index=0,
                    key="campaign_ads_period",
                )

                if period_key == "Custom Range":
                    selected = st.date_input(
                        "Select Date Range",
                        value=presets["Last 7 Day"],
                        key="campaign_ads_custom_range",
                    )
                    if not isinstance(selected, tuple) or len(selected) != 2:
                        st.warning("Please select a valid date range.")
                        return
                    start_date, end_date = selected
                else:
                    start_date, end_date = presets[period_key]

            with source_col:
                selected_source = st.selectbox(
                    "Performance Source",
                    options=list(source_options.keys()),
                    index=0,
                    key="campaign_ads_source",
                )

            apply_filter = st.form_submit_button("Apply Filters", type="primary")

    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    should_fetch = (
        apply_filter
        or "campaign_ads_payload" not in st.session_state
    )

    if should_fetch:
        token_data = get_user(st.session_state._user_id)
        if token_data is None or not getattr(token_data, "access_token", None):
            st.error("Session invalid. Please log in again.")
            return

        with st.spinner("Fetching data..."):
            response = await fetch_data(
                st=st,
                host=host,
                uri="campaign",
                method="GET",
                params={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "chart": "both",
                },
            )

        if not isinstance(response, dict):
            st.error("Invalid response from campaign overview endpoint.")
            return
        if not response.get("success", False):
            st.error(response.get("detail") or response.get("message") or "Failed to fetch campaign overview.")
            return

        st.session_state["campaign_ads_payload"] = response
        st.session_state["campaign_ads_range"] = (start_date, end_date)

    payload = st.session_state.get("campaign_ads_payload", {})
    overview_data = payload.get("data", {})
    leads_by_source = overview_data.get("leads_by_source", {})
    ads_metrics = overview_data.get("ads_metrics_with_growth", {})

    table_payload = leads_by_source.get("table", {}).get("figure")
    pie_payload = leads_by_source.get("pie", {}).get("figure")
    table_figure = campaign_figure_from_payload(table_payload, "Leads Source")
    pie_figure = campaign_figure_from_payload(pie_payload, "Leads Source")
    table_figure.update_layout(height=460)
    pie_figure.update_layout(height=460)

    left_col, right_col = st.columns(2, gap="small")
    with left_col:
        with st.container(border=True):
            st.plotly_chart(table_figure, width="stretch")
    with right_col:
        with st.container(border=True):
            st.plotly_chart(pie_figure, width="stretch")

    selected_key = source_options[selected_source]
    selected_metrics = ads_metrics.get(selected_key, {})
    render_campaign_metric_cards(st, selected_metrics, selected_source)


async def show_campaign_ads_page(host: str) -> None:
    """Backward-compatible alias for previous page handler name."""
    await show_user_acquisition_page(host)
