"""Deposit module.

This module is part of `streamlit_app.page` and contains runtime logic used by the
Traders Family application.
"""

import datetime as dt

import streamlit as st

from streamlit_app.functions.utils import campaign_preset_ranges, fetch_data, get_user

PAGE_STYLE = """
<style>
.deposit-title {
    text-align: center;
    font-size: 2.8rem;
    font-weight: 800;
    margin-bottom: 1rem;
}
.deposit-table-wrap {
    width: 100%;
    overflow-x: auto;
    border: 1px solid rgba(120, 140, 170, 0.32);
    border-radius: 10px;
}
.deposit-table {
    border-collapse: collapse;
    width: max-content;
    min-width: 100%;
    font-size: 0.96rem;
}
.deposit-table th,
.deposit-table td {
    border: 1px solid rgba(120, 140, 170, 0.28);
    padding: 7px 10px;
    white-space: nowrap;
}
.deposit-table thead th {
    background: rgba(43, 63, 92, 0.32);
    text-align: center;
}
.sticky-col {
    position: sticky;
    left: 0;
    z-index: 2;
    background: rgb(15, 23, 42);
}
.section-head td {
    background: rgba(37, 99, 235, 0.25);
    font-weight: 700;
    border-top: 2px solid rgba(96, 165, 250, 0.9);
}
.metric-name {
    font-weight: 600;
}
.metric-section-title {
    text-align: center;
    font-size: 2.2rem;
    font-weight: 800;
    margin: 1.2rem 0 0.8rem 0;
}
.deposit-group-title {
    text-align: center;
    font-size: 1.9rem;
    font-weight: 700;
    margin: 1.0rem 0 0.5rem 0;
}
div[data-testid="stMetricLabel"] > div {
    font-size: 1.05rem !important;
}
div[data-testid="stMetricValue"] > div {
    font-size: 2.1rem !important;
    line-height: 1.1 !important;
}
div[data-testid="stMetricDelta"] > div {
    font-size: 0.9rem !important;
}
</style>
"""


def _format_amount(value: float | int) -> str:
    """Format numeric value into USD amount text.

    Args:
        value (float | int): Raw numeric amount.

    Returns:
        str: Currency text with dollar sign and thousands separators.
    """
    return f"${float(value):,.0f}"


def _format_qty(value: float | int) -> str:
    """Format quantity metric as integer with group separators.

    Args:
        value (float | int): Raw quantity value.

    Returns:
        str: Human-readable integer text.
    """
    return f"{int(float(value)):,}"


def _format_aov(value: float | int) -> str:
    """Format AOV metric as USD currency text.

    Args:
        value (float | int): Raw AOV numeric value.

    Returns:
        str: Currency text with dollar sign and two decimals.
    """
    return f"${float(value):,.0f}"


def _metric_formatter(metric_key: str, value: float | int) -> str:
    """Dispatch cell formatter based on report metric key.

    Args:
        metric_key (str): Metric identifier (`depo_amount`, `qty`, `aov`).
        value (float | int): Raw cell numeric value.

    Returns:
        str: Formatted text suitable for table rendering.
    """
    if metric_key == "depo_amount":
        return _format_amount(value)
    if metric_key == "qty":
        return _format_qty(value)
    return _format_aov(value)


def _render_status_cards(
    title: str,
    totals: dict[str, float],
    growth: dict[str, float],
) -> None:
    """Render three KPI cards for one user status bucket.

    Args:
        title (str): Group title shown above cards (e.g., New User).
        totals (dict[str, float]): Current-period totals by metric key.
        growth (dict[str, float]): Growth percentages vs previous period.

    Returns:
        None: Renders Streamlit components as side effects.
    """
    st.markdown(f'<div class="deposit-group-title">{title}</div>', unsafe_allow_html=True)
    columns = st.columns(3, gap="small")
    card_specs = [
        ("Depo Amount ($)", "depo_amount", _format_amount),
        ("Total Deposit (Qty)", "qty", _format_qty),
        ("AOV ($)", "aov", _format_aov),
    ]
    for column, (label, key, formatter) in zip(columns, card_specs):
        with column:
            with st.container(border=True):
                st.metric(
                    label=label,
                    value=formatter(totals.get(key, 0.0)),
                    delta=f"{growth.get(key, 0.0):+.2f}% vs prev period",
                )


def _render_metric_cards(report: dict[str, object]) -> None:
    """Render top summary card section from report payload.

    Args:
        report (dict[str, object]): Report payload from API endpoint.

    Returns:
        None: Renders two card groups (new/existing users).
    """
    summary = report.get("summary", {})
    if not summary:
        return

    totals = summary.get("current_period", {}).get("totals", {})
    growth = summary.get("growth_percentage", {})
    new_totals = totals.get("new", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0})
    existing_totals = totals.get("existing", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0})
    new_growth = growth.get("new", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0})
    existing_growth = growth.get("existing", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0})

    st.markdown('<div class="metric-section-title">Deposit Summary</div>', unsafe_allow_html=True)
    left_col, right_col = st.columns(2, gap="small")
    with left_col:
        _render_status_cards("New User", new_totals, new_growth)
    with right_col:
        _render_status_cards("Existing User", existing_totals, existing_growth)


def _render_report_table(report: dict[str, object]) -> None:
    """Render horizontally-scrollable deposit cross-tab table.

    Args:
        report (dict[str, object]): Report payload containing timeline and
            campaign sections.

    Returns:
        None: Writes HTML table markup into Streamlit container.
    """
    timeline = report.get("timeline", [])
    sections = report.get("sections", [])
    if not timeline or not sections:
        st.info("No deposit data for selected date range.")
        return

    header_cells = ['<th class="sticky-col" rowspan="2">Metric</th>']
    for day in timeline:
        day_label = dt.date.fromisoformat(day).strftime("%b-%d-%Y")
        header_cells.append(f'<th colspan="2">{day_label}</th>')

    sub_header_cells = []
    for _ in timeline:
        sub_header_cells.append("<th>New</th><th>Existing</th>")

    body_rows: list[str] = []
    total_columns = 1 + (2 * len(timeline))
    for section in sections:
        title = str(section.get("title", ""))
        campaign_id = str(section.get("campaign_id", ""))
        if campaign_id and campaign_id != "TOTAL":
            section_label = f"{title} | Campaign ID: {campaign_id}"
        else:
            section_label = title or "TOTAL"

        body_rows.append(f'<tr class="section-head"><td colspan="{total_columns}" class="sticky-col">{section_label}</td></tr>')
        for row in section.get("rows", []):
            metric_name = str(row.get("metric", "-"))
            metric_key = str(row.get("key", ""))
            values = row.get("values", {})
            metric_cells = [f'<td class="sticky-col metric-name">{metric_name}</td>']
            for day in timeline:
                day_data = values.get(day, {"new": 0, "existing": 0})
                metric_cells.append(f"<td>{_metric_formatter(metric_key, day_data.get('new', 0))}</td>")
                metric_cells.append(f"<td>{_metric_formatter(metric_key, day_data.get('existing', 0))}</td>")
            body_rows.append(f"<tr>{''.join(metric_cells)}</tr>")

    table_html = f"""
    <div class="deposit-table-wrap">
      <table class="deposit-table">
        <thead>
          <tr>{''.join(header_cells)}</tr>
          <tr>{''.join(sub_header_cells)}</tr>
        </thead>
        <tbody>
          {''.join(body_rows)}
        </tbody>
      </table>
    </div>
    """
    st.markdown(table_html, unsafe_allow_html=True)


async def show_deposit_page(host: str) -> None:
    """Render Deposit Daily Report page and handle API synchronization.

    Args:
        host (str): API base host used for backend requests.

    Returns:
        None: Renders page components as side effects.
    """
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="deposit-title">Deposit Daily Report</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    type_options = {
        "All": "all",
        "User Acquisition": "user_acquisition",
        "Brand Awareness": "brand_awareness",
    }
    date_range_key = "deposit_date_range"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]
    with st.container(border=True):
        left_col, right_col = st.columns([2, 2], gap="small")
        with left_col:
            period_key = st.selectbox(
                "Periods",
                options=list(presets.keys()),
                index=0,
                key="deposit_period",
            )
            if period_key == "Custom Range":
                selected = st.date_input(
                    "Select Date Range",
                    key=date_range_key,
                )
                if not isinstance(selected, tuple) or len(selected) != 2:
                    st.warning("Please select a valid date range.")
                    return
                start_date, end_date = selected
            else:
                start_date, end_date = presets[period_key]
                if st.session_state.get(date_range_key) != (start_date, end_date):
                    st.session_state[date_range_key] = (start_date, end_date)

        with right_col:
            selected_type_label = st.selectbox(
                "Campaign Type",
                options=list(type_options.keys()),
                index=0,
                key="deposit_campaign_type",
            )

    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_type = type_options[selected_type_label]
    selected_range = (start_date, end_date, selected_type)
    should_fetch = (
        "deposit_daily_payload" not in st.session_state
        or st.session_state.get("deposit_daily_range") != selected_range
    )

    if should_fetch:
        token_data = get_user(st.session_state._user_id)
        if token_data is None or not getattr(token_data, "access_token", None):
            st.error("Session invalid. Please log in again.")
            return

        with st.spinner("Fetching deposit report..."):
            response = await fetch_data(
                st=st,
                host=host,
                uri="deposit/daily-report",
                method="GET",
                params={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "campaign_type": selected_type,
                },
            )

        if not isinstance(response, dict):
            st.error("Invalid response from deposit report endpoint.")
            return
        if not response.get("success", False):
            st.error(response.get("detail") or response.get("message") or "Failed to fetch deposit report.")
            return

        st.session_state["deposit_daily_payload"] = response
        st.session_state["deposit_daily_range"] = selected_range

    payload = st.session_state.get("deposit_daily_payload", {})
    data = payload.get("data", {})
    report = data.get("report", {})
    _render_metric_cards(report)
    _render_report_table(report)
