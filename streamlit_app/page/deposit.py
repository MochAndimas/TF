"""Deposit module.

This module is part of `streamlit_app.page` and contains runtime logic used by the
Traders Family application.
"""

import datetime as dt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from decouple import config

from streamlit_app.functions.utils import campaign_preset_ranges, fetch_data

USD_TO_IDR_RATE = config("USD_TO_IDR_RATE", default=16968, cast=float)

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
.currency-inline-label {
    font-size: 0.98rem;
    font-weight: 600;
    padding-top: 0.2rem;
    text-align: right;
}
.currency-inline-row {
    max-width: 320px;
    margin: 0 auto 1rem auto;
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


def _currency_multiplier(currency_unit: str) -> float:
    """Return multiplier used to display money in the selected currency."""
    if currency_unit == "IDR":
        return float(USD_TO_IDR_RATE)
    return 1.0


def _currency_label(currency_unit: str) -> str:
    """Return short label used in UI for the selected currency."""
    if currency_unit == "IDR":
        return "Rp"
    return "$"


def _compact_currency_value(value: float, currency_unit: str) -> str:
    """Format currency value compactly for cards to avoid truncation."""
    if currency_unit == "USD":
        return f"${value:,.0f}"

    absolute = abs(value)
    if absolute >= 1_000_000_000_000:
        compact = f"{value / 1_000_000_000_000:.1f}T"
    elif absolute >= 1_000_000_000:
        compact = f"{value / 1_000_000_000:.1f}B"
    elif absolute >= 1_000_000:
        compact = f"{value / 1_000_000:.1f}M"
    elif absolute >= 1_000:
        compact = f"{value / 1_000:.1f}K"
    else:
        compact = f"{value:,.0f}"
    compact = compact.rstrip("0").rstrip(".")
    return f"Rp{compact}"


def _format_amount(value: float | int, currency_unit: str = "USD") -> str:
    """Format numeric value into currency text.

    Args:
        value (float | int): Raw numeric amount.
        currency_unit (str): Display currency (`USD` or `IDR`).

    Returns:
        str: Currency text with thousands separators.
    """
    converted_value = float(value) * _currency_multiplier(currency_unit)
    return _compact_currency_value(converted_value, currency_unit)


def _format_qty(value: float | int) -> str:
    """Format quantity metric as integer with group separators.

    Args:
        value (float | int): Raw quantity value.

    Returns:
        str: Human-readable integer text.
    """
    return f"{int(float(value)):,}"


def _format_aov(value: float | int, currency_unit: str = "USD") -> str:
    """Format AOV metric as currency text.

    Args:
        value (float | int): Raw AOV numeric value.
        currency_unit (str): Display currency (`USD` or `IDR`).

    Returns:
        str: Currency text with group separators.
    """
    return _format_amount(value, currency_unit=currency_unit)


def _metric_formatter(metric_key: str, value: float | int, currency_unit: str = "USD") -> str:
    """Dispatch cell formatter based on report metric key.

    Args:
        metric_key (str): Metric identifier (`depo_amount`, `qty`, `aov`).
        value (float | int): Raw cell numeric value.
        currency_unit (str): Display currency (`USD` or `IDR`).

    Returns:
        str: Formatted text suitable for table rendering.
    """
    if metric_key == "depo_amount":
        return _format_amount(value, currency_unit=currency_unit)
    if metric_key == "qty":
        return _format_qty(value)
    return _format_aov(value, currency_unit=currency_unit)


def _extract_section_metric_values(section: dict[str, object], metric_key: str, timeline: list[str]) -> dict[str, dict[str, float]]:
    """Extract one metric's per-day status map from a section payload."""
    for row in section.get("rows", []):
        if str(row.get("key")) == metric_key:
            values = row.get("values", {})
            return {
                day: {
                    "new": float(values.get(day, {}).get("new", 0) or 0),
                    "existing": float(values.get(day, {}).get("existing", 0) or 0),
                }
                for day in timeline
            }
    return {day: {"new": 0.0, "existing": 0.0} for day in timeline}


def _build_daily_deposit_amount_figure(report: dict[str, object], currency_unit: str) -> go.Figure:
    """Build stacked daily deposit amount chart split by new vs existing users."""
    timeline = report.get("timeline", [])
    sections = report.get("sections", [])
    total_section = next((section for section in sections if str(section.get("campaign_id")) == "TOTAL"), None)
    if not timeline or total_section is None:
        figure = go.Figure()
        figure.update_layout(
            title="Daily Deposit Amount: New vs Existing",
            annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    amount_map = _extract_section_metric_values(total_section, "depo_amount", timeline)
    date_labels = [dt.date.fromisoformat(day).strftime("%b %d\n%Y") for day in timeline]
    multiplier = _currency_multiplier(currency_unit)
    new_values = [amount_map[day]["new"] * multiplier for day in timeline]
    existing_values = [amount_map[day]["existing"] * multiplier for day in timeline]
    currency_symbol = _currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=date_labels,
            y=new_values,
            name="New User",
            marker_color="#6176ff",
            hovertemplate=f"<b>%{{x}}</b><br>New: {currency_symbol} %{{y:,{decimals}}}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=date_labels,
            y=existing_values,
            name="Existing User",
            marker_color="#ff7a59",
            hovertemplate=f"<b>%{{x}}</b><br>Existing: {currency_symbol} %{{y:,{decimals}}}<extra></extra>",
        )
    )
    figure.update_layout(
        title="Daily Deposit Amount: New vs Existing",
        barmode="stack",
        xaxis=dict(type="category"),
        yaxis=dict(title=f"Deposit Amount ({currency_symbol})"),
        legend=dict(orientation="h", y=1.08, x=0),
        margin=dict(l=24, r=24, t=60, b=24),
    )
    return figure


def _build_daily_deposit_qty_aov_figure(report: dict[str, object], currency_unit: str) -> go.Figure:
    """Build combo chart for daily deposit quantity and AOV by user status."""
    timeline = report.get("timeline", [])
    sections = report.get("sections", [])
    total_section = next((section for section in sections if str(section.get("campaign_id")) == "TOTAL"), None)
    if not timeline or total_section is None:
        figure = go.Figure()
        figure.update_layout(
            title="Daily Deposit Qty + AOV",
            annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    qty_map = _extract_section_metric_values(total_section, "qty", timeline)
    aov_map = _extract_section_metric_values(total_section, "aov", timeline)
    date_labels = [dt.date.fromisoformat(day).strftime("%b %d\n%Y") for day in timeline]
    multiplier = _currency_multiplier(currency_unit)
    new_qty = [qty_map[day]["new"] for day in timeline]
    existing_qty = [qty_map[day]["existing"] for day in timeline]
    new_aov = [aov_map[day]["new"] * multiplier for day in timeline]
    existing_aov = [aov_map[day]["existing"] * multiplier for day in timeline]
    currency_symbol = _currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=date_labels,
            y=new_qty,
            name="New Qty",
            marker_color="#6176ff",
            offsetgroup="new_qty",
            hovertemplate="<b>%{x}</b><br>New Qty: %{y:,.0f}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=date_labels,
            y=existing_qty,
            name="Existing Qty",
            marker_color="#8ea0ff",
            offsetgroup="existing_qty",
            hovertemplate="<b>%{x}</b><br>Existing Qty: %{y:,.0f}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=date_labels,
            y=new_aov,
            mode="lines+markers",
            name="New AOV",
            yaxis="y2",
            line=dict(color="#22c55e", width=2),
            hovertemplate=f"<b>%{{x}}</b><br>New AOV: {currency_symbol} %{{y:,{decimals}}}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=date_labels,
            y=existing_aov,
            mode="lines+markers",
            name="Existing AOV",
            yaxis="y2",
            line=dict(color="#f97316", width=2, dash="dot"),
            hovertemplate=f"<b>%{{x}}</b><br>Existing AOV: {currency_symbol} %{{y:,{decimals}}}<extra></extra>",
        )
    )
    figure.update_layout(
        title="Daily Deposit Qty + AOV",
        xaxis=dict(type="category"),
        yaxis=dict(title="Deposit Qty"),
        yaxis2=dict(title=f"AOV ({currency_symbol})", overlaying="y", side="right"),
        legend=dict(orientation="h", y=1.12, x=0),
        margin=dict(l=24, r=24, t=68, b=24),
    )
    return figure


def _build_top_campaign_deposit_figure(report: dict[str, object], currency_unit: str, top_n: int = 10) -> go.Figure:
    """Build ranked campaign chart by total deposit amount."""
    timeline = report.get("timeline", [])
    sections = report.get("sections", [])
    campaign_sections = [section for section in sections if str(section.get("campaign_id")) != "TOTAL"]
    if not timeline or not campaign_sections:
        figure = go.Figure()
        figure.update_layout(
            title="Top Campaign by Deposit Amount",
            annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    rows: list[dict[str, object]] = []
    multiplier = _currency_multiplier(currency_unit)
    currency_symbol = _currency_label(currency_unit)
    decimals = ".2f" if currency_unit == "USD" else ".0f"
    for section in campaign_sections:
        amount_map = _extract_section_metric_values(section, "depo_amount", timeline)
        new_total = sum(amount_map[day]["new"] for day in timeline)
        existing_total = sum(amount_map[day]["existing"] for day in timeline)
        total = new_total + existing_total
        if total <= 0:
            continue
        label = str(section.get("title") or section.get("campaign_name") or "Unknown Campaign").strip()
        short_label = label if len(label) <= 44 else f"{label[:41]}..."
        rows.append(
            {
                "label": short_label,
                "new_total": new_total * multiplier,
                "existing_total": existing_total * multiplier,
                "total": total * multiplier,
            }
        )

    if not rows:
        figure = go.Figure()
        figure.update_layout(
            title="Top Campaign by Deposit Amount",
            annotations=[{"text": "No positive deposit data", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    ranked = pd.DataFrame(rows).sort_values("total", ascending=False).head(top_n).sort_values("total", ascending=True)
    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=ranked["new_total"].tolist(),
            y=ranked["label"].tolist(),
            orientation="h",
            name="New User",
            marker_color="#6176ff",
            hovertemplate=f"<b>%{{y}}</b><br>New: {currency_symbol} %{{x:,{decimals}}}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=ranked["existing_total"].tolist(),
            y=ranked["label"].tolist(),
            orientation="h",
            name="Existing User",
            marker_color="#ff7a59",
            hovertemplate=f"<b>%{{y}}</b><br>Existing: {currency_symbol} %{{x:,{decimals}}}<extra></extra>",
        )
    )
    figure.update_layout(
        title="Top Campaign by Deposit Amount",
        barmode="stack",
        xaxis=dict(title=f"Deposit Amount ({currency_symbol})"),
        yaxis=dict(title="Campaign"),
        legend=dict(orientation="h", y=1.08, x=0),
        margin=dict(l=24, r=24, t=60, b=24),
    )
    return figure


def _render_status_cards(
    title: str,
    totals: dict[str, float],
    growth: dict[str, float],
    currency_unit: str,
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
        (f"Depo Amount ({_currency_label(currency_unit)})", "depo_amount", _format_amount),
        ("Total Deposit (Qty)", "qty", _format_qty),
        (f"AOV ({_currency_label(currency_unit)})", "aov", _format_aov),
    ]
    for column, (label, key, formatter) in zip(columns, card_specs):
        with column:
            with st.container(border=True):
                st.metric(
                    label=label,
                    value=formatter(totals.get(key, 0.0), currency_unit=currency_unit)
                    if key in {"depo_amount", "aov"}
                    else formatter(totals.get(key, 0.0)),
                    delta=f"{growth.get(key, 0.0):+.2f}% vs prev period",
                )


def _render_metric_cards(report: dict[str, object], currency_unit: str) -> None:
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

    left_col, right_col = st.columns(2, gap="small")
    with left_col:
        _render_status_cards("New User", new_totals, new_growth, currency_unit=currency_unit)
    with right_col:
        _render_status_cards("Existing User", existing_totals, existing_growth, currency_unit=currency_unit)


def _render_report_table(report: dict[str, object], currency_unit: str) -> None:
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
            if metric_key == "depo_amount":
                metric_name = f"Depo Amount ({_currency_label(currency_unit)})"
            elif metric_key == "aov":
                metric_name = f"AOV ({_currency_label(currency_unit)})"
            metric_cells = [f'<td class="sticky-col metric-name">{metric_name}</td>']
            for day in timeline:
                day_data = values.get(day, {"new": 0, "existing": 0})
                metric_cells.append(
                    f"<td>{_metric_formatter(metric_key, day_data.get('new', 0), currency_unit=currency_unit)}</td>"
                )
                metric_cells.append(
                    f"<td>{_metric_formatter(metric_key, day_data.get('existing', 0), currency_unit=currency_unit)}</td>"
                )
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
    st.markdown('<div class="deposit-title">First Deposit Report</div>', unsafe_allow_html=True)

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
        if not st.session_state.get("access_token"):
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
    st.markdown('<div class="metric-section-title">First Deposit Summary</div>', unsafe_allow_html=True)
    currency_wrap_left, currency_wrap_mid, currency_wrap_right = st.columns([2.2, 2.6, 2.2], gap="small")
    with currency_wrap_mid:
        label_col, control_col = st.columns([1.1, 2.2], gap="small")
        with label_col:
            st.markdown('<div class="currency-inline-label">Currency</div>', unsafe_allow_html=True)
        with control_col:
            currency_unit = st.radio(
                "Currency",
                options=["USD", "IDR"],
                horizontal=True,
                key="deposit_currency_unit",
                label_visibility="collapsed",
            )
    _render_metric_cards(report, currency_unit=currency_unit)
    daily_amount_figure = _build_daily_deposit_amount_figure(report, currency_unit=currency_unit)
    qty_aov_figure = _build_daily_deposit_qty_aov_figure(report, currency_unit=currency_unit)
    top_campaign_figure = _build_top_campaign_deposit_figure(report, currency_unit=currency_unit)
    daily_amount_figure.update_layout(height=420, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    qty_aov_figure.update_layout(height=420, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    top_campaign_figure.update_layout(height=480, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")

    charts_left, charts_right = st.columns(2, gap="small")
    with charts_left:
        with st.container(border=True):
            st.plotly_chart(daily_amount_figure, width="stretch")
    with charts_right:
        with st.container(border=True):
            st.plotly_chart(qty_aov_figure, width="stretch")

    with st.container(border=True):
        st.plotly_chart(top_campaign_figure, width="stretch")
    _render_report_table(report, currency_unit=currency_unit)
