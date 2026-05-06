"""Rendering helpers for the deposit page."""

from __future__ import annotations

import datetime as dt

import streamlit as st

from streamlit_app.functions.metrics import _render_hover_metric_card
from streamlit_app.page.deposit_components.formatting import (
    currency_label,
    format_amount,
    format_amount_full,
    format_aov,
    format_qty,
    metric_formatter,
)


def render_status_cards(title: str, totals: dict[str, float], growth: dict[str, float], currency_unit: str) -> None:
    st.markdown(f'<div class="deposit-group-title">{title}</div>', unsafe_allow_html=True)
    card_specs = [(f"Depo Amount ({currency_label(currency_unit)})", "depo_amount", format_amount), ("Deposit (Qty)", "qty", format_qty), (f"AOV ({currency_label(currency_unit)})", "aov", format_aov)]
    for column, (label, key, formatter) in zip(st.columns(3, gap="small"), card_specs):
        with column:
            with st.container(border=True):
                if key in {"depo_amount", "aov"}:
                    raw_value = totals.get(key, 0.0)
                    _render_hover_metric_card(st, label=label, value=formatter(raw_value, currency_unit=currency_unit), delta=f"{growth.get(key, 0.0):+.2f}% vs prev period", growth_value=growth.get(key, 0.0), tooltip=format_amount_full(raw_value, currency_unit=currency_unit))
                else:
                    st.metric(label=label, value=formatter(totals.get(key, 0.0)), delta=f"{growth.get(key, 0.0):+.2f}% vs prev period")


def render_metric_cards(report: dict[str, object], currency_unit: str) -> None:
    summary = report.get("summary", {})
    if not summary:
        return
    totals = summary.get("current_period", {}).get("totals", {})
    growth = summary.get("growth_percentage", {})
    left_col, right_col = st.columns(2, gap="small")
    with left_col:
        render_status_cards("New User", totals.get("new", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}), growth.get("new", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}), currency_unit=currency_unit)
    with right_col:
        render_status_cards("Existing User", totals.get("existing", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}), growth.get("existing", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}), currency_unit=currency_unit)


def render_report_table(report: dict[str, object], currency_unit: str) -> None:
    timeline = report.get("timeline", [])
    sections = report.get("sections", [])
    if not timeline or not sections:
        st.info("No deposit data for selected date range.")
        return
    header_cells = ['<th class="sticky-col" rowspan="2">Metric</th>']
    for day in timeline:
        header_cells.append(f'<th colspan="2">{dt.date.fromisoformat(day).strftime("%b-%d-%Y")}</th>')
    sub_header_cells = ["<th>New</th><th>Existing</th>" for _ in timeline]
    body_rows: list[str] = []
    total_columns = 1 + (2 * len(timeline))
    for section in sections:
        title = str(section.get("title", ""))
        campaign_id = str(section.get("campaign_id", ""))
        section_label = f"{title} | Campaign ID: {campaign_id}" if campaign_id and campaign_id != "TOTAL" else (title or "TOTAL")
        body_rows.append(f'<tr class="section-head"><td colspan="{total_columns}" class="sticky-col">{section_label}</td></tr>')
        for row in section.get("rows", []):
            metric_name = str(row.get("metric", "-"))
            metric_key = str(row.get("key", ""))
            values = row.get("values", {})
            if metric_key == "depo_amount":
                metric_name = f"Depo Amount ({currency_label(currency_unit)})"
            elif metric_key == "aov":
                metric_name = f"AOV ({currency_label(currency_unit)})"
            metric_cells = [f'<td class="sticky-col metric-name">{metric_name}</td>']
            for day in timeline:
                day_data = values.get(day, {"new": 0, "existing": 0})
                metric_cells.append(f"<td>{metric_formatter(metric_key, day_data.get('new', 0), currency_unit=currency_unit)}</td>")
                metric_cells.append(f"<td>{metric_formatter(metric_key, day_data.get('existing', 0), currency_unit=currency_unit)}</td>")
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
