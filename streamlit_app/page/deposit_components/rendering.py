"""Rendering helpers for the deposit page."""

from __future__ import annotations

import html as html_escape
from textwrap import dedent

import pandas as pd
import streamlit as st
from streamlit.components.v1 import html as components_html

from streamlit_app.functions.metrics import _render_hover_metric_card
from streamlit_app.page.deposit_components.formatting import (
    currency_label,
    currency_multiplier,
    format_amount,
    format_amount_full,
    format_aov,
    format_qty,
)


def render_status_cards(
    title: str,
    totals: dict[str, float],
    growth: dict[str, float],
    currency_unit: str,
    deposit_label: str = "First Deposit",
) -> None:
    st.markdown(f'<div class="deposit-group-title">{title}</div>', unsafe_allow_html=True)
    card_specs = [(f"{deposit_label} Amount ({currency_label(currency_unit)})", "depo_amount", format_amount), (f"{deposit_label} (Qty)", "qty", format_qty), (f"AOV ({currency_label(currency_unit)})", "aov", format_aov)]
    for column, (label, key, formatter) in zip(st.columns(3, gap="small"), card_specs):
        with column:
            with st.container(border=True):
                if key in {"depo_amount", "aov"}:
                    raw_value = totals.get(key, 0.0)
                    _render_hover_metric_card(st, label=label, value=formatter(raw_value, currency_unit=currency_unit), delta=f"{growth.get(key, 0.0):+.2f}% vs prev period", growth_value=growth.get(key, 0.0), tooltip=format_amount_full(raw_value, currency_unit=currency_unit))
                else:
                    st.metric(label=label, value=formatter(totals.get(key, 0.0)), delta=f"{growth.get(key, 0.0):+.2f}% vs prev period")


def render_metric_cards(report: dict[str, object], currency_unit: str, deposit_label: str = "First Deposit") -> None:
    summary = report.get("summary", {})
    if not summary:
        return
    totals = summary.get("current_period", {}).get("totals", {})
    growth = summary.get("growth_percentage", {})
    left_col, right_col = st.columns(2, gap="small")
    with left_col:
        render_status_cards("New User", totals.get("new", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}), growth.get("new", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}), currency_unit=currency_unit, deposit_label=deposit_label)
    with right_col:
        render_status_cards("Existing User", totals.get("existing", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}), growth.get("existing", {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}), currency_unit=currency_unit, deposit_label=deposit_label)


def render_deposit_method_table(report: dict[str, object], currency_unit: str, height: int = 320) -> None:
    rows = report.get("deposit_method_summary", [])
    if not rows:
        return

    multiplier = currency_multiplier(currency_unit)
    table_df = pd.DataFrame(
        [
            {
                "Deposit Method": row.get("method", "-"),
                "Qty": int(row.get("deposit_qty", 0) or 0),
                "Share %": float(row.get("share_pct", 0) or 0),
                "Amount": float(row.get("deposit_amount", 0) or 0) * multiplier,
                "Avg Deposit": float(row.get("average_deposit", 0) or 0) * multiplier,
            }
            for row in rows
        ]
    )
    amount_format = "Rp %.0f" if currency_unit == "IDR" else "$ %.2f"
    st.dataframe(
        table_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Deposit Method": st.column_config.TextColumn("Deposit Method", width="medium"),
            "Qty": st.column_config.NumberColumn("Qty", format="%d", width="small"),
            "Share %": st.column_config.NumberColumn("Share %", format="%.0f%%", width="small"),
            "Amount": st.column_config.NumberColumn("Amount", format=amount_format, width="small"),
            "Avg Deposit": st.column_config.NumberColumn("Avg Deposit", format=amount_format, width="small"),
        },
        height=height,
    )


def render_campaign_deposit_table(report: dict[str, object], currency_unit: str, height: int = 380) -> None:
    rows = report.get("campaign_totals", [])
    if not rows:
        st.info("No campaign deposit data for selected date range.")
        return

    multiplier = currency_multiplier(currency_unit)
    table_rows: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        amount_map = row.get("depo_amount", {})
        qty_map = row.get("qty", {})
        total_amount = (float(amount_map.get("new", 0) or 0) + float(amount_map.get("existing", 0) or 0)) * multiplier
        total_qty = int(qty_map.get("new", 0) or 0) + int(qty_map.get("existing", 0) or 0)
        avg_deposit = total_amount / total_qty if total_qty else 0.0
        table_rows.append(
            {
                "Campaign ID": str(row.get("campaign_id") or "-"),
                "Campaign Name": str(row.get("campaign_name") or "-"),
                "Depo Amount": total_amount,
                "Jumlah Depo (qty)": total_qty,
                "Avg Depo Value": avg_deposit,
            }
        )

    if not table_rows:
        st.info("No campaign deposit data for selected date range.")
        return

    table_df = pd.DataFrame(table_rows).sort_values("Depo Amount", ascending=False)

    def currency_value(value: object) -> str:
        numeric_value = float(value or 0)
        if currency_unit == "IDR":
            return f"Rp{numeric_value:,.0f}"
        return f"${numeric_value:,.2f}"

    body_rows = []
    for row in table_df.to_dict("records"):
        body_rows.append(
            dedent(
                """
            <tr>
                <td class="tf-nowrap">{campaign_id}</td>
                <td class="tf-wrap">{campaign_name}</td>
                <td class="tf-number">{depo_amount}</td>
                <td class="tf-number">{qty}</td>
                <td class="tf-number">{avg_depo}</td>
            </tr>
            """
            ).format(
                campaign_id=html_escape.escape(str(row["Campaign ID"])),
                campaign_name=html_escape.escape(str(row["Campaign Name"])),
                depo_amount=currency_value(row["Depo Amount"]),
                qty=f"{int(row['Jumlah Depo (qty)']):,}",
                avg_depo=currency_value(row["Avg Depo Value"]),
            )
        )

    table_height = max(180, min(height, 92 + (len(body_rows) * 76)))
    table_markup = dedent(
        """
        <!doctype html>
        <html>
        <head>
        <style>
            :root {{
                color-scheme: dark;
            }}
            body {{
                margin: 0;
                background: transparent;
                color: rgb(250, 250, 250);
                font-family: "Source Sans Pro", sans-serif;
            }}
            .tf-campaign-deposit-table {{
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                overflow: hidden;
                border: 1px solid rgba(148, 163, 184, 0.24);
                border-radius: 8px;
                font-size: 0.95rem;
            }}
            .tf-campaign-deposit-table th,
            .tf-campaign-deposit-table td {{
                border-right: 1px solid rgba(148, 163, 184, 0.18);
                border-bottom: 1px solid rgba(148, 163, 184, 0.18);
                padding: 0.75rem 0.8rem;
                vertical-align: top;
            }}
            .tf-campaign-deposit-table th {{
                background: rgba(148, 163, 184, 0.08);
                color: rgba(255, 255, 255, 0.72);
                font-weight: 700;
                text-align: left;
                white-space: normal;
            }}
            .tf-campaign-deposit-table tr:last-child td {{
                border-bottom: 0;
            }}
            .tf-campaign-deposit-table th:last-child,
            .tf-campaign-deposit-table td:last-child {{
                border-right: 0;
            }}
            .tf-campaign-deposit-table .tf-wrap {{
                white-space: normal;
                overflow-wrap: anywhere;
                word-break: normal;
                line-height: 1.35;
                min-width: 260px;
            }}
            .tf-campaign-deposit-table .tf-nowrap {{
                white-space: nowrap;
            }}
            .tf-campaign-deposit-table .tf-number {{
                text-align: right;
                white-space: nowrap;
                font-variant-numeric: tabular-nums;
            }}
        </style>
        </head>
        <body>
        <table class="tf-campaign-deposit-table">
            <thead>
                <tr>
                    <th>Campaign ID</th>
                    <th>Campaign Name</th>
                    <th>Depo Amount</th>
                    <th>Jumlah Depo (qty)</th>
                    <th>Avg Depo Value</th>
                </tr>
            </thead>
            <tbody>
                {body_rows}
            </tbody>
        </table>
        </body>
        </html>
        """
    ).format(body_rows="".join(body_rows)).strip()
    components_html(
        table_markup,
        height=table_height,
        scrolling=True,
    )
