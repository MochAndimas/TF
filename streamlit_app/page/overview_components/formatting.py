"""Formatting helpers for the Overview Streamlit page."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from decouple import config

USD_TO_IDR_RATE = config("USD_TO_IDR_RATE", default=16968, cast=float)


def set_transparent_chart_background(figure):
    figure.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return figure


def render_currency_toggle(key: str) -> str:
    left, mid, right = st.columns([2.2, 2.6, 2.2], gap="small")
    with mid:
        label_col, control_col = st.columns([1.1, 2.2], gap="small")
        with label_col:
            st.markdown('<div style="font-size:0.98rem;font-weight:600;padding-top:0.15rem;text-align:right;">Currency</div>', unsafe_allow_html=True)
        with control_col:
            return st.radio("Currency", options=["IDR", "USD"], horizontal=True, key=key, label_visibility="collapsed")


def convert_idr_to_usd(value: float | int) -> float:
    rate = float(USD_TO_IDR_RATE or 0)
    if rate == 0:
        return float(value)
    return float(value) / rate


def format_currency_value(value: float | int, currency_unit: str) -> str:
    number = float(value)
    if currency_unit == "USD":
        return f"$ {number:,.2f}" if abs(number) < 1000 else f"$ {number:,.0f}"
    return f"Rp {number:,.0f}"


def apply_currency_to_ua_table(leads_table_df: pd.DataFrame, currency_unit: str) -> pd.DataFrame:
    if leads_table_df.empty:
        return leads_table_df
    formatted = leads_table_df.copy()
    if currency_unit == "USD":
        formatted["Cost"] = formatted["Cost"].apply(lambda value: format_currency_value(convert_idr_to_usd(value), "USD"))
        formatted["Cost/Lead"] = formatted["Cost/Lead"].apply(lambda value: format_currency_value(convert_idr_to_usd(value), "USD"))
    else:
        formatted["Cost"] = formatted["Cost"].apply(lambda value: format_currency_value(value, "IDR"))
        formatted["Cost/Lead"] = formatted["Cost/Lead"].apply(lambda value: format_currency_value(value, "IDR"))
    formatted["Impressions"] = formatted["Impressions"].apply(lambda value: f"{int(value):,}")
    formatted["Clicks"] = formatted["Clicks"].apply(lambda value: f"{int(value):,}")
    formatted["Leads"] = formatted["Leads"].apply(lambda value: f"{int(value):,}")
    return formatted


def apply_currency_to_ua_figure(figure: go.Figure, chart_type: str, currency_unit: str) -> go.Figure:
    if currency_unit == "IDR" or not figure.data:
        return figure

    if chart_type == "cost_vs_leads":
        cost_trace = figure.data[0]
        cpl_trace = figure.data[1]
        cost_trace.y = [convert_idr_to_usd(value) for value in (cost_trace.y or [])]
        cpl_trace.y = [convert_idr_to_usd(value) for value in (cpl_trace.y or [])]
        cost_trace.name = "Cost (USD)"
        cpl_trace.name = "Cost/Lead (USD)"
        cost_trace.hovertemplate = "<b>%{x}</b><br>Cost: $ %{y:,.2f}<extra></extra>"
        cpl_trace.hovertemplate = "<b>%{x}</b><br>Cost/Lead: $ %{y:,.2f}<extra></extra>"
        figure.update_layout(yaxis=dict(title="Cost (USD)"), yaxis2=dict(title="Cost/Lead (USD)"))
        return figure

    if chart_type == "cost_to_revenue":
        cost_trace = figure.data[0]
        deposit_trace = figure.data[1]
        cost_trace.y = [convert_idr_to_usd(value) for value in (cost_trace.y or [])]
        deposit_trace.y = [convert_idr_to_usd(value) for value in (deposit_trace.y or [])]
        cost_trace.name = "Cost (USD)"
        deposit_trace.name = "Total Deposit (USD)"
        cost_trace.hovertemplate = "<b>%{x}</b><br>Cost: $ %{y:,.2f}<extra></extra>"
        deposit_trace.hovertemplate = "<b>%{x}</b><br>Total Deposit: $ %{y:,.2f}<extra></extra>"
        figure.update_layout(yaxis=dict(title="Cost (USD)"), yaxis2=dict(title="Total Deposit (USD)"))
    return figure
