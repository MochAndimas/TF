"""Independent comparison periods for management line charts."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.functions.api import fetch_api_result
from streamlit_app.functions.chart_comparison import comparison_period


async def comparison_rows(host, dates, key, path):
    period = comparison_period(dates, key)
    if period is None:
        return None
    first, last = period
    # Cache only this graph's latest comparison, tied to the current main period.
    cache_key = (host, dates, first, last, st.session_state.get("analytics_comparison"))
    cache = st.session_state.get(f"{key}_comparison_payload")
    if cache is None or cache[0] != cache_key:
        result = await fetch_api_result(
            st=st, host=host, uri="deposit-revenue/register-total",
            params={"start_date": first.isoformat(), "end_date": last.isoformat()},
        )
        if not result.ok:
            st.error(result.message or "Could not load comparison data.")
            return None
        cache = (cache_key, result.data)
        st.session_state[f"{key}_comparison_payload"] = cache
    rows = cache[1]
    for field in path:
        rows = rows[field]
    by_day = {row["date"]: row for row in rows}
    return [
        by_day.get(day.date().isoformat(), {"date": day.date().isoformat()})
        for day in pd.date_range(first, last)
    ]


def add_comparison_line(figure, rows, dates, field, name, color, *, secondary_y=None, ratio=False):
    if rows is None:
        return
    if not any(row.get(field) is not None for row in rows):
        st.info("No comparison data for the selected period.")
        return
    start, end = dates
    # Align by day position. Extend the axis if a custom comparison is longer.
    x = pd.date_range(start, periods=max((end - start).days + 1, len(rows)))[:len(rows)]
    values = [
        row["cost"] / row["revenue_idr"] * 100 if row.get("revenue_idr") else None
        for row in rows
    ] if ratio else [row.get(field) for row in rows]
    trace = go.Scatter(
        x=x, y=values, mode="lines+markers", name=f"{name} (comparison)",
        line={"color": color, "dash": "dot"},
        customdata=[row["date"] for row in rows],
        hovertemplate="Comparison: %{customdata}<br>%{y:,.2f}<extra>%{fullData.name}</extra>",
    )
    if secondary_y is None:
        figure.add_trace(trace)
    else:
        figure.add_trace(trace, secondary_y=secondary_y)
    figure.update_layout(showlegend=True)
