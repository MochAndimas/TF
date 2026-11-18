"""Per-chart period comparisons shared by analytics pages."""
from datetime import date, datetime, timedelta
import hashlib
import json

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.functions.api import fetch_api_result
from app.utils.period_comparison import previous_period_range


def comparison_period(dates, key):
    """Render independent controls and resolve an inclusive comparison window."""
    if not st.toggle("Compare", key=f"{key}_compare"):
        return None
    mode = st.selectbox("Compare with", ["Previous period", "Previous year", "Custom"], key=f"{key}_compare_mode")
    start, end = dates
    length = (end - start).days + 1
    if mode == "Previous period":
        first, last = previous_period_range(
            start, end, mode=st.session_state.get("analytics_comparison", "previous_period")
        )
    elif mode == "Previous year":
        first = (pd.Timestamp(start) - pd.DateOffset(years=1)).date()
        last = (pd.Timestamp(end) - pd.DateOffset(years=1)).date()
    else:
        selected = st.date_input(
            "Comparison date range",
            value=(start - timedelta(days=length), start - timedelta(days=1)),
            key=f"{key}_compare_dates",
        )
        if len(selected) != 2 or selected[0] > selected[1]:
            st.warning("Select a valid comparison date range.")
            return None
        first, last = selected
    st.caption(f"Compared: {first:%d %b %Y} – {last:%d %b %Y}")
    return first, last


def _values(value):
    """Decode Plotly's typed-array transport as well as ordinary sequences."""
    if isinstance(value, dict) and "bdata" in value:
        import base64
        return np.frombuffer(base64.b64decode(value["bdata"]), dtype=value["dtype"]).tolist()
    return list(value) if value is not None else []


def _day(value):
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, np.datetime64):
        return None if np.isnat(value) else pd.Timestamp(value).date()
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%b %d\n%Y", "%b %d %Y", "%d %b %Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def overlay_comparison(current, previous, dates, comparison_dates):
    """Copy traces while preserving axes, category identities, and missing days."""
    figure = go.Figure(current)
    palette = list(figure.layout.colorway or (
        "#6176ff", "#13c39c", "#ff7f7f", "#fbbf24", "#a78bfa", "#38bdf8",
    ))
    for index, trace in enumerate(figure.data):
        color = palette[index % len(palette)]
        if trace.type == "bar":
            trace.offsetgroup = f"current_{index}"
            if trace.marker.color is None:
                trace.marker.color = color
        elif trace.type == "scatter" and trace.line.color is None:
            trace.line.color = color
    time_axis = any(
        _day(value) is not None
        for trace in (*current.data, *previous.data) if trace.type in {"bar", "scatter"}
        for value in _values(trace.x)
    )
    if time_axis:
        for trace in figure.data:
            trace.x = [(_day(value) or value) for value in _values(trace.x)]
        figure.update_xaxes(type="date")
    for index, source in enumerate(previous.data):
        if source.type not in {"bar", "scatter"} or (source.type == "scatter" and source.mode == "markers"):
            continue
        trace = source.to_plotly_json()
        # Match colors by series identity, even when a ranking changes order.
        matching = next((item for item in figure.data if item.type == source.type and item.name == source.name), None)
        if matching is not None:
            if source.type == "scatter" and matching.line.color:
                trace.setdefault("line", {})["color"] = matching.line.color
            if source.type == "bar" and isinstance(matching.marker.color, str):
                trace.setdefault("marker", {})["color"] = matching.marker.color
        for field in ("text", "texttemplate", "textposition", "customdata", "ids"):
            trace.pop(field, None)
        x = _values(source.x)
        original_dates = [_day(value) for value in x]
        if time_axis and any(day is not None for day in original_dates):
            points = {
                day: value for day, value in zip(original_dates, _values(source.y))
                if day is not None
            }
            timeline = pd.date_range(*comparison_dates)
            trace["x"] = [dates[0] + (day.date() - comparison_dates[0]) for day in timeline]
            trace["y"] = [points.get(day.date()) for day in timeline]
            trace["customdata"] = [day.date().isoformat() for day in timeline]
            trace["hovertemplate"] = "Comparison: %{customdata}<br>%{y:,.2f}<extra>%{fullData.name}</extra>"
        else:
            trace["hovertemplate"] = (
                "%{y}<br>%{x:,.2f}<extra>%{fullData.name}</extra>"
                if source.type == "bar" and source.orientation == "h"
                else "%{x}<br>%{y:,.2f}<extra>%{fullData.name}</extra>"
            )
        trace["name"] = f"{source.name or 'Series ' + str(index + 1)} (comparison)"
        trace["showlegend"] = True
        trace["legendgroup"] = f"comparison_{source.legendgroup or index}"
        if source.type == "bar":
            trace["opacity"] = 0.4
            trace["offsetgroup"] = f"comparison_{source.offsetgroup or index}"
        else:
            trace.setdefault("line", {})["dash"] = "dot"
        figure.add_trace(trace)
    figure.update_layout(showlegend=True)
    if any(trace.type == "bar" for trace in figure.data):
        figure.update_layout(barmode="group")
    return figure


def _find_path(tree, target, path=()):
    if tree is target:
        return path
    if isinstance(tree, dict):
        for key, value in tree.items():
            found = _find_path(value, target, (*path, key))
            if found is not None:
                return found
    return None


class ChartComparison:
    """Fetch a comparison payload once per period and rebuild each chart normally."""

    def __init__(self, host, dates, page, uri, payload, params=None):
        self.host, self.dates, self.page = host, dates, page
        self.uri, self.payload, self.params = uri, payload, params or {}
        self._payloads = {}

    def figure(self, builder):
        """Build current and comparison figures with the same transforms."""
        figure = builder(self.payload, self.dates)
        object.__setattr__(figure, "_comparison_builder", builder)
        return figure

    async def render(self, figure, builder=None, *, key=None, **kwargs):
        builder = builder or getattr(figure, "_comparison_builder", None)
        eligible = any(
            trace.type == "bar" or (trace.type == "scatter" and trace.mode != "markers")
            for trace in figure.data
        )
        if not figure.data and (builder is not None or getattr(figure, "_comparison_source", None) is not None):
            eligible = True
        if not eligible or self.host is None:
            st.plotly_chart(figure, **kwargs)
            return
        title = str(figure.layout.title.text or "chart")
        identity = key or hashlib.sha256(title.encode()).hexdigest()[:16]
        widget_key = f"chart_{self.page}_{identity}"
        path = None
        if builder is None:
            source = getattr(figure, "_comparison_source", None)
            path = _find_path(self.payload, source) if source is not None else None
            if path is None:
                raise ValueError(f"A comparison builder is required for {self.page}: {title}")
        period = comparison_period(self.dates, widget_key)
        if period is not None:
            cache_key = (period, self.uri, json.dumps(self.params, sort_keys=True))
            if cache_key not in self._payloads:
                result = await fetch_api_result(
                    st=st, host=self.host, uri=self.uri,
                    params={**self.params, "start_date": period[0].isoformat(), "end_date": period[1].isoformat()},
                )
                self._payloads[cache_key] = result
            result = self._payloads[cache_key]
            if result.ok:
                if builder:
                    previous = builder(result.data, period)
                else:
                    payload = result.data
                    for part in path:
                        payload = payload[part]
                    previous = go.Figure(payload)
                if previous.data:
                    figure = overlay_comparison(figure, previous, self.dates, period)
                else:
                    st.info("No comparison data for the selected period.")
            else:
                st.error(result.message or "Could not load comparison data.")
        st.plotly_chart(figure, **kwargs)
