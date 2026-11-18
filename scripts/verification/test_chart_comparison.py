"""Regressions for shared chart period comparisons."""
from datetime import date

import plotly.graph_objects as go
from streamlit.testing.v1 import AppTest

from streamlit_app.functions.chart_comparison import overlay_comparison


def test_sparse_daily_comparison_preserves_missing_days_and_original_dates():
    current = go.Figure(go.Scatter(
        x=["Nov 01\n2026", "Nov 03\n2026"], y=[5, 9], name="Register", mode="lines",
    ))
    previous = go.Figure(go.Scatter(
        x=["Oct 01\n2026", "Oct 03\n2026"], y=[2, 4], name="Register", mode="lines",
    ))
    result = overlay_comparison(
        current, previous, (date(2026, 11, 1), date(2026, 11, 3)),
        (date(2026, 10, 1), date(2026, 10, 3)),
    )
    assert list(result.data[1].y) == [2, None, 4]
    assert list(result.data[1].customdata) == ["2026-10-01", "2026-10-02", "2026-10-03"]
    assert result.data[1].x[0] == date(2026, 11, 1)
    assert result.data[1].line.dash == "dot"
    assert len(current.data) == 1


def test_horizontal_ranking_matches_category_labels_and_preserves_axis():
    current = go.Figure(go.Bar(x=[10], y=["Campaign A"], orientation="h"))
    previous = go.Figure(go.Bar(x=[7, 4], y=["Campaign B", "Campaign A"], orientation="h", xaxis="x2"))
    result = overlay_comparison(
        current, previous, (date(2026, 11, 1), date(2026, 11, 3)),
        (date(2026, 10, 1), date(2026, 10, 3)),
    )
    assert list(result.data[1].y) == ["Campaign B", "Campaign A"]
    assert result.data[1].xaxis == "x2"
    assert result.data[1].opacity == 0.4


def test_dual_axis_comparison_keeps_qty_on_secondary_axis():
    current = go.Figure([go.Bar(x=["2026-11-01"], y=[100]), go.Scatter(x=["2026-11-01"], y=[2], yaxis="y2")])
    previous = go.Figure([go.Bar(x=["2026-10-01"], y=[50]), go.Scatter(x=["2026-10-01"], y=[1], yaxis="y2")])
    result = overlay_comparison(
        current, previous, (date(2026, 11, 1), date(2026, 11, 1)),
        (date(2026, 10, 1), date(2026, 10, 1)),
    )
    assert result.data[2].type == "bar"
    assert result.data[3].yaxis == "y2"


def test_controls_are_independent_and_previous_year_handles_leap_day():
    source = """
from datetime import date
from streamlit_app.functions.chart_comparison import comparison_period
import streamlit as st
dates = (date(2024,2,29),date(2024,3,1))
for key in ("first","second"):
    st.write(str(comparison_period(dates, key)))
"""
    app = AppTest.from_string(source).run()
    app.toggle(key="first_compare").set_value(True).run()
    app.selectbox(key="first_compare_mode").set_value("Previous year").run()
    assert not app.exception
    assert "28 Feb 2023" in app.caption[0].value
    assert app.toggle(key="second_compare").value is False
    app.selectbox(key="first_compare_mode").set_value("Custom").run()
    app.date_input(key="first_compare_dates").set_value((date(2022,1,1),)).run()
    assert app.warning


def test_payload_builder_reuses_filters_without_changing_primary_data():
    source = """
import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch
import plotly.graph_objects as go
from streamlit_app.functions.chart_comparison import ChartComparison
from streamlit_app.functions.charting import campaign_figure_from_payload
payload = {"chart": {"figure": go.Figure(go.Bar(x=["2026-11-01"], y=[20])).to_plotly_json()}}
async def fetch(**kwargs):
    assert kwargs["params"]["source"] == "google"
    return SimpleNamespace(ok=True, data=payload)
async def run():
    compare = ChartComparison("", (date(2026,11,1),date(2026,11,2)), "test", "analytics", payload, {"source":"google"})
    await compare.render(campaign_figure_from_payload(payload["chart"]["figure"], "Test"), key="chart", width="stretch")
with patch("streamlit_app.functions.chart_comparison.fetch_api_result", fetch):
    asyncio.run(run())
"""
    app = AppTest.from_string(source).run()
    assert not app.exception
    app.toggle(key="chart_test_chart_compare").set_value(True).run()
    assert not app.exception
    assert '"opacity":0.4' in app.get("plotly_chart")[0].proto.spec
