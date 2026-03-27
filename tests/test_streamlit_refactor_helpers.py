"""Regression tests for refactored Streamlit helper modules."""

from __future__ import annotations

from datetime import date, datetime
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
import sys
import types


class _StreamlitStub(types.SimpleNamespace):
    def date_input(self, *args, **kwargs):  # pragma: no cover - patched in tests
        return ()


sys.modules.setdefault("streamlit", _StreamlitStub())


class _FakeTrace:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class _FakeFigure:
    def __init__(self, data=None):
        self.data = list(data or [])
        self.layout = {}

    def add_trace(self, trace):
        self.data.append(trace)

    def update_layout(self, **kwargs):
        self.layout.update(kwargs)


fake_plotly = types.ModuleType("plotly")
fake_graph_objects = types.ModuleType("plotly.graph_objects")
fake_graph_objects.Figure = _FakeFigure
fake_graph_objects.Bar = _FakeTrace
fake_graph_objects.Scatter = _FakeTrace
sys.modules.setdefault("plotly", fake_plotly)
sys.modules.setdefault("plotly.graph_objects", fake_graph_objects)

fake_decouple = types.ModuleType("decouple")
fake_decouple.config = lambda *args, **kwargs: kwargs.get("default")
sys.modules.setdefault("decouple", fake_decouple)

from streamlit_app.page.campaign_components.brand_awareness import (
    build_performance_dataframe as build_ba_performance_dataframe,
)
from streamlit_app.page.campaign_components.user_acquisition import (
    build_performance_dataframe as build_ua_performance_dataframe,
)
from streamlit_app.page.home_components.data import format_datetime, role_label
from streamlit_app.page.overview_components.charts import build_cost_vs_deposit_figure
from streamlit_app.page.overview_components.formatting import apply_currency_to_ua_table
from streamlit_app.page.update_data_components.form import date_presets, resolve_date_input


class TestStreamlitRefactorHelpers(TestCase):
    """Unit tests for refactored Streamlit helper modules."""

    def test_role_label_maps_known_and_unknown_roles(self):
        self.assertEqual(role_label("superadmin"), "Super Admin")
        self.assertEqual(role_label("digital_marketing"), "Digital Marketing")
        self.assertEqual(role_label("custom-role"), "custom-role")
        self.assertEqual(role_label(None), "-")

    def test_format_datetime_returns_dash_for_missing_values(self):
        self.assertEqual(format_datetime(None), "-")
        self.assertEqual(format_datetime(datetime(2026, 3, 27, 14, 30)), "27 Mar 2026, 14:30")

    def test_date_presets_build_expected_ranges(self):
        presets = date_presets(date(2026, 3, 27))
        self.assertEqual(presets["Yesterday"], (date(2026, 3, 26), date(2026, 3, 26)))
        self.assertEqual(presets["Last 7 Days"], (date(2026, 3, 20), date(2026, 3, 26)))
        self.assertEqual(presets["This Month"], (date(2026, 3, 1), date(2026, 3, 27)))
        self.assertEqual(presets["Last Month"], (date(2026, 2, 1), date(2026, 2, 28)))
        self.assertIsNone(presets["Custom Range"])

    def test_resolve_date_input_uses_manual_preset_directly(self):
        presets = date_presets(date(2026, 3, 27))
        result = resolve_date_input("manual", "Last 7 Days", presets)
        self.assertEqual(result, (date(2026, 3, 20), date(2026, 3, 26)))

    def test_resolve_date_input_uses_auto_mode_yesterday(self):
        with patch("streamlit_app.page.update_data_components.form.dt.date") as mock_date:
            mock_date.today.return_value = date(2026, 3, 27)
            mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
            result = resolve_date_input("auto", "Yesterday", {})
        self.assertEqual(result, (date(2026, 3, 26), date(2026, 3, 26)))

    def test_resolve_date_input_validates_custom_range(self):
        with patch("streamlit_app.page.update_data_components.form.st.date_input", return_value=(date(2026, 3, 10), date(2026, 3, 15))):
            result = resolve_date_input("manual", "Custom Range", {"Custom Range": None})
        self.assertEqual(result, (date(2026, 3, 10), date(2026, 3, 15)))

    def test_ua_performance_dataframe_aggregates_metrics(self):
        detail_rows = [
            {"campaign_source": "google", "campaign_id": "cmp-1", "spend": 100.0, "impressions": 1000, "clicks": 100, "leads": 10},
            {"campaign_source": "google", "campaign_id": "cmp-1", "spend": 50.0, "impressions": 500, "clicks": 25, "leads": 5},
        ]
        dataframe = build_ua_performance_dataframe(detail_rows, "campaign_id")
        self.assertEqual(len(dataframe), 1)
        row = dataframe.iloc[0]
        self.assertEqual(row["campaign_source"], "google")
        self.assertEqual(row["campaign_id"], "cmp-1")
        self.assertEqual(float(row["spend"]), 150.0)
        self.assertEqual(int(row["impressions"]), 1500)
        self.assertEqual(int(row["clicks"]), 125)
        self.assertEqual(int(row["leads"]), 15)
        self.assertEqual(float(row["avg_click_to_leads_pct"]), 12.0)
        self.assertEqual(float(row["avg_ctr_pct"]), 8.33)
        self.assertEqual(float(row["cpc"]), 1.2)
        self.assertEqual(float(row["cpm"]), 100.0)
        self.assertEqual(float(row["cost_per_leads"]), 10.0)

    def test_ba_performance_dataframe_aggregates_metrics(self):
        detail_rows = [
            {"campaign_source": "facebook", "ad_group": "grp-1", "spend": 80.0, "impressions": 4000, "clicks": 80},
            {"campaign_source": "facebook", "ad_group": "grp-1", "spend": 20.0, "impressions": 1000, "clicks": 20},
        ]
        dataframe = build_ba_performance_dataframe(detail_rows, "ad_group")
        self.assertEqual(len(dataframe), 1)
        row = dataframe.iloc[0]
        self.assertEqual(row["campaign_source"], "facebook")
        self.assertEqual(row["ad_group"], "grp-1")
        self.assertEqual(float(row["spend"]), 100.0)
        self.assertEqual(int(row["impressions"]), 5000)
        self.assertEqual(int(row["clicks"]), 100)
        self.assertEqual(float(row["ctr"]), 2.0)
        self.assertEqual(float(row["cpm"]), 20.0)
        self.assertEqual(float(row["cpc"]), 1.0)

    def test_apply_currency_to_ua_table_formats_values_for_usd(self):
        dataframe = pd.DataFrame(
            [
                {
                    "Source": "Google",
                    "Cost": 16000.0,
                    "Impressions": 2000,
                    "Clicks": 100,
                    "Leads": 25,
                    "Cost/Lead": 800.0,
                }
            ]
        )
        formatted = apply_currency_to_ua_table(dataframe, "USD")
        self.assertTrue(str(formatted.iloc[0]["Cost"]).startswith("$ "))
        self.assertEqual(formatted.iloc[0]["Impressions"], "2,000")
        self.assertEqual(formatted.iloc[0]["Clicks"], "100")
        self.assertEqual(formatted.iloc[0]["Leads"], "25")

    def test_build_cost_vs_deposit_figure_builds_currency_specific_trace_names(self):
        figure = build_cost_vs_deposit_figure(
            rows=[{"date": "2026-03-20", "cost": 32000.0, "first_deposit_idr": 16000.0}],
            currency_unit="USD",
        )
        self.assertEqual(len(figure.data), 2)
        self.assertEqual(figure.data[0].name, "Cost (USD)")
        self.assertEqual(figure.data[1].name, "First Deposit (USD)")
