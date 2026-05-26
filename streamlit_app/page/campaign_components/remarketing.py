"""Remarketing page helpers."""

from __future__ import annotations

from streamlit_app.page.campaign_components.brand_awareness import (
    build_performance_table as _build_performance_table,
    render_performance_table,
)


def build_performance_table(detail_rows: list[dict]):
    """Build remarketing performance table data with isolated widget state key."""
    return _build_performance_table(
        detail_rows=detail_rows,
        selectbox_key="remarketing_performance_level",
    )
