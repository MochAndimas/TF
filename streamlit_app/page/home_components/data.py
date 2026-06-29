"""Data loading helpers for the home page."""

from __future__ import annotations

from datetime import datetime

from streamlit_app.functions.api import fetch_api_result
from streamlit_app.functions.accounts import format_role_label


def format_datetime(value: datetime | None) -> str:
    """Format nullable datetimes for the home-page status cards."""
    if value is None:
        return "-"
    return value.strftime("%d %b %Y, %H:%M")


def role_label(role: str | None) -> str:
    """Map stored role codes into friendlier labels for the portal UI."""
    return format_role_label(role)


async def load_home_context(host: str) -> dict[str, object]:
    """Load the minimal account and ETL context needed by the home page."""
    result = await fetch_api_result(st=None, host=host, uri="home/context", method="GET")
    if not result.ok:
        return {"account": {}, "latest_run": None}

    data = result.data if isinstance(result.data, dict) else {}
    return {
        "account": data.get("account", {}) or {},
        "latest_run": data.get("latest_run"),
    }
