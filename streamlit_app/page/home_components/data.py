"""Data loading helpers for the home page."""

from __future__ import annotations

from datetime import datetime

from streamlit_app.functions.api import fetch_data


def format_datetime(value: datetime | None) -> str:
    """Format nullable datetimes for the home-page status cards."""
    if value is None:
        return "-"
    return value.strftime("%d %b %Y, %H:%M")


def role_label(role: str | None) -> str:
    """Map stored role codes into friendlier labels for the portal UI."""
    labels = {
        "superadmin": "Super Admin",
        "admin": "Admin",
        "digital_marketing": "Digital Marketing",
        "sales": "Sales",
    }
    return labels.get(role or "", role or "-")


async def load_home_context(host: str) -> dict[str, object]:
    """Load the minimal account and ETL context needed by the home page."""
    response = await fetch_data(st=None, host=host, uri="home/context", method="GET")
    if not response or not response.get("success"):
        return {"account": {}, "latest_run": None}

    data = response.get("data", {})
    return {
        "account": data.get("account", {}) or {},
        "latest_run": data.get("latest_run"),
    }
