"""Account and validation helpers for Streamlit admin flows."""

from __future__ import annotations

import re

import pandas as pd

from streamlit_app.functions.api import fetch_api_result

ROLE_LABELS: dict[str, str] = {
    "superadmin": "Super Admin",
    "analyst": "Analyst",
    "digital_marketing": "Digital Marketing",
    "finance": "Finance",
    "social_media": "Social Media",
    "tech_it": "Tech/IT",
    "sales": "Sales",
    "admin": "Analyst",
}

ROLE_OPTIONS: dict[str, str] = {
    "Super Admin": "superadmin",
    "Analyst": "analyst",
    "Digital Marketing": "digital_marketing",
    "Finance": "finance",
    "Social Media": "social_media",
    "Tech/IT": "tech_it",
    "Sales": "sales",
}


async def get_accounts(host: str) -> pd.DataFrame:
    """Retrieve account records from the backend API for admin pages."""
    result = await fetch_api_result(st=None, host=host, uri="accounts", method="GET")
    if not result.ok:
        return pd.DataFrame(columns=["user_id", "fullname", "email", "role", "created_at", "updated_at"])

    rows = result.data if isinstance(result.data, list) else []
    if not rows:
        return pd.DataFrame(columns=["user_id", "fullname", "email", "role", "created_at", "updated_at"])

    return pd.DataFrame(rows)


def is_valid_email(email):
    """Validate email string against a basic regex pattern."""
    return re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", email)


def format_role_label(role: str | None) -> str:
    """Return the display label for a stored role code."""
    return ROLE_LABELS.get(role or "", role or "-")
