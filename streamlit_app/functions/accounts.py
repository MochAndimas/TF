"""Account and validation helpers for Streamlit admin flows."""

from __future__ import annotations

import re

import pandas as pd

from streamlit_app.functions.api import fetch_data

ROLE_LABELS: dict[str, str] = {
    "superadmin": "Super Admin",
    "admin": "Analyst",
    "digital_marketing": "Digital Marketing",
    "sales": "Sales",
    "finance": "Finance",
}

ROLE_OPTIONS: dict[str, str] = {
    label: role for role, label in ROLE_LABELS.items()
}


async def get_accounts(host: str) -> pd.DataFrame:
    """Retrieve account records from the backend API for admin pages."""
    response = await fetch_data(st=None, host=host, uri="accounts", method="GET")
    if not response or not response.get("success"):
        return pd.DataFrame(columns=["user_id", "fullname", "email", "role", "created_at", "updated_at"])

    rows = response.get("data", [])
    if not rows:
        return pd.DataFrame(columns=["user_id", "fullname", "email", "role", "created_at", "updated_at"])

    return pd.DataFrame(rows)


def is_valid_email(email):
    """Validate email string against a basic regex pattern."""
    return re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", email)


def format_role_label(role: str | None) -> str:
    """Return the display label for a stored role code."""
    return ROLE_LABELS.get(role or "", role or "-")
