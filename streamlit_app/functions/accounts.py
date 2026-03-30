"""Account and validation helpers for Streamlit admin flows."""

from __future__ import annotations

import re

import pandas as pd

from streamlit_app.functions.api import fetch_data


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
