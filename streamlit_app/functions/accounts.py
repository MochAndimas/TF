"""Account and validation helpers for Streamlit admin flows."""

from __future__ import annotations

import re

import pandas as pd
from sqlalchemy import select

from app.db.models.user import TfUser
from streamlit_app.functions.runtime import get_streamlit


def get_accounts(data: str = "all", user_id: str = None):
    """Retrieve account records for admin pages."""
    session_gen = get_streamlit()
    session = next(session_gen)
    with session.begin():
        if data == "all":
            query = select(TfUser.user_id, TfUser.fullname, TfUser.email, TfUser.role).where(TfUser.deleted_at == None)
        else:
            query = select(TfUser).where(TfUser.user_id == user_id, TfUser.deleted_at == None)
        result = session.execute(query)

    return pd.DataFrame(result.fetchall()) if data == "all" else result.scalar_one_or_none()


def is_valid_email(email):
    """Validate email string against a basic regex pattern."""
    return re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", email)
