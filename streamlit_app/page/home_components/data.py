"""Data loading helpers for the home page."""

from __future__ import annotations

from datetime import datetime


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


def load_home_context(user_id: str) -> dict[str, object]:
    """Load the minimal account and ETL context needed by the home page."""
    from sqlalchemy import select

    from app.db.models.etl_run import EtlRun
    from app.db.models.user import TfUser
    from streamlit_app.functions.runtime import get_streamlit

    session_gen = get_streamlit()
    session = next(session_gen)
    try:
        with session.begin():
            account = session.execute(select(TfUser).where(TfUser.user_id == user_id, TfUser.deleted_at == None)).scalar_one_or_none()
            latest_run = session.execute(select(EtlRun).order_by(EtlRun.started_at.desc())).scalars().first()
    finally:
        session.close()
    return {"account": account, "latest_run": latest_run}
