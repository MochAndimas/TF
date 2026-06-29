"""API adapter helpers for activity Streamlit pages."""

from __future__ import annotations

import datetime as dt

import streamlit as st

from streamlit_app.functions.api import fetch_api_result


async def fetch_legacy_activity_payload(
    *,
    host: str,
    uri: str,
    start_date: dt.date,
    end_date: dt.date,
    source: str,
    fallback_message: str,
) -> dict[str, object] | None:
    """Fetch activity payload using the standard API client and return raw shape."""
    result = await fetch_api_result(
        st=st,
        host=host,
        uri=uri,
        method="GET",
        params={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "source": source,
        },
    )
    if result.ok and isinstance(result.raw, dict):
        return result.raw
    st.error(result.message or fallback_message)
    return None
