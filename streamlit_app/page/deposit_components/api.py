"""API adapter helpers for deposit/revenue Streamlit pages."""

from __future__ import annotations

import datetime as dt

import streamlit as st

from streamlit_app.functions.api import fetch_api_result


async def fetch_legacy_deposit_payload(
    *,
    host: str,
    uri: str,
    start_date: dt.date,
    end_date: dt.date,
    campaign_type: str,
    fallback_message: str,
) -> dict[str, object] | None:
    """Fetch deposit payload using the standard API client and return raw shape."""
    result = await fetch_api_result(
        st=st,
        host=host,
        uri=uri,
        method="GET",
        params={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "campaign_type": campaign_type,
        },
    )
    if result.ok and isinstance(result.raw, dict):
        return result.raw
    st.error(result.message or fallback_message)
    return None
