"""Streamlit renderer for the internal portal home page."""

from __future__ import annotations

from datetime import datetime

import streamlit as st
from streamlit_app.page.home_components.data import format_datetime, load_home_context, role_label
from streamlit_app.page.home_components.ui import PAGE_STYLE, render_hero, render_quick_access, render_status_cards


async def show_home_page(host: str) -> None:
    """Render the internal portal landing page with account and ETL context.

    Args:
        host (str): Backend host parameter kept for dispatcher signature
            consistency across Streamlit pages.

    Returns:
        None: Writes the home-page layout, quick actions, and recent ETL
        context into the active Streamlit session.
    """
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)

    context = await load_home_context(host)
    account = context["account"]
    latest_run = context["latest_run"]
    fullname = account.get("fullname") or account.get("email") or "Team"
    active_role_label = role_label(st.session_state.get("role"))
    if latest_run is not None:
        latest_run["formatted_started_at"] = format_datetime(
            datetime.fromisoformat(latest_run["started_at"]) if latest_run.get("started_at") else None
        )

    st.markdown('<div class="tf-home-shell">', unsafe_allow_html=True)
    render_hero(fullname)
    render_quick_access()
    render_status_cards(account=account, latest_run=latest_run, role_label=active_role_label)
    st.markdown("</div>", unsafe_allow_html=True)
