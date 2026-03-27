"""Session bootstrap and public-route helpers for Streamlit shell."""

from __future__ import annotations

import asyncio

import streamlit as st

from streamlit_app.functions.runtime import (
    cookie_controller,
    resolve_backend_base_url,
    restore_backend_session,
    sync_refresh_cookie,
)


def resolve_host() -> str:
    """Resolve backend host URL from environment-aware Streamlit secrets."""
    return resolve_backend_base_url(prefer_internal=True)


def initialize_session_state() -> None:
    """Initialize required Streamlit session-state keys."""
    defaults = {
        "page": "home",
        "logged_in": False,
        "role": None,
        "access_token": None,
        "refresh_token": None,
        "session_id": None,
    }
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value


def query_param(name: str) -> str | None:
    """Read one query parameter and normalize list-style values into one string."""
    value = st.query_params.get(name)
    if isinstance(value, list):
        return value[0] if value else None
    return value


def resolve_public_page_from_query_params() -> str | None:
    """Detect public pages that must stay reachable before login completes."""
    oauth_callback_params = ("code", "error", "state", "scope", "authuser", "prompt")
    if query_param("google_ads_oauth") == "1":
        return "google_ads_token"
    if any(query_param(param) for param in oauth_callback_params):
        return "google_ads_token"
    if st.session_state.get("google_ads_oauth_state") and st.query_params:
        return "google_ads_token"
    return None


def restore_login_state_from_cookie() -> None:
    """Restore login/session state from persisted cookie value."""
    if st.session_state.get("logged_in") and st.session_state.get("_user_id"):
        return

    remembered_refresh_token = cookie_controller.get("refresh_token") or None
    if not remembered_refresh_token:
        initialize_session_state()
        return

    host = resolve_host()
    restored_payload = asyncio.run(restore_backend_session(host, remembered_refresh_token))
    if not restored_payload or not restored_payload.get("success"):
        cookie_controller.set("refresh_token", "", max_age=0)
        initialize_session_state()
        return

    st.session_state.logged_in = True
    st.session_state.role = restored_payload.get("role")
    st.session_state._user_id = restored_payload.get("user_id")
    st.session_state.access_token = restored_payload.get("access_token")
    st.session_state.refresh_token = restored_payload.get("refresh_token")
    st.session_state.session_id = restored_payload.get("session_id")
    sync_refresh_cookie(host, restored_payload.get("refresh_token"))
    st.session_state.page = st.session_state.get("page", "home")
