"""Session bootstrap and public-route helpers for Streamlit shell."""

from __future__ import annotations

from urllib.parse import urlparse

import streamlit as st

from streamlit_app.app_shell.config import PAGE_KEYS_BY_SLUG
from streamlit_app.functions.runtime import (
    apply_auth_payload,
    clear_auth_state,
    consume_auth_bridge_response,
    resolve_backend_base_url,
    start_auth_bridge_request,
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
        "auth_bridge_request": None,
        "auth_restore_completed": False,
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


def resolve_page_from_query_params() -> str | None:
    """Resolve a normal dashboard page from the URL page slug."""
    slug = query_param("page")
    if not slug:
        return None
    return PAGE_KEYS_BY_SLUG.get(slug.strip().lower())


def resolve_public_page_from_query_params() -> str | None:
    """Detect public pages that must stay reachable before login completes."""
    current_url = getattr(st.context, "url", "")
    path = urlparse(current_url).path.rstrip("/")
    if path == "/terms":
        return "terms"
    if path == "/privacy":
        return "privacy"

    page_slug = (query_param("page") or "").strip().lower()
    if page_slug in {"terms", "privacy"}:
        return page_slug

    oauth_callback_params = ("code", "error", "state", "scope", "authuser", "prompt")
    if query_param("google_ads_oauth") == "1":
        return "google_ads_token"
    if any(query_param(param) for param in oauth_callback_params):
        return "google_ads_token"
    if st.session_state.get("google_ads_oauth_state") and st.query_params:
        return "google_ads_token"
    return None


def restore_login_state_from_cookie(host: str) -> None:
    """Restore login/session state from the browser HttpOnly auth cookie."""
    if st.session_state.get("logged_in") and st.session_state.get("_user_id"):
        return

    if st.session_state.get("auth_restore_completed"):
        return

    if not st.session_state.get("auth_bridge_request"):
        start_auth_bridge_request("restore")

    bridge_response = consume_auth_bridge_response(host, component_key="auth_restore_bridge")
    if bridge_response is None:
        st.caption("Restoring your session...")
        st.stop()

    st.session_state.auth_restore_completed = True
    payload = bridge_response.get("payload", {})
    if not bridge_response.get("ok") or not payload.get("success"):
        clear_auth_state()
        return

    apply_auth_payload(payload)
    st.session_state.page = st.session_state.get("page", "home")
