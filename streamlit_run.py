"""Main Streamlit entrypoint for Traders Family dashboard navigation.

This module defines:
    - role-based page access configuration,
    - sidebar grouping and navigation behavior,
    - page dispatching into async page handlers.
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

import streamlit as st
from streamlit.components.v1 import html
from decouple import config

from streamlit_app.functions.utils import (
    cookie_controller,
    footer,
    logout,
    restore_backend_session,
)
from streamlit_app.page import (
    brand_awareness,
    deposit,
    google_ads_token,
    home,
    login,
    meta_ads_token,
    overview,
    register,
    update_data,
    user_acquisition,
)

st.set_page_config(
    page_title="Traders Family Dashboard",
    page_icon="./streamlit_app/page/logotf.png",
    layout="wide",
    initial_sidebar_state="collapsed",
)

PageHandler = Callable[[str], Awaitable[None]]

PAGE_LABELS: dict[str, str] = {
    "home": "Home",
    "overview": "Overview",
    "user_acquisition": "User Acquisition",
    "brand_awareness": "Brand Awareness",
    "deposit_report": "First Deposit",
    "register": "Create Account",
    "update_data": "Update Data",
    "google_ads_token": "Google Ads Token",
    "meta_ads_token": "Meta Ads Token",
}

PAGE_BUTTON_TYPES: dict[str, str] = {
    "home": "secondary",
    "overview": "tertiary",
    "user_acquisition": "tertiary",
    "brand_awareness": "tertiary",
    "deposit_report": "tertiary",
    "register": "tertiary",
    "update_data": "tertiary",
    "google_ads_token": "tertiary",
    "meta_ads_token": "tertiary",
}

NAV_GROUPS: dict[str, list[str]] = {
    "Portal": ["home"],
    "Overall": ["overview"],
    "Revenue": ["deposit_report"],
    "Campaign": ["user_acquisition", "brand_awareness"],
    "Settings": ["register", "update_data", "google_ads_token", "meta_ads_token"],
}

ROLE_PAGE_ACCESS: dict[str, list[str]] = {
    "superadmin": ["home", "overview", "user_acquisition", "brand_awareness", "deposit_report", "register", "update_data", "google_ads_token", "meta_ads_token"],
    "admin": ["home", "overview", "user_acquisition", "brand_awareness", "deposit_report"],
    "digital_marketing": ["home", "overview", "user_acquisition", "brand_awareness", "deposit_report"],
    "sales": ["home", "overview", "user_acquisition", "brand_awareness", "deposit_report"],
}


def _inject_navigation_style() -> None:
    """Inject custom CSS for sidebar visual hierarchy.

    Returns:
        None: Writes style block to Streamlit frontend.
    """
    st.markdown(
        """
        <style>
            .tf-nav-title {
                font-size: 0.78rem;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                opacity: 0.72;
                margin-top: 0.35rem;
                margin-bottom: 0.2rem;
            }
            .tf-nav-standalone {
                margin-bottom: 0.7rem;
            }
            .tf-nav-divider {
                border-bottom: 1px solid var(--secondary-background-color);
                margin-top: 0.45rem;
                margin-bottom: 0.55rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _collapse_sidebar_if_requested() -> None:
    """Run best-effort sidebar auto-collapse after navigation click.

    Returns:
        None: Executes a small browser script and updates session flags.
    """
    if not st.session_state.get("collapse_sidebar_once", False):
        return

    nonce = st.session_state.get("collapse_sidebar_nonce", 0)
    script = """
        <script>
        // nonce: __NONCE__
        const findCollapseButton = () => (
          window.parent.document.querySelector('button[aria-label="Close sidebar"]') ||
          window.parent.document.querySelector('button[aria-label="Collapse sidebar"]') ||
          window.parent.document.querySelector('[data-testid="stSidebarCollapseButton"] button') ||
          window.parent.document.querySelector('[data-testid="collapsedControl"]')
        );

        let attempts = 0;
        const timer = setInterval(() => {
          const button = findCollapseButton();
          if (button) {
            button.click();
            clearInterval(timer);
          }
          attempts += 1;
          if (attempts > 20) {
            clearInterval(timer);
          }
        }, 80);
        </script>
        """
    html(
        script.replace("__NONCE__", str(nonce)),
        height=0,
        width=0,
    )
    st.session_state["collapse_sidebar_once"] = False


def _hide_sidebar_on_login() -> None:
    """Hide sidebar elements when user is on login view.

    Returns:
        None: Injects CSS override for sidebar components.
    """
    st.markdown(
        """
        <style>
            [data-testid="stSidebar"],
            [data-testid="collapsedControl"] {
                display: none;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _resolve_host() -> str:
    """Resolve backend host URL from environment-aware Streamlit secrets.

    Returns:
        str: API base URL used by all Streamlit page handlers.
    """
    internal_api_host = config("STREAMLIT_API_HOST", default="", cast=str).strip()
    if internal_api_host:
        return internal_api_host.rstrip("/")

    env_name = config("ENV", default="development", cast=str).lower()
    if env_name == "production":
        return st.secrets["api"]["HOST"]
    return st.secrets["api"]["DEV_HOST"]


def _initialize_session_state() -> None:
    """Initialize required Streamlit session-state keys.

    Returns:
        None: Creates default keys when they do not exist.
    """
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


def _query_param(name: str) -> str | None:
    """Read a query parameter as a single string value."""
    value = st.query_params.get(name)
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _resolve_public_page_from_query_params() -> str | None:
    """Resolve non-authenticated pages that can be reached via query params."""
    oauth_callback_params = ("code", "error", "state", "scope", "authuser", "prompt")
    if _query_param("google_ads_oauth") == "1":
        return "google_ads_token"

    if any(_query_param(param) for param in oauth_callback_params):
        return "google_ads_token"

    if st.session_state.get("google_ads_oauth_state") and st.query_params:
        return "google_ads_token"

    return None


def _restore_login_state_from_cookie() -> None:
    """Restore login/session state from persisted cookie value.

    Returns:
        None: Restores session values in-place when cookie/session exists.
    """
    if st.session_state.get("logged_in") and st.session_state.get("_user_id"):
        return

    refresh_cookie = cookie_controller.get("refresh_token")
    remembered_refresh_token = refresh_cookie or None
    if not remembered_refresh_token:
        _initialize_session_state()
        return

    host = _resolve_host()
    restored_payload = asyncio.run(restore_backend_session(host, remembered_refresh_token))
    if not restored_payload or not restored_payload.get("success"):
        cookie_controller.set("refresh_token", "", max_age=0)
        _initialize_session_state()
        return

    st.session_state.logged_in = True
    st.session_state.role = restored_payload.get("role")
    st.session_state._user_id = restored_payload.get("user_id")
    st.session_state.access_token = restored_payload.get("access_token")
    st.session_state.refresh_token = restored_payload.get("refresh_token")
    st.session_state.session_id = restored_payload.get("session_id")
    st.session_state.page = st.session_state.get("page", "home")


def _allowed_pages_for_role(role: str | None) -> list[str]:
    """Get allowed page keys based on active authenticated role.

    Args:
        role (str | None): Current authenticated user role from session state.

    Returns:
        list[str]: Ordered page keys allowed for current role.
    """
    return ROLE_PAGE_ACCESS.get(role or "", [])


def _render_sidebar_navigation(host: str) -> str | None:
    """Render sidebar navigation and return selected page key.

    Args:
        host (str): API base URL used by logout flow.

    Returns:
        str | None: Selected page key for dispatcher, or ``None`` when not logged in.
    """
    public_page = _resolve_public_page_from_query_params()
    if public_page:
        return public_page

    available_pages = _allowed_pages_for_role(st.session_state.role)
    st.session_state["allowed_pages"] = available_pages
    if not available_pages:
        st.warning("No page access configured for your account role.")
        asyncio.run(logout(st, host))
        return None

    with st.sidebar:
        st.image("./streamlit_app/page/logotf.png", width=96)

        current_page = st.session_state.page
        if current_page not in available_pages:
            current_page = available_pages[0]
            st.session_state.page = current_page

        st.markdown('<p class="tf-nav-title">Navigation</p>', unsafe_allow_html=True)
        collapse_nav_once = st.session_state.get("collapse_nav_once", False)
        selected_page = current_page

        if "home" in available_pages:
            st.markdown('<div class="tf-nav-standalone">', unsafe_allow_html=True)
            if st.button(
                PAGE_LABELS["home"],
                key="nav_home",
                type=PAGE_BUTTON_TYPES.get("home", "secondary"),
                width="stretch",
            ):
                selected_page = "home"
                st.session_state["collapse_nav_once"] = True
                st.session_state["collapse_sidebar_once"] = True
                st.session_state["collapse_sidebar_nonce"] = (
                    st.session_state.get("collapse_sidebar_nonce", 0) + 1
                )
            st.markdown("</div>", unsafe_allow_html=True)

        for group_title, group_pages in NAV_GROUPS.items():
            visible_pages = [page_key for page_key in group_pages if page_key in available_pages]
            if not visible_pages:
                continue
            if visible_pages == ["home"]:
                continue

            group_expanded = False if collapse_nav_once else (current_page in visible_pages)
            with st.expander(group_title, expanded=group_expanded):
                for page_key in visible_pages:
                    if st.button(
                        PAGE_LABELS.get(page_key, page_key),
                        key=f"nav_{page_key}",
                        type=PAGE_BUTTON_TYPES.get(page_key, "secondary"),
                        width="stretch",
                    ):
                        selected_page = page_key
                        st.session_state["collapse_nav_once"] = True
                        st.session_state["collapse_sidebar_once"] = True
                        st.session_state["collapse_sidebar_nonce"] = (
                            st.session_state.get("collapse_sidebar_nonce", 0) + 1
                        )

        if collapse_nav_once:
            st.session_state["collapse_nav_once"] = False

        st.markdown('<div class="tf-nav-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="tf-nav-title">Session</div>', unsafe_allow_html=True)
        asyncio.run(logout(st, host))
        return selected_page


async def _dispatch_page(host: str, selected_page: str | None) -> None:
    """Dispatch selected page handler.

    Args:
        host (str): API base URL for backend page calls.
        selected_page (str | None): Selected page key from sidebar.

    Returns:
        None: Renders page UI as side effect.
    """
    if selected_page == "google_ads_token":
        st.session_state.page = selected_page
        await google_ads_token.show_google_ads_token_page(host)
        return
    elif selected_page == "meta_ads_token":
        st.session_state.page = selected_page
        await meta_ads_token.show_meta_ads_token_page(host)
        return

    if not st.session_state.logged_in:
        await login.show_login_page(host)
        return

    if not selected_page:
        st.error("Unable to determine page selection.")
        return

    page_handlers: dict[str, PageHandler] = {
        "home": home.show_home_page,
        "overview": overview.show_overview_page,
        "user_acquisition": user_acquisition.show_user_acquisition_page,
        "brand_awareness": brand_awareness.show_brand_awareness_page,
        "deposit_report": deposit.show_deposit_page,
        "register": register.create_account,
        "update_data": update_data.show_update_page,
        "google_ads_token": google_ads_token.show_google_ads_token_page,
        "meta_ads_token": meta_ads_token.show_meta_ads_token_page,
    }

    handler = page_handlers.get(selected_page)
    if handler is None:
        st.error("Page is not available.")
        return

    st.session_state.page = selected_page
    await handler(host)


def main() -> None:
    """Run dashboard app lifecycle and dispatch selected page.

    Returns:
        None: Renders Streamlit application as side effects.
    """
    _inject_navigation_style()
    _initialize_session_state()
    _restore_login_state_from_cookie()
    host = _resolve_host()
    footer(st)

    public_page = _resolve_public_page_from_query_params()
    selected_page = public_page
    if st.session_state.logged_in and public_page is None:
        selected_page = _render_sidebar_navigation(host=host)
        _collapse_sidebar_if_requested()
    else:
        _hide_sidebar_on_login()

    try:
        asyncio.run(_dispatch_page(host=host, selected_page=selected_page))
    except Exception as error:
        st.error(f"Error fetching data: {error}")


if __name__ == "__main__":
    main()
