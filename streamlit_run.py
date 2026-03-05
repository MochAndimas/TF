"""Main Streamlit entrypoint for Traders Family dashboard navigation."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

import streamlit as st
from streamlit.components.v1 import html
from decouple import config

from streamlit_app.functions.utils import cookie_controller, footer, get_session, logout
from streamlit_app.page import campaign_ads, login, overall, register, update_data

st.set_page_config(
    page_title="Traders Family Dashboard",
    page_icon="./streamlit_app/page/logotf.png",
    layout="wide",
    initial_sidebar_state="collapsed",
)

PageHandler = Callable[[str], Awaitable[None]]

PAGE_LABELS: dict[str, str] = {
    "user_acquisition": "User Acquisition",
    "register": "Create Account",
    "update_data": "Update Data",
}

PAGE_BUTTON_TYPES: dict[str, str] = {
    "user_acquisition": "tertiary",
    "register": "tertiary",
    "update_data": "tertiary",
}

NAV_GROUPS: dict[str, list[str]] = {
    "Campaign": ["user_acquisition"],
    "Settings": ["register", "update_data"],
}

ROLE_PAGE_ACCESS: dict[str, list[str]] = {
    "superadmin": ["user_acquisition", "register", "update_data"],
    "admin": ["overall", "user_acquisition"],
    "digital_marketing": ["overall", "user_acquisition"],
    "sales": ["overall", "user_acquisition"],
}


def _inject_navigation_style() -> None:
    """Render minimal sidebar style for cleaner navigation hierarchy."""
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
    """Best-effort sidebar auto-collapse triggered after page navigation click."""
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


def _resolve_host() -> str:
    """Resolve backend host URL from environment-aware Streamlit secrets.

    Returns:
        str: API base URL used by all Streamlit page handlers.
    """
    env_name = config("ENV", default="development", cast=str).lower()
    if env_name == "production":
        return st.secrets["api"]["HOST"]
    return st.secrets["api"]["DEV_HOST"]


def _initialize_session_state() -> None:
    """Initialize required Streamlit state keys with safe defaults."""
    defaults = {
        "page": "overall",
        "logged_in": False,
        "role": None,
    }
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value


def _restore_login_state_from_cookie() -> None:
    """Restore login/session state from persisted browser cookie when available."""
    if st.session_state.get("logged_in") and st.session_state.get("_user_id"):
        return

    session_cookie = cookie_controller.get("session_id")
    get_session(session_cookie)
    _initialize_session_state()


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
    with st.sidebar:
        if not st.session_state.logged_in:
            st.markdown('<div class="tf-nav-divider"></div>', unsafe_allow_html=True)
            st.info("Please sign in to access dashboard pages.")
            return None

        st.image("./streamlit_app/page/logotf.png", width=96)
        available_pages = _allowed_pages_for_role(st.session_state.role)
        if not available_pages:
            st.warning("No page access configured for your account role.")
            asyncio.run(logout(st, host, None))
            return None

        current_page = st.session_state.page
        if current_page not in available_pages:
            current_page = available_pages[0]
            st.session_state.page = current_page

        st.markdown('<p class="tf-nav-title">Navigation</p>', unsafe_allow_html=True)
        collapse_nav_once = st.session_state.get("collapse_nav_once", False)
        selected_page = current_page

        for group_title, group_pages in NAV_GROUPS.items():
            visible_pages = [page_key for page_key in group_pages if page_key in available_pages]
            if not visible_pages:
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
        asyncio.run(logout(st, host, None))
        return selected_page


async def _dispatch_page(host: str, selected_page: str | None) -> None:
    """Dispatch selected page handler.

    Args:
        host (str): API base URL for backend page calls.
        selected_page (str | None): Selected page key from sidebar.

    Returns:
        None: Renders page UI as side effect.
    """
    if not st.session_state.logged_in:
        await login.show_login_page(host)
        return

    if not selected_page:
        st.error("Unable to determine page selection.")
        return

    page_handlers: dict[str, PageHandler] = {
        "user_acquisition": campaign_ads.show_user_acquisition_page,
        "register": register.create_account,
        "update_data": update_data.show_update_page,
    }

    handler = page_handlers.get(selected_page)
    if handler is None:
        st.error("Page is not available.")
        return

    st.session_state.page = selected_page
    await handler(host)


def main() -> None:
    """Run Streamlit dashboard app with role-based navigation."""
    _inject_navigation_style()
    _initialize_session_state()
    _restore_login_state_from_cookie()
    host = _resolve_host()
    footer(st)

    selected_page = _render_sidebar_navigation(host=host)
    _collapse_sidebar_if_requested()

    try:
        asyncio.run(_dispatch_page(host=host, selected_page=selected_page))
    except Exception as error:
        st.error(f"Error fetching data: {error}")


if __name__ == "__main__":
    main()
