"""Sidebar navigation rendering for the Streamlit shell."""

from __future__ import annotations

import asyncio

import streamlit as st
from streamlit.components.v1 import html

from streamlit_app.app_shell.config import NAV_GROUPS, PAGE_BUTTON_TYPES, PAGE_LABELS, ROLE_PAGE_ACCESS
from streamlit_app.app_shell.session import resolve_public_page_from_query_params
from streamlit_app.functions.api import logout


def inject_navigation_style() -> None:
    """Inject custom CSS for sidebar visual hierarchy."""
    st.markdown(
        """
        <style>
            .tf-nav-title {
                font-size: 1.2rem;
                font-weight: 700;
                text-transform: none;
                letter-spacing: 0.01em;
                opacity: 0.92;
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


def collapse_sidebar_if_requested() -> None:
    """Run best-effort sidebar auto-collapse after navigation click."""
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
    html(script.replace("__NONCE__", str(nonce)), height=0, width=0)
    st.session_state["collapse_sidebar_once"] = False


def hide_sidebar_on_login() -> None:
    """Hide sidebar elements when user is on login view."""
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


def allowed_pages_for_role(role: str | None) -> list[str]:
    """Get allowed page keys based on active authenticated role."""
    return ROLE_PAGE_ACCESS.get(role or "", [])


def _mark_navigation_change(selected_page: str) -> str:
    st.session_state["collapse_nav_once"] = True
    st.session_state["collapse_sidebar_once"] = True
    st.session_state["collapse_sidebar_nonce"] = st.session_state.get("collapse_sidebar_nonce", 0) + 1
    return selected_page


def render_sidebar_navigation(host: str) -> str | None:
    """Render sidebar navigation and return selected page key."""
    public_page = resolve_public_page_from_query_params()
    if public_page:
        return public_page

    available_pages = allowed_pages_for_role(st.session_state.role)
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

        st.markdown('<p class="tf-nav-title">Traders Family</p>', unsafe_allow_html=True)
        collapse_nav_once = st.session_state.get("collapse_nav_once", False)
        selected_page = current_page

        if "home" in available_pages:
            st.markdown('<div class="tf-nav-standalone">', unsafe_allow_html=True)
            if st.button(PAGE_LABELS["home"], key="nav_home", type=PAGE_BUTTON_TYPES.get("home", "secondary"), width="stretch"):
                selected_page = _mark_navigation_change("home")
            st.markdown("</div>", unsafe_allow_html=True)

        for group_title, group_pages in NAV_GROUPS.items():
            visible_pages = [page_key for page_key in group_pages if page_key in available_pages]
            if not visible_pages or visible_pages == ["home"]:
                continue
            group_expanded = False if collapse_nav_once else (current_page in visible_pages)
            with st.expander(group_title, expanded=group_expanded):
                for page_key in visible_pages:
                    if st.button(PAGE_LABELS.get(page_key, page_key), key=f"nav_{page_key}", type=PAGE_BUTTON_TYPES.get(page_key, "secondary"), width="stretch"):
                        selected_page = _mark_navigation_change(page_key)

        if collapse_nav_once:
            st.session_state["collapse_nav_once"] = False

        st.markdown('<div class="tf-nav-divider"></div>', unsafe_allow_html=True)
        asyncio.run(logout(st, host))
        return selected_page
