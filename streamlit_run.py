"""Main Streamlit entrypoint for Traders Family dashboard navigation.

This module defines:
    - role-based page access configuration,
    - sidebar grouping and navigation behavior,
    - page dispatching into async page handlers.
"""

from __future__ import annotations

import asyncio

import streamlit as st

from streamlit_app.app_shell.config import PAGE_HANDLERS, PUBLIC_PAGE_KEYS
from streamlit_app.app_shell.navigation import (
    collapse_sidebar_if_requested,
    hide_sidebar_on_login,
    inject_navigation_style,
    render_sidebar_navigation,
)
from streamlit_app.app_shell.session import (
    initialize_session_state,
    resolve_host,
    resolve_public_page_from_query_params,
    restore_login_state_from_cookie,
)
from streamlit_app.functions.ui import footer
from streamlit_app.page import login

st.set_page_config(
    page_title="Traders Family Dashboard",
    page_icon="./streamlit_app/page/logotf.png",
    layout="wide",
    initial_sidebar_state="collapsed",
)



async def _dispatch_page(host: str, selected_page: str | None) -> None:
    """Dispatch selected page handler.

    Args:
        host (str): API base URL for backend page calls.
        selected_page (str | None): Selected page key from sidebar.

    Returns:
        None: Renders page UI as side effect.
    """
    if selected_page in PUBLIC_PAGE_KEYS:
        st.session_state.page = selected_page
        await PAGE_HANDLERS[selected_page](host)
        return

    if not st.session_state.logged_in:
        await login.show_login_page(host)
        return

    if not selected_page:
        st.error("Unable to determine page selection.")
        return

    handler = PAGE_HANDLERS.get(selected_page)
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
    inject_navigation_style()
    initialize_session_state()
    host = resolve_host()
    restore_login_state_from_cookie(host)
    footer(st)

    public_page = resolve_public_page_from_query_params()
    selected_page = public_page
    if st.session_state.logged_in and public_page is None:
        selected_page = render_sidebar_navigation(host=host)
        collapse_sidebar_if_requested()
    else:
        hide_sidebar_on_login()

    try:
        asyncio.run(_dispatch_page(host=host, selected_page=selected_page))
    except Exception as error:
        st.error(f"Error fetching data: {error}")


if __name__ == "__main__":
    main()
