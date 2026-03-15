"""Login module.

This module is part of `streamlit_app.page` and contains runtime logic used by the
Traders Family application.
"""

import httpx
import streamlit as st
from datetime import datetime, timedelta
from urllib.parse import urlparse
from streamlit_app.functions.utils import cookie_controller

LOGIN_STYLE = """
<style>
.tf-auth-wrap {
    max-width: 680px;
    margin: 2rem auto 0 auto;
}
.tf-auth-card {
    border: 1px solid var(--secondary-background-color);
    border-radius: 16px;
    padding: 1.2rem 1.1rem 1rem 1.1rem;
    background: var(--background-color);
}
.tf-auth-title {
    font-size: 1.45rem;
    font-weight: 700;
    letter-spacing: 0.2px;
    margin: 0;
    color: var(--text-color);
}
.tf-auth-subtitle {
    font-size: 0.95rem;
    color: var(--text-color);
    opacity: 0.82;
    margin-top: 0.2rem;
    margin-bottom: 0.6rem;
}
.tf-auth-meta {
    font-size: 0.82rem;
    color: var(--text-color);
    opacity: 0.72;
}
div[data-testid="stForm"] {
    border: 0;
    padding: 0;
    background: transparent;
}
</style>
"""


def _extract_error_message(payload: dict, default: str) -> str:
    """Extract the most useful human-readable error text from an API payload.

    Args:
        payload (dict): JSON response payload returned by the backend.
        default (str): Fallback message used when the payload has no useful
            ``detail`` or ``message`` field.

    Returns:
        str: Best-effort error message for FE display.
    """
    return payload.get("detail") or payload.get("message") or default


def _cookie_options_from_host(host_url: str) -> dict[str, object]:
    """Build cookie options that behave correctly for localhost and HTTPS hosts."""
    parsed = urlparse(host_url)
    hostname = parsed.hostname
    secure = parsed.scheme == "https"
    options: dict[str, object] = {
        "path": "/",
        "same_site": "strict",
        "secure": secure,
    }
    if hostname and hostname not in {"localhost", "127.0.0.1"}:
        options["domain"] = hostname
    return options


async def _request_login(
    client: httpx.AsyncClient,
    host: str,
    email: str,
    password: str,
) -> tuple[dict, httpx.Response]:
    """Send the authenticated login request.

    Args:
        client (httpx.AsyncClient): Shared HTTP client used for auth requests.
        host (str): Backend API base URL.
        email (str): Submitted login email.
        password (str): Submitted login password.
    Returns:
        tuple[dict, httpx.Response]: Parsed JSON payload plus the raw HTTP
        response object so the caller can inspect headers and cookies.
    """
    response = await client.post(
        f"{host}/api/login",
        data={"username": email, "password": password},
    )
    payload = response.json() if response.content else {}
    return payload, response


async def show_login_page(host):
    """Render login form and authenticate user.

    Args:
        host (str): Base URL of backend API service.

    Returns:
        None: UI side effects only; updates Streamlit session state on success.
    """
    st.markdown(LOGIN_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="tf-auth-wrap">', unsafe_allow_html=True)
    st.markdown('<div class="tf-auth-card">', unsafe_allow_html=True)
    st.image("./streamlit_app/page/tf_wide.png", width=260)
    st.markdown(
        '<p class="tf-auth-subtitle" style="font-size:1.2rem;font-weight:700;letter-spacing:0.2px;'
        'margin-top:0.35rem;margin-bottom:1rem;opacity:0.95;">Campaign Dashboard</p>',
        unsafe_allow_html=True,
    )

    with st.form("log-in", border=False):
        email = st.text_input("Email", placeholder="name@company.com")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        remember = st.checkbox("Keep me signed in for 7 days")
        submitted = st.form_submit_button("Sign In", width="stretch")

    st.markdown("</div></div>", unsafe_allow_html=True)

    if not submitted:
        return

    email = email.strip()
    if not email or not password:
        st.warning("Email and password are required.")
        return

    with st.spinner("Signing in..."):
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                login_payload, response = await _request_login(
                    client=client,
                    host=host,
                    email=email,
                    password=password,
                )
        except httpx.RequestError as error:
            st.error(f"Unable to reach authentication service: {error}")
            return

    if not login_payload.get("success"):
        st.error(_extract_error_message(login_payload, "Invalid email or password."))
        return

    user_id = login_payload.get("user_id") or response.headers.get("Authentication")
    access_token = login_payload.get("access_token")
    refresh_token = login_payload.get("refresh_token")
    if not user_id or not access_token or not refresh_token:
        st.error("Login response is incomplete. Please try again.")
        return

    st.session_state.role = login_payload.get("role")
    st.session_state.logged_in = True
    st.session_state.page = "home"
    st.session_state._user_id = user_id
    st.session_state.access_token = access_token
    st.session_state.refresh_token = refresh_token
    st.session_state.session_id = login_payload.get("session_id")

    if remember:
        cookie_options = _cookie_options_from_host(st.secrets["api"]["HOST"])
        cookie_controller.set(
            name="refresh_token",
            value=refresh_token,
            expires=datetime.now() + timedelta(days=7),
            **cookie_options,
        )

    st.rerun()
