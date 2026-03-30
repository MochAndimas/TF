"""Login module.

This module is part of `streamlit_app.page` and contains runtime logic used by the
Traders Family application.
"""

import streamlit as st

from streamlit_app.functions.runtime import (
    apply_auth_payload,
    consume_auth_bridge_response,
    start_auth_bridge_request,
)

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
    """Extract the most useful human-readable error text from an API payload."""
    return payload.get("detail") or payload.get("message") or default


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

    pending_request = st.session_state.get("auth_bridge_request")
    if pending_request and pending_request.get("action") == "login":
        bridge_response = consume_auth_bridge_response(host, component_key="auth_login_bridge")
        if bridge_response is None:
            st.caption("Signing in...")
            return

        login_payload = bridge_response.get("payload", {})
        if not bridge_response.get("ok") or not login_payload.get("success"):
            st.error(_extract_error_message(login_payload, bridge_response.get("error") or "Invalid email or password."))
            return

        if not login_payload.get("user_id") or not login_payload.get("access_token"):
            st.error("Login response is incomplete. Please try again.")
            return

        apply_auth_payload(login_payload)
        st.session_state.page = "home"
        st.rerun()

    if not submitted:
        return

    email = email.strip()
    if not email or not password:
        st.warning("Email and password are required.")
        return

    start_auth_bridge_request(
        "login",
        {"email": email, "password": password, "remember": remember},
    )
    st.rerun()
