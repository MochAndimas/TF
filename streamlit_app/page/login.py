import httpx
import streamlit as st
from datetime import datetime, timedelta
from streamlit_app.functions.utils import cookie_controller, get_user

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
    """Extract the most useful error message from API payload."""
    return payload.get("detail") or payload.get("message") or default


async def _request_csrf_token(
    client: httpx.AsyncClient,
    host: str,
    email: str,
    password: str,
) -> tuple[str | None, str | None, str | None]:
    """Initialize CSRF token and return `(csrf_token, server_session, error_message)`."""
    response = await client.post(
        f"{host}/api/login/csrf-token",
        data={"username": email, "password": password},
    )
    payload = response.json() if response.content else {}

    if response.status_code >= 400:
        return None, None, _extract_error_message(payload, "Failed to initialize CSRF token.")

    csrf_token = response.cookies.get("csrf_token") or client.cookies.get("csrf_token")
    server_session = client.cookies.get("session")
    if not csrf_token:
        return None, None, "CSRF token initialization failed. Please try again."

    return csrf_token, server_session, None


async def _request_login(
    client: httpx.AsyncClient,
    host: str,
    email: str,
    password: str,
    csrf_token: str,
) -> tuple[dict, httpx.Response]:
    """Send login request and return `(payload, response)`."""
    response = await client.post(
        f"{host}/api/login",
        data={"username": email, "password": password},
        cookies={"csrf_token": csrf_token},
        headers={"X-CSRF-Token": csrf_token},
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
    top_col, title_col = st.columns([1, 5], vertical_alignment="center")
    with top_col:
        st.image("./streamlit_app/page/logotf.png", width=58)
    with title_col:
        st.markdown('<p class="tf-auth-title">Traders Family Dashboard</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="tf-auth-subtitle">Sign in to see campaign data.</p>',
            unsafe_allow_html=True,
        )

    with st.form("log-in", border=False):
        email = st.text_input("Email", placeholder="name@company.com")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        remember = st.checkbox("Keep me signed in for 7 days")
        submitted = st.form_submit_button("Sign In", use_container_width=True)
    st.markdown('<p class="tf-auth-meta">Secure login protected with CSRF validation.</p>', unsafe_allow_html=True)
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
                csrf_token, server_session, csrf_error = await _request_csrf_token(
                    client=client,
                    host=host,
                    email=email,
                    password=password,
                )
                if csrf_error:
                    st.error(csrf_error)
                    return

                login_payload, response = await _request_login(
                    client=client,
                    host=host,
                    email=email,
                    password=password,
                    csrf_token=csrf_token,
                )
        except httpx.RequestError as error:
            st.error(f"Unable to reach authentication service: {error}")
            return

    if not login_payload.get("success"):
        st.error(_extract_error_message(login_payload, "Invalid email or password."))
        return

    user_id = response.headers.get("Authentication")
    if not user_id:
        st.error("Login response is incomplete. Please try again.")
        return

    user = get_user(user_id)
    if user is None:
        st.error("Session record not found. Please try again.")
        return

    st.session_state.role = login_payload.get("role")
    st.session_state.logged_in = True
    st.session_state.page = "home"
    st.session_state._user_id = user_id
    st.session_state.csrf_token = csrf_token
    st.session_state.server_session = server_session

    if remember:
        cookie_controller.set(
            name="session_id",
            value=user.session_id,
            path="/",
            expires=datetime.now() + timedelta(days=7),
            domain=st.secrets["api"]["HOST"],
            same_site="strict",
            secure=True,
        )

    st.rerun()
