"""Google Ads OAuth page for generating a refresh token."""

from __future__ import annotations

import streamlit as st
from decouple import config as env

def _default_redirect_uri() -> str:
    """Resolve the callback URI users should register in Google Cloud.

    The helper mirrors backend callback resolution so the Streamlit helper page
    can display the same expected redirect target the FastAPI OAuth endpoint
    uses during the Google consent flow.

    Returns:
        str: Fully qualified callback URL pointing to
        ``/api/google-ads/oauth/callback`` on the configured backend host.
    """
    configured_redirect = env("GOOGLE_ADS_REDIRECT_URI", default="", cast=str).strip()
    if configured_redirect:
        return configured_redirect
    backend_public_url = env("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return f"{backend_public_url.rstrip('/')}/api/google-ads/oauth/callback"
    backend_host = env("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = env("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return f"{backend_host.rstrip('/')}/api/google-ads/oauth/callback"
    return f"http://{backend_host}:{backend_port}/api/google-ads/oauth/callback"


def _oauth_start_url() -> str:
    """Resolve the backend endpoint that starts the Google OAuth redirect.

    Returns:
        str: Fully qualified URL for ``/api/google-ads/oauth/start`` on the
        current backend host, used by the Streamlit page's primary action
        button.
    """
    backend_public_url = env("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return f"{backend_public_url.rstrip('/')}/api/google-ads/oauth/start"
    backend_host = env("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = env("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return f"{backend_host.rstrip('/')}/api/google-ads/oauth/start"
    return f"http://{backend_host}:{backend_port}/api/google-ads/oauth/start"


async def show_google_ads_token_page(host: str) -> None:
    """Render the operational helper page for Google Ads OAuth setup.

    This page does not handle the token exchange itself. Instead, it educates
    the operator about the expected redirect URI and launches the backend-owned
    OAuth start endpoint so the refresh token can be stored server-side without
    ever being rendered back into the Streamlit UI.

    Args:
        host (str): Backend host passed by the main Streamlit dispatcher. The
            current implementation derives display/start URLs from environment
            settings instead, so the value is intentionally unused here.

    Returns:
        None: Writes Streamlit content and navigation controls as UI side
        effects.
    """
    del host

    st.title("Google Ads Refresh Token")
    st.caption("Page ini memulai Google OAuth via backend callback supaya query callback tidak hilang di Streamlit.")

    client_id = env("GOOGLE_ADS_CLIENT_ID", default="", cast=str).strip()
    client_secret = env("GOOGLE_ADS_CLIENT_SECRET", default="", cast=str).strip()
    if not client_id or not client_secret:
        st.error(
            "OAuth client config tidak ditemukan. Set "
            "`GOOGLE_ADS_CLIENT_ID` dan `GOOGLE_ADS_CLIENT_SECRET`."
        )
        return

    default_redirect_uri = st.session_state.get("google_ads_oauth_redirect_uri", _default_redirect_uri())
    redirect_uri = st.text_input(
        "Redirect URI",
        value=default_redirect_uri,
        help="Harus sama persis dengan Authorized redirect URI di Google Cloud OAuth client.",
    ).strip()

    st.info(
        "Flow yang stabil pakai callback backend, misalnya "
        "`http://localhost:8000/api/google-ads/oauth/callback`."
    )
    st.caption(f"Active redirect URI: `{redirect_uri}`")

    if not redirect_uri:
        st.warning("Isi redirect URI dulu.")
        return

    start_url = _oauth_start_url()

    with st.container(border=True):
        st.markdown("### Start OAuth")
        st.write("Klik tombol di bawah untuk mulai OAuth dari backend FastAPI.")

        st.link_button("Start Google OAuth", start_url, type="primary")
        st.code(start_url, language="text")

    st.caption(
        "Sesudah approve access, Google akan redirect ke endpoint backend callback dan token akan disimpan di backend tanpa ditampilkan ke UI."
    )
