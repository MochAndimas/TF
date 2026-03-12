"""Google Ads OAuth page for generating a refresh token."""

from __future__ import annotations

import streamlit as st
from decouple import config as env

def _load_client_config() -> dict | None:
    """Load OAuth client configuration from environment variables only."""
    client_id = env("GOOGLE_ADS_CLIENT_ID", default="", cast=str).strip()
    client_secret = env("GOOGLE_ADS_CLIENT_SECRET", default="", cast=str).strip()
    if client_id and client_secret:
        return {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [],
            }
        }

    return None


def _default_redirect_uri() -> str:
    """Resolve default redirect URI for the current Streamlit app."""
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
    """Resolve backend URL that initiates the OAuth flow."""
    backend_public_url = env("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return f"{backend_public_url.rstrip('/')}/api/google-ads/oauth/start"
    backend_host = env("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = env("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return f"{backend_host.rstrip('/')}/api/google-ads/oauth/start"
    return f"http://{backend_host}:{backend_port}/api/google-ads/oauth/start"


async def show_google_ads_token_page(host: str) -> None:
    """Render Google Ads OAuth helper page."""
    del host

    st.title("Google Ads Refresh Token")
    st.caption("Page ini memulai Google OAuth via backend callback supaya query callback tidak hilang di Streamlit.")

    client_config = _load_client_config()
    if client_config is None:
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
        "Sesudah approve access, Google akan redirect ke endpoint backend callback dan refresh token akan tampil di sana."
    )
