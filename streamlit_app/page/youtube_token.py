"""YouTube OAuth page for securely connecting the configured channel."""

from __future__ import annotations

import httpx
import streamlit as st
from decouple import config as env

from streamlit_app.functions.runtime import get_access_token, resolve_backend_base_url


def _default_redirect_uri() -> str:
    """Resolve the callback URI users must register in Google Cloud."""
    configured = env("YOUTUBE_REDIRECT_URI", default="", cast=str).strip()
    if configured:
        return configured
    backend_public_url = env("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return f"{backend_public_url.rstrip('/')}/api/youtube/oauth/callback"
    backend_host = env("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = env("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return f"{backend_host.rstrip('/')}/api/youtube/oauth/callback"
    return f"http://{backend_host}:{backend_port}/api/youtube/oauth/callback"


def _oauth_start_url() -> str:
    """Resolve the authenticated backend endpoint that starts YouTube OAuth."""
    return f"{resolve_backend_base_url(prefer_internal=True)}/api/youtube/oauth/start"


async def show_youtube_token_page(host: str) -> None:
    """Render the operational page used to connect a YouTube channel."""
    del host

    st.title("YouTube Refresh Token")
    st.caption(
        "Hubungkan channel melalui callback backend. Refresh token disimpan "
        "terenkripsi dan tidak pernah ditampilkan di dashboard."
    )

    client_id = env("YOUTUBE_CLIENT_ID", default="", cast=str).strip()
    client_secret = env("YOUTUBE_CLIENT_SECRET", default="", cast=str).strip()
    channel_id = env("YOUTUBE_CHANNEL_ID", default="", cast=str).strip()
    if not client_id or not client_secret or not channel_id:
        st.error(
            "Konfigurasi belum lengkap. Set `YOUTUBE_CLIENT_ID`, "
            "`YOUTUBE_CLIENT_SECRET`, dan `YOUTUBE_CHANNEL_ID`."
        )
        return

    redirect_uri = _default_redirect_uri()
    st.info(
        "Pastikan Authorized redirect URI di Google Cloud sama persis dengan "
        f"`{redirect_uri}`."
    )
    st.caption(f"Configured Channel ID: `{channel_id}`")

    access_token = get_access_token()
    if not access_token:
        st.error("Session login tidak ditemukan. Silakan login ulang.")
        return

    with st.container(border=True):
        st.markdown("### Connect YouTube")
        st.write(
            "Gunakan akun Google yang memiliki akses Brand Account, lalu pilih "
            "channel yang sesuai saat consent."
        )

        if st.button("Generate YouTube OAuth Link", type="primary", width="stretch"):
            try:
                async with httpx.AsyncClient(timeout=120) as client:
                    response = await client.post(
                        _oauth_start_url(),
                        headers={"Authorization": f"Bearer {access_token}"},
                    )
                payload = response.json() if response.content else {}
            except httpx.RequestError as error:
                st.error(f"Gagal menghubungi backend OAuth: {error}")
            else:
                if response.status_code >= 400 or not payload.get("success"):
                    detail = payload.get("detail") or payload.get("message") or "Gagal membuat OAuth link."
                    st.error(detail)
                else:
                    st.session_state["youtube_authorization_url"] = payload.get(
                        "authorization_url"
                    )

        authorization_url = st.session_state.get("youtube_authorization_url")
        if authorization_url:
            st.success("OAuth link siap dipakai.")
            st.link_button("Continue to YouTube OAuth", authorization_url, type="primary")
        else:
            st.caption("Consent link akan muncul setelah berhasil dibuat.")

    st.caption(
        "Callback akan memvalidasi Channel ID sebelum menyimpan refresh token."
    )
