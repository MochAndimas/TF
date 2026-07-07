"""TikTok OAuth page for securely connecting an authorized TikTok account."""

from __future__ import annotations

import httpx
import streamlit as st
from decouple import config as env

from streamlit_app.functions.runtime import get_access_token, resolve_backend_base_url


DEFAULT_SCOPES = "user.info.basic, user.info.profile, user.info.stats, video.list"


def _default_redirect_uri() -> str:
    """Resolve the callback URI users must register in TikTok Developer."""
    configured = env("TIKTOK_REDIRECT_URI", default="", cast=str).strip()
    if configured:
        return configured
    backend_public_url = env("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return f"{backend_public_url.rstrip('/')}/api/tiktok/oauth/callback"
    backend_host = env("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = env("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return f"{backend_host.rstrip('/')}/api/tiktok/oauth/callback"
    return f"http://{backend_host}:{backend_port}/api/tiktok/oauth/callback"


def _oauth_start_url() -> str:
    """Resolve the authenticated backend endpoint that starts TikTok OAuth."""
    return f"{resolve_backend_base_url(prefer_internal=True)}/api/tiktok/oauth/start"


async def show_tiktok_token_page(host: str) -> None:
    """Render the operational page used to connect a TikTok account."""
    del host

    st.title("TikTok Token")
    st.caption(
        "Hubungkan akun TikTok lewat OAuth/Login Kit. Access token dan refresh "
        "token disimpan terenkripsi dan tidak ditampilkan di dashboard."
    )

    client_key = env("TIKTOK_CLIENT_KEY", default="", cast=str).strip()
    client_secret = env("TIKTOK_CLIENT_SECRET", default="", cast=str).strip()
    if not client_key or not client_secret:
        st.error(
            "Konfigurasi belum lengkap. Set `TIKTOK_CLIENT_KEY` dan "
            "`TIKTOK_CLIENT_SECRET` di environment backend."
        )
        return

    redirect_uri = _default_redirect_uri()
    scopes = env("TIKTOK_SCOPES", default=DEFAULT_SCOPES, cast=str).strip()
    st.info(
        "Pastikan Redirect URI di TikTok Developer sama persis dengan "
        f"`{redirect_uri}`."
    )
    st.caption(f"Configured scopes: `{scopes}`")

    access_token = get_access_token()
    if not access_token:
        st.error("Session login tidak ditemukan. Silakan login ulang.")
        return

    with st.container(border=True):
        st.markdown("### Connect TikTok")
        st.write(
            "Gunakan akun TikTok yang datanya mau ditarik, lalu approve "
            "permission yang diminta di halaman TikTok."
        )

        if st.button("Generate TikTok OAuth Link", type="primary", width="stretch"):
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
                    st.session_state["tiktok_authorization_url"] = payload.get(
                        "authorization_url"
                    )
                    st.session_state["tiktok_redirect_uri"] = payload.get("redirect_uri")
                    st.session_state["tiktok_scopes"] = payload.get("scopes") or []

        authorization_url = st.session_state.get("tiktok_authorization_url")
        if authorization_url:
            st.success("OAuth link siap dipakai.")
            st.link_button("Continue to TikTok OAuth", authorization_url, type="primary")

            generated_redirect = st.session_state.get("tiktok_redirect_uri")
            generated_scopes = st.session_state.get("tiktok_scopes") or []
            if generated_redirect:
                st.caption(f"Redirect URI: `{generated_redirect}`")
            if generated_scopes:
                st.caption(f"Scopes: `{', '.join(generated_scopes)}`")
        else:
            st.caption("Consent link akan muncul setelah berhasil dibuat.")

    st.caption(
        "Callback akan menukar authorization code menjadi token, memvalidasi "
        "akun TikTok, lalu menyimpan token terenkripsi di backend."
    )
