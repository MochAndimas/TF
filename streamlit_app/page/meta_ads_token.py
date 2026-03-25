"""Meta Ads token exchange page for storing a long-lived token in backend storage."""

from __future__ import annotations

import httpx
import streamlit as st
from decouple import config as env

from streamlit_app.functions.utils import refresh_backend_tokens


def _internal_backend_url() -> str:
    """Resolve backend base URL for server-side calls from the Streamlit container."""
    internal_api_host = env("STREAMLIT_API_HOST", default="", cast=str).strip()
    if internal_api_host:
        return internal_api_host.rstrip("/")

    backend_public_url = env("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return backend_public_url.rstrip("/")

    backend_host = env("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = env("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return backend_host.rstrip("/")
    return f"http://{backend_host}:{backend_port}"


def _meta_status_url() -> str:
    """Resolve backend URL used to inspect current Meta token status."""
    return f"{_internal_backend_url()}/api/meta-ads/token/status"


def _meta_exchange_url() -> str:
    """Resolve backend URL used to exchange a Meta token."""
    return f"{_internal_backend_url()}/api/meta-ads/token/exchange"


async def _fetch_status(access_token: str) -> dict:
    """Fetch current Meta token storage status from backend."""
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(
            _meta_status_url(),
            headers={"Authorization": f"Bearer {access_token}"},
        )
    return response.json() if response.content else {}


async def _authorized_request(
    method: str,
    url: str,
    access_token: str,
    *,
    json_payload: dict | None = None,
) -> tuple[httpx.Response, dict]:
    """Send an authenticated request and refresh tokens once on 401."""
    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.request(
            method=method,
            url=url,
            headers=headers,
            json=json_payload,
        )

        if response.status_code == 401 and st.session_state.get("refresh_token"):
            refreshed_payload = await refresh_backend_tokens(
                host=_internal_backend_url(),
                refresh_token=st.session_state["refresh_token"],
            )
            if refreshed_payload and refreshed_payload.get("success"):
                st.session_state.access_token = refreshed_payload.get("access_token")
                st.session_state.refresh_token = refreshed_payload.get("refresh_token")
                headers["Authorization"] = f"Bearer {st.session_state.access_token}"
                response = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json_payload,
                )

    payload = response.json() if response.content else {}
    return response, payload


async def show_meta_ads_token_page(host: str) -> None:
    """Render helper page for exchanging and storing Meta long-lived token."""
    del host

    st.title("Meta Ads Token")
    st.caption("Paste short-lived user token, backend akan tukar ke long-lived token dan simpan encrypted di database.")

    app_id = env("META_APP_ID", default="", cast=str).strip()
    app_secret = env("META_APP_SECRET", default="", cast=str).strip()
    if not app_id or not app_secret:
        st.error("Meta app config belum lengkap. Set `META_APP_ID` dan `META_APP_SECRET` di backend env.")
        return

    access_token = st.session_state.get("access_token")
    if not access_token:
        st.error("Session is invalid. Please log in again.")
        return

    status_data: dict = {}
    try:
        response, status_data = await _authorized_request(
            "GET",
            _meta_status_url(),
            access_token,
        )
        if response.status_code == 401:
            st.error("Session login sudah expired. Silakan login ulang.")
            return
    except Exception as error:  # noqa: BLE001
        st.warning(f"Gagal ambil status token saat ini: {error}")

    with st.container(border=True):
        st.markdown("### App Config")
        st.write(f"`META_APP_ID`: `{app_id}`")
        st.write("`META_APP_SECRET` terbaca di backend.")

    if status_data.get("configured"):
        st.success(
            "Long-lived token sudah tersimpan. "
            f"Last update: `{status_data.get('updated_at') or '-'}`"
        )
    else:
        st.info("Belum ada long-lived token Meta yang tersimpan di database.")

    with st.form("meta_ads_token_exchange_form"):
        short_lived_token = st.text_area(
            "Short-lived User Token",
            height=180,
            help="Token mentah hanya dikirim ke backend untuk ditukar. UI tidak akan menyimpan token long-lived.",
        ).strip()
        submitted = st.form_submit_button("Exchange and Save", type="primary", use_container_width=True)

    if not submitted:
        return

    if not short_lived_token:
        st.warning("Isi short-lived token dulu.")
        return

    with st.spinner("Exchanging Meta token..."):
        try:
            response, data = await _authorized_request(
                "POST",
                _meta_exchange_url(),
                st.session_state.get("access_token") or access_token,
                json_payload={"short_lived_token": short_lived_token},
            )
        except Exception as error:  # noqa: BLE001
            st.error(f"Token exchange gagal: {error}")
            return

    if response.status_code >= 400:
        message = data.get("detail") or data.get("message") or "Meta token exchange failed."
        st.error(message)
        return

    st.success(data.get("message", "Meta long-lived token berhasil disimpan."))
    if data.get("expires_in") is not None:
        st.caption(f"expires_in: `{data['expires_in']}` seconds")
    if data.get("updated_at"):
        st.caption(f"stored_at: `{data['updated_at']}`")
