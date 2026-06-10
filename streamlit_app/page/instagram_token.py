"""Instagram token exchange page for storing a long-lived token in backend storage."""

from __future__ import annotations

import httpx
import streamlit as st
from decouple import config as env

from streamlit_app.functions.runtime import (
    refresh_backend_tokens,
    resolve_backend_base_url,
)


def _internal_backend_url() -> str:
    """Resolve backend base URL for server-side calls from the Streamlit container."""
    return resolve_backend_base_url(prefer_internal=True)


def _instagram_status_url() -> str:
    """Build the backend URL used to inspect stored Instagram token status."""
    return f"{_internal_backend_url()}/api/instagram/token/status"


def _instagram_exchange_url() -> str:
    """Build the backend URL used to exchange a short-lived Instagram token."""
    return f"{_internal_backend_url()}/api/instagram/token/exchange"


def _instagram_save_url() -> str:
    """Build the backend URL used to validate and save an Instagram token directly."""
    return f"{_internal_backend_url()}/api/instagram/token/save"


def _instagram_refresh_url() -> str:
    """Build the backend URL used to refresh the stored Instagram token."""
    return f"{_internal_backend_url()}/api/instagram/token/refresh"


async def _authorized_request(
    method: str,
    url: str,
    access_token: str,
    *,
    json_payload: dict | None = None,
) -> tuple[httpx.Response, dict]:
    """Send one authenticated request with a single refresh-token retry path."""
    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.request(
            method=method,
            url=url,
            headers=headers,
            json=json_payload,
        )

        if response.status_code == 401:
            refreshed_payload = await refresh_backend_tokens(
                host=_internal_backend_url(),
            )
            if refreshed_payload and refreshed_payload.get("success"):
                st.session_state.access_token = refreshed_payload.get("access_token")
                headers["Authorization"] = f"Bearer {st.session_state.access_token}"
                response = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json_payload,
                )

    payload = response.json() if response.content else {}
    return response, payload


async def show_instagram_token_page(host: str) -> None:
    """Render the admin UI for exchanging, storing, and refreshing Instagram tokens."""
    del host

    st.title("Instagram Token")
    st.caption("Paste short-lived Instagram token, backend akan tukar ke long-lived token dan simpan encrypted di database.")

    app_id = env("META_APP_ID", default="", cast=str).strip()
    ig_app_secret = env("IG_APP_SECRET", default="", cast=str).strip()
    meta_app_secret = env("META_APP_SECRET", default="", cast=str).strip()
    api_version = env("META_API_VERSION", default="v24.0", cast=str).strip() or "v24.0"
    if not ig_app_secret and not meta_app_secret:
        st.error("Instagram app config belum lengkap. Set `IG_APP_SECRET` atau fallback `META_APP_SECRET` di backend env.")
        return

    access_token = st.session_state.get("access_token")
    if not access_token:
        st.error("Session is invalid. Please log in again.")
        return

    status_data: dict = {}
    try:
        response, status_data = await _authorized_request(
            "GET",
            _instagram_status_url(),
            access_token,
        )
        if response.status_code == 401:
            st.error("Session login sudah expired. Silakan login ulang.")
            return
    except Exception as error:  # noqa: BLE001
        st.warning(f"Gagal ambil status token saat ini: {error}")

    with st.container(border=True):
        st.markdown("### App Config")
        st.write(f"`META_APP_ID`: `{app_id or '-'}`")
        st.write(f"`META_API_VERSION`: `{api_version}`")
        st.write("`IG_APP_SECRET` terbaca di backend." if ig_app_secret else "`META_APP_SECRET` fallback terbaca di backend.")

    if status_data.get("configured"):
        st.success(
            "Instagram long-lived token sudah tersimpan. "
            f"Last update: `{status_data.get('updated_at') or '-'}`"
        )
        if st.button("Refresh Stored Token", type="secondary", use_container_width=True):
            with st.spinner("Refreshing Instagram token..."):
                try:
                    response, data = await _authorized_request(
                        "POST",
                        _instagram_refresh_url(),
                        st.session_state.get("access_token") or access_token,
                    )
                except Exception as error:  # noqa: BLE001
                    st.error(f"Token refresh gagal: {error}")
                    return

            if response.status_code >= 400:
                message = data.get("detail") or data.get("message") or "Instagram token refresh failed."
                st.error(message)
                return

            st.success(data.get("message", "Instagram token berhasil di-refresh."))
            if data.get("expires_in") is not None:
                st.caption(f"expires_in: `{data['expires_in']}` seconds")
            if data.get("updated_at"):
                st.caption(f"stored_at: `{data['updated_at']}`")
    else:
        st.info("Belum ada long-lived token Instagram yang tersimpan di database.")

    with st.form("instagram_token_exchange_form"):
        instagram_token = st.text_area(
            "Instagram Access Token",
            height=180,
            help="Token mentah hanya dikirim ke backend. UI tidak akan menyimpan token.",
        ).strip()
        exchange_submitted = st.form_submit_button("Exchange and Save", type="primary", use_container_width=True)
        save_submitted = st.form_submit_button("Validate and Save Directly", type="secondary", use_container_width=True)

    if not exchange_submitted and not save_submitted:
        return

    if not instagram_token:
        st.warning("Isi Instagram access token dulu.")
        return

    if save_submitted:
        with st.spinner("Validating Instagram token..."):
            try:
                response, data = await _authorized_request(
                    "POST",
                    _instagram_save_url(),
                    st.session_state.get("access_token") or access_token,
                    json_payload={"access_token": instagram_token},
                )
            except Exception as error:  # noqa: BLE001
                st.error(f"Token save gagal: {error}")
                return

        if response.status_code >= 400:
            message = data.get("detail") or data.get("message") or "Instagram token validation failed."
            st.error(message)
            return

        st.success(data.get("message", "Instagram access token berhasil disimpan."))
        if data.get("username"):
            st.caption(f"username: `{data['username']}`")
        if data.get("instagram_user_id"):
            st.caption(f"instagram_user_id: `{data['instagram_user_id']}`")
        if data.get("updated_at"):
            st.caption(f"stored_at: `{data['updated_at']}`")
        return

    with st.spinner("Exchanging Instagram token..."):
        try:
            response, data = await _authorized_request(
                "POST",
                _instagram_exchange_url(),
                st.session_state.get("access_token") or access_token,
                json_payload={"short_lived_token": instagram_token},
            )
        except Exception as error:  # noqa: BLE001
            st.error(f"Token exchange gagal: {error}")
            return

    if response.status_code >= 400:
        message = data.get("detail") or data.get("message") or "Instagram token exchange failed."
        st.error(message)
        return

    st.success(data.get("message", "Instagram long-lived token berhasil disimpan."))
    if data.get("expires_in") is not None:
        st.caption(f"expires_in: `{data['expires_in']}` seconds")
    if data.get("updated_at"):
        st.caption(f"stored_at: `{data['updated_at']}`")
