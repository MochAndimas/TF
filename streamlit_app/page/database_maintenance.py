"""Database maintenance page for admin SQLite operations."""

from __future__ import annotations

import httpx
import streamlit as st

from streamlit_app.functions.runtime import (
    refresh_backend_tokens,
    resolve_backend_base_url,
)


def _internal_backend_url() -> str:
    return resolve_backend_base_url(prefer_internal=True)


def _format_bytes(value: int | None) -> str:
    if value is None:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{value} B"


async def _authorized_request(
    method: str,
    path: str,
    access_token: str,
) -> tuple[httpx.Response, dict]:
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{_internal_backend_url()}{path}"
    async with httpx.AsyncClient(timeout=300) as client:
        response = await client.request(method=method, url=url, headers=headers)

        if response.status_code == 401:
            refreshed_payload = await refresh_backend_tokens(
                host=_internal_backend_url(),
            )
            if refreshed_payload and refreshed_payload.get("success"):
                st.session_state.access_token = refreshed_payload.get("access_token")
                headers["Authorization"] = f"Bearer {st.session_state.access_token}"
                response = await client.request(method=method, url=url, headers=headers)

    payload = response.json() if response.content else {}
    return response, payload


def _show_status_metrics(payload: dict) -> None:
    columns = st.columns(4, gap="small")
    columns[0].metric("Database Size", _format_bytes(payload.get("database_size_bytes")))
    columns[1].metric("Reclaimable", _format_bytes(payload.get("reclaimable_bytes")))
    columns[2].metric("Freelist Pages", f"{payload.get('freelist_count', 0):,}")
    columns[3].metric("Staging Rows", f"{payload.get('stg_ads_raw_rows', 0):,}")

    etl_runs = int(payload.get("active_etl_runs") or 0)
    if etl_runs:
        st.warning(f"Active ETL runs: `{etl_runs}`. VACUUM tunggu sampai job selesai.")


async def show_database_maintenance_page(host: str) -> None:
    """Render SQLite maintenance controls."""
    del host

    st.markdown("""<h1 align="center">Database Maintenance</h1>""", unsafe_allow_html=True)
    st.caption("Pantau ukuran SQLite dan jalankan VACUUM saat database punya ruang kosong yang bisa direclaim.")

    access_token = st.session_state.get("access_token")
    if not access_token:
        st.error("Session is invalid. Please log in again.")
        return

    try:
        response, status_payload = await _authorized_request(
            "GET",
            "/api/sqlite-maintenance/status",
            access_token,
        )
    except httpx.RequestError as error:
        st.error(f"Network error while loading database status: {error}")
        return

    if response.status_code >= 400:
        st.error(status_payload.get("detail") or "Failed to load database status.")
        return

    _show_status_metrics(status_payload)

    button_disabled = int(status_payload.get("active_etl_runs") or 0) > 0
    if st.button(
        "Run VACUUM",
        type="primary",
        width="stretch",
        disabled=button_disabled,
    ):
        with st.spinner("Running VACUUM..."):
            try:
                vacuum_response, vacuum_payload = await _authorized_request(
                    "POST",
                    "/api/sqlite-maintenance/vacuum",
                    st.session_state.access_token,
                )
            except httpx.RequestError as error:
                st.error(f"Network error while running VACUUM: {error}")
                return

        if vacuum_response.status_code >= 400:
            st.error(vacuum_payload.get("detail") or "VACUUM failed.")
            return

        st.success(vacuum_payload.get("message", "VACUUM completed successfully."))
        before = vacuum_payload.get("before", {})
        after = vacuum_payload.get("after", {})
        st.caption(
            "Size: "
            f"`{_format_bytes(before.get('database_size_bytes'))}` -> "
            f"`{_format_bytes(after.get('database_size_bytes'))}`"
        )
        _show_status_metrics(after)
