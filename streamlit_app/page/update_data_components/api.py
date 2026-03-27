"""Backend request helpers for the update-data page."""

from __future__ import annotations

import asyncio

import httpx
import streamlit as st


async def trigger_update_job(host: str, access_token: str, payload: dict[str, object]) -> dict[str, object]:
    """Send the update request and normalize the initial backend response."""
    async with httpx.AsyncClient(timeout=600) as client:
        response = await client.post(
            f"{host}/api/feature-data/update-external-api",
            headers={"Authorization": f"Bearer {access_token}"},
            json=payload,
        )
    data = response.json() if response.content else {}
    return {"response": response, "data": data}


async def poll_update_job(host: str, access_token: str, run_id: str) -> dict[str, object]:
    """Poll one update job until completion or timeout."""
    status_url = f"{host}/api/feature-data/update-external-api/{run_id}"
    status_placeholder = st.empty()
    progress_placeholder = st.empty()
    max_wait_seconds = 600
    poll_interval_seconds = 5
    elapsed = 0
    final_status = final_message = final_error = None

    async with httpx.AsyncClient(timeout=600) as client:
        while elapsed < max_wait_seconds:
            status_response = await client.get(status_url, headers={"Authorization": f"Bearer {access_token}"})
            status_data = status_response.json() if status_response.content else {}
            if status_response.status_code >= 400:
                message = status_data.get("detail") or status_data.get("message") or "Failed to fetch update status."
                return {"error": message}

            final_status = status_data.get("status")
            final_message = status_data.get("message")
            final_error = status_data.get("error_detail")
            status_placeholder.info(f"Current status: `{final_status}` | Elapsed: {elapsed}s / {max_wait_seconds}s")
            progress_placeholder.progress(min(elapsed / max_wait_seconds, 1.0))

            if final_status in {"success", "failed"}:
                break
            await asyncio.sleep(poll_interval_seconds)
            elapsed += poll_interval_seconds

    return {
        "status_placeholder": status_placeholder,
        "progress_placeholder": progress_placeholder,
        "final_status": final_status,
        "final_message": final_message,
        "final_error": final_error,
        "run_id": run_id,
    }
