"""Update Data module.

This module is part of `streamlit_app.page` and contains runtime logic used by the
Traders Family application.
"""

import asyncio
import httpx
import streamlit as st

from streamlit_app.page.update_data_components.api import poll_update_job, trigger_update_job
from streamlit_app.page.update_data_components.form import DATA_SOURCE_OPTIONS, render_update_form, resolve_date_input


async def show_update_page(host):
    """Render update form and execute external data synchronization.

    Args:
        host (str): Base URL for backend API requests.

    Returns:
        None: UI side effects only.
    """
    st.markdown("""<h1 align="center">Update Data</h1>""", unsafe_allow_html=True)
    st.caption("Trigger manual or automatic synchronization for campaign and GA4 datasets.")

    form_state = render_update_form()
    submitted = form_state["submitted"]
    source_label = form_state["source_label"]
    mode = form_state["mode"]
    preset_key = form_state["preset_key"]
    presets = form_state["presets"]
    from_date = form_state["from_date"]
    to_date = form_state["to_date"]

    if not submitted:
        return

    if not source_label:
        st.warning("Please select a data source before running update.")
        return

    if mode == "auto":
        from_date, to_date = resolve_date_input(mode, "Yesterday", presets)
    elif not from_date or not to_date:
        st.warning("Please provide a valid date range.")
        return

    access_token = st.session_state.get("access_token")
    if not access_token:
        st.error("Session is invalid. Please log in again.")
        return

    payload = {
        "types": mode,
        "start_date": from_date.isoformat(),
        "end_date": to_date.isoformat(),
        "data": DATA_SOURCE_OPTIONS[source_label],
    }

    with st.spinner("Updating data..."):
        try:
            initial_result = await trigger_update_job(host, access_token, payload)
            response = initial_result["response"]
            data = initial_result["data"]
            if response.status_code >= 400:
                message = data.get("detail") or data.get("message") or "Update request failed."
                st.error(message)
                return

            run_id = data.get("run_id")
            if not run_id:
                st.success(data.get("message", "Update request accepted."))
                return

            status_url = f"{host}/api/feature-data/update-external-api/{run_id}"
            st.info(f"Job accepted. Run ID: `{run_id}`")
            poll_result = await poll_update_job(host, access_token, run_id)
            if poll_result.get("error"):
                st.error(poll_result["error"])
                return
            status_placeholder = poll_result["status_placeholder"]
            progress_placeholder = poll_result["progress_placeholder"]
            final_status = poll_result["final_status"]
            final_message = poll_result["final_message"]
            final_error = poll_result["final_error"]

            if final_status == "success":
                status_placeholder.empty()
                progress_placeholder.empty()
                st.success(final_message or "Update completed successfully.")
                return

            if final_status == "failed":
                status_placeholder.empty()
                progress_placeholder.empty()
                st.error(final_error or final_message or "Update failed.")
                return

            status_placeholder.warning("Current status: `running`")
            progress_placeholder.progress(1.0)
            st.warning(f"Update still running. Please check again later using this run_id: `{run_id}`")
        except httpx.RequestError as error:
            st.error(f"Network error while updating data: {error}")
        except Exception as error:
            st.error(f"Unexpected error while updating data: {error}")
