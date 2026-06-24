"""Update Data module.

This module is part of `streamlit_app.page` and contains runtime logic used by the
Traders Family application.
"""

import httpx
import streamlit as st

from streamlit_app.page.update_data_components.api import poll_update_job, trigger_update_job
from streamlit_app.page.update_data_components.form import (
    ALL_DATA_SOURCES_LABEL,
    ALL_DATA_SOURCE_VALUES,
    DATA_SOURCE_OPTIONS,
    render_update_form,
    resolve_date_input,
)


async def _run_update_for_source(
    *,
    host: str,
    access_token: str,
    source: str,
    mode: str,
    from_date,
    to_date,
) -> bool:
    """Trigger and poll one ETL source update."""
    payload = {
        "types": mode,
        "start_date": from_date.isoformat(),
        "end_date": to_date.isoformat(),
        "data": source,
    }

    initial_result = await trigger_update_job(host, access_token, payload)
    response = initial_result["response"]
    data = initial_result["data"]
    if response.status_code >= 400:
        message = data.get("detail") or data.get("message") or "Update request failed."
        st.error(message)
        return False

    run_id = data.get("run_id")
    if not run_id:
        st.success(data.get("message", "Update request accepted."))
        return True

    recovered_stale_runs = data.get("recovered_stale_runs", 0)
    if recovered_stale_runs:
        st.warning(
            f"Recovered {recovered_stale_runs} stale ETL run(s) before starting this job."
        )
    st.info(f"Job accepted. Run ID: `{run_id}`")
    poll_result = await poll_update_job(host, access_token, run_id)
    if poll_result.get("error"):
        st.error(poll_result["error"])
        return False

    status_placeholder = poll_result["status_placeholder"]
    progress_placeholder = poll_result["progress_placeholder"]
    final_status = poll_result["final_status"]
    final_message = poll_result["final_message"]
    final_error = poll_result["final_error"]

    if final_status == "success":
        status_placeholder.empty()
        progress_placeholder.empty()
        st.success(final_message or "Update completed successfully.")
        return True

    if final_status == "failed":
        status_placeholder.empty()
        progress_placeholder.empty()
        st.error(final_error or final_message or "Update failed.")
        return False

    status_placeholder.warning(f"Current status: `{final_status or 'queued'}`")
    progress_placeholder.progress(1.0)
    st.warning(f"Update still running. Please check again later using this run_id: `{run_id}`")
    return False


async def show_update_page(host):
    """Render update form and execute external data synchronization.

    Args:
        host (str): Base URL for backend API requests.

    Returns:
        None: UI side effects only.
    """
    st.markdown("""<h1 align="center">Update Data</h1>""", unsafe_allow_html=True)
    st.caption("Trigger manual or automatic synchronization for campaign, GA4, register, and first deposit datasets.")

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

    with st.spinner("Updating data..."):
        try:
            selected_source = DATA_SOURCE_OPTIONS[source_label]
            if source_label == ALL_DATA_SOURCES_LABEL:
                successful_sources = 0
                failed_sources = []
                source_labels_by_value = {
                    value: label
                    for label, value in DATA_SOURCE_OPTIONS.items()
                    if label != ALL_DATA_SOURCES_LABEL
                }
                total_sources = len(ALL_DATA_SOURCE_VALUES)

                for index, source in enumerate(ALL_DATA_SOURCE_VALUES, start=1):
                    readable_source = source_labels_by_value.get(source, source)
                    st.markdown(f"### {index}/{total_sources} - {readable_source}")
                    source_success = await _run_update_for_source(
                        host=host,
                        access_token=access_token,
                        source=source,
                        mode=mode,
                        from_date=from_date,
                        to_date=to_date,
                    )
                    if source_success:
                        successful_sources += 1
                    else:
                        failed_sources.append(readable_source)

                if failed_sources:
                    st.warning(
                        f"Finished with {successful_sources}/{total_sources} successful source(s). "
                        f"Failed: {', '.join(failed_sources)}"
                    )
                else:
                    st.success(f"All {total_sources} data source(s) updated successfully.")
                return

            await _run_update_for_source(
                host=host,
                access_token=access_token,
                source=selected_source,
                mode=mode,
                from_date=from_date,
                to_date=to_date,
            )
            return
        except httpx.RequestError as error:
            st.error(f"Network error while updating data: {error}")
        except Exception as error:
            st.error(f"Unexpected error while updating data: {error}")
