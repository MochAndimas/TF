"""Feature module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

import asyncio
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import require_roles_dep
from app.db.models.etl_run import EtlRun
from app.db.models.user import TfUser
from app.db.session import get_db
from app.etl.job_runner import execute_update_job, resolve_run_window
from app.schemas.feature import (
    EtlRunSummary,
    UpdateData,
    UpdateDataResponse,
    UpdateDataStatusResponse,
    UpdateDataSummaryResponse,
)
from app.utils.etl_run_utils import (
    cleanup_stale_runs,
    fail_run,
    get_run,
    latest_runs,
    start_run,
    summarize_runs,
)

router = APIRouter()
logger = logging.getLogger(__name__)

DbSession = Annotated[AsyncSession, Depends(get_db)]
SuperadminUser = Annotated[TfUser, Depends(require_roles_dep("superadmin"))]


def _serialize_etl_run(run: EtlRun) -> EtlRunSummary:
    """Serialize ETL run model into public API payload."""
    return EtlRunSummary(
        run_id=run.run_id,
        pipeline=run.pipeline,
        source=run.source,
        mode=run.mode,
        status=run.status,
        message=run.message,
        error_detail=run.error_detail,
        window_start=run.window_start,
        window_end=run.window_end,
        started_at=run.started_at,
        ended_at=run.ended_at,
        triggered_by=run.triggered_by,
        rows_extracted=run.rows_extracted,
        rows_loaded=run.rows_loaded,
        duration_ms=run.duration_ms,
        quality_report=run.quality_report,
    )


@router.post("/api/feature-data/update-external-api", response_model=UpdateDataResponse)
async def update_data(
    response: UpdateData,
    session: DbSession,
    current_user: SuperadminUser,
):
    """Run data update jobs for selected external source and date range."""
    try:
        if response.data == "__all__" or "," in response.data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Manual ETL accepts exactly one data source per run.",
            )
        start_date = response.start_date
        end_date = response.end_date
        types = response.types
        cleaned_runs = await cleanup_stale_runs(session=session)
        window_start, window_end = resolve_run_window(
            data=response.data,
            types=types,
            start_date=start_date,
            end_date=end_date,
        )
        run_id = await start_run(
            session=session,
            source=response.data,
            mode=types,
            window_start=window_start,
            window_end=window_end,
            triggered_by=current_user.user_id,
        )
        asyncio.create_task(
            execute_update_job(
                run_id=run_id,
                data=response.data,
                types=types,
                start_date=start_date,
                end_date=end_date,
            )
        )
        return UpdateDataResponse(
            success=True,
            message=(
                "Update job accepted and queued. "
                "Track progress via status endpoint."
            ),
            run_id=run_id,
            status="queued",
            recovered_stale_runs=cleaned_runs,
        )
    except HTTPException as error:
        if "run_id" in locals():
            await fail_run(session=session, run_id=run_id, error_detail=str(error.detail))
        raise
    except Exception as error:
        if "run_id" in locals():
            await fail_run(session=session, run_id=run_id, error_detail=str(error))
        logger.exception("Feature update job scheduling failed")
        raise HTTPException(
            status_code=500,
            detail="An internal error occurred while scheduling the update job.",
        ) from error


@router.get(
    "/api/feature-data/update-external-api/summary",
    response_model=UpdateDataSummaryResponse,
)
async def update_data_summary(
    session: DbSession,
    current_user: SuperadminUser,  # noqa: ARG001
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    source: str | None = None,
    status_filter: Annotated[str | None, Query(alias="status")] = None,
):
    """Return ETL status summary for operational dashboards."""
    runs = await latest_runs(
        session=session,
        limit=limit,
        source=source,
        status_filter=status_filter,
    )
    counts = await summarize_runs(session=session)
    return UpdateDataSummaryResponse(
        success=True,
        message="ETL summary loaded successfully.",
        counts=counts,
        latest_runs=[_serialize_etl_run(run) for run in runs],
    )


@router.get(
    "/api/feature-data/update-external-api/{run_id}",
    response_model=UpdateDataStatusResponse,
)
async def update_data_status(
    run_id: str,
    session: DbSession,
    current_user: SuperadminUser,  # noqa: ARG001
):
    """Fetch ETL run status payload by run identifier."""
    run = await get_run(session=session, run_id=run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found.")

    return UpdateDataStatusResponse(
        **_serialize_etl_run(run).model_dump(),
    )
