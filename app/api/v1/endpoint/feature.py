"""Feature module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

import asyncio

from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.utils.user_utils import get_current_user
from app.db.models.user import TfUser
from app.schemas.feature import UpdateData, UpdateDataResponse, UpdateDataStatusResponse
from app.etl.job_runner import execute_update_job, resolve_run_window
from app.utils.etl_run_utils import fail_run, start_run
from app.db.models.etl_run import EtlRun


router = APIRouter()


@router.post("/api/feature-data/update-external-api", response_model=UpdateDataResponse)
async def update_data(
    response: UpdateData,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user)
):
    """Run data update jobs for selected external source and date range.

    Args:
        response (UpdateData): Request payload containing source type and period.
        session (AsyncSession): Database session injected by FastAPI.
        current_user (TfUser): Authenticated user allowed to trigger update.

    Returns:
        JSONResponse: Message describing update status.

    Raises:
        HTTPException: Raised when source type is invalid or update process fails.
    """
    try:
        start_date = response.start_date
        end_date = response.end_date
        types = response.types
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
        return JSONResponse(
            content={
                "message": "Update job accepted. Track progress via status endpoint.",
                "run_id": run_id,
                "status": "running",
            }
        )
    except HTTPException as error:
        if "run_id" in locals():
            await fail_run(session=session, run_id=run_id, error_detail=str(error.detail))
        raise
    except Exception as e:
        if "run_id" in locals():
            await fail_run(session=session, run_id=run_id, error_detail=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while updating data: {str(e)}"
        )


@router.get(
    "/api/feature-data/update-external-api/{run_id}",
    response_model=UpdateDataStatusResponse,
)
async def update_data_status(
    run_id: str,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Fetch ETL run status payload by run identifier.

    Args:
        run_id (str): ETL run identifier to inspect.
        session (AsyncSession): Database session injected by FastAPI.
        current_user (TfUser): Authenticated user resolved from bearer token.

    Returns:
        UpdateDataStatusResponse: Current ETL run metadata including status,
        message/error, and execution timestamps.

    Raises:
        HTTPException: ``404`` when run identifier is not found.
    """
    result = await session.execute(select(EtlRun).where(EtlRun.run_id == run_id))
    run = result.scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found.")

    return UpdateDataStatusResponse(
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
    )
