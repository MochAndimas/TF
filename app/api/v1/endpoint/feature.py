"""Feature module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

import asyncio
import json
import logging

from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.utils.user_utils import get_current_user
from app.db.models.user import TfUser
from app.schemas.feature import UpdateData, UpdateDataResponse, UpdateDataStatusResponse
from app.db.models.external_api import GoogleAds, FacebookAds, TikTokAds
from app.etl.load import rebuild_unique_campaign
from app.etl.pipelines import GoogleSheetApi
from app.utils.etl_run_utils import fail_run, finish_run, start_run
from app.db.models.etl_run import EtlRun
from app.db.session import sqlite_async_session


router = APIRouter()
logger = logging.getLogger(__name__)


async def _execute_update_job(
    *,
    run_id: str,
    data: str,
    types: str,
    start_date,
    end_date,
) -> None:
    """Execute update job in background and persist run status."""
    logger.info(
        json.dumps(
            {
                "event": "etl_background_job_started",
                "run_id": run_id,
                "source": data,
                "types": types,
            },
            default=str,
        )
    )
    async with sqlite_async_session() as session:
        try:
            gsheet = GoogleSheetApi()
            if data == "unique_campaign":
                message = await rebuild_unique_campaign(session=session)
            elif data == "data_depo":
                message = await gsheet.data_depo(
                    types=types,
                    start_date=start_date,
                    end_date=end_date,
                    session=session,
                    run_id=run_id,
                )
            elif data == "google_ads":
                message = await gsheet.campaign_ads(
                    types=types,
                    range_name="'Google Ads Campaign'!A:I",
                    start_date=start_date,
                    end_date=end_date,
                    session=session,
                    classes=GoogleAds,
                    run_id=run_id,
                )
            elif data == "facebook_ads":
                message = await gsheet.campaign_ads(
                    types=types,
                    range_name="'Meta Ads Campaign'!A:I",
                    start_date=start_date,
                    end_date=end_date,
                    session=session,
                    classes=FacebookAds,
                    run_id=run_id,
                )
            elif data == "tiktok_ads":
                message = await gsheet.campaign_ads(
                    types=types,
                    range_name="'TikTok Ads Campaign'!A:I",
                    start_date=start_date,
                    end_date=end_date,
                    session=session,
                    classes=TikTokAds,
                    run_id=run_id,
                )
            else:
                raise HTTPException(
                    status_code=404,
                    detail="Please chose one data to update!",
                )

            if not message:
                raise HTTPException(
                    status_code=404,
                    detail="Something is error, data update is failed!",
                )

            await finish_run(session=session, run_id=run_id, message=message)
            logger.info(
                json.dumps(
                    {
                        "event": "etl_background_job_completed",
                        "run_id": run_id,
                        "source": data,
                        "status": "success",
                        "message": message,
                    },
                    default=str,
                )
            )
        except HTTPException as error:
            await fail_run(session=session, run_id=run_id, error_detail=str(error.detail))
            logger.error(
                json.dumps(
                    {
                        "event": "etl_background_job_failed_http",
                        "run_id": run_id,
                        "source": data,
                        "status": "failed",
                        "error": str(error.detail),
                    },
                    default=str,
                )
            )
        except Exception as error:
            await fail_run(session=session, run_id=run_id, error_detail=str(error))
            logger.error(
                json.dumps(
                    {
                        "event": "etl_background_job_failed",
                        "run_id": run_id,
                        "source": data,
                        "status": "failed",
                        "error": str(error),
                    },
                    default=str,
                )
            )


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
        run_id = await start_run(
            session=session,
            source=response.data,
            mode=types,
            window_start=start_date.date() if start_date else None,
            window_end=end_date.date() if end_date else None,
            triggered_by=current_user.user_id,
        )
        asyncio.create_task(
            _execute_update_job(
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
    """Fetch ETL run status by run identifier."""
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
