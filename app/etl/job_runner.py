"""Shared ETL job runners for API-triggered and scheduler-triggered updates."""

from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import HTTPException

from app.db.models.external_api import FacebookAds, GoogleAds, TikTokAds
from app.db.session import sqlite_async_session
from app.etl.load import rebuild_unique_campaign
from app.etl.pipelines import GoogleSheetApi
from app.etl.transform import resolve_date_window
from app.utils.etl_run_utils import fail_run, finish_run, start_run

logger = logging.getLogger(__name__)

DEFAULT_SCHEDULED_SOURCES: tuple[str, ...] = (
    "google_ads",
    "facebook_ads",
    "tiktok_ads",
    "unique_campaign",
    "ga4_daily_metrics",
    "first_deposit",
)


def resolve_run_window(data: str, types: str, start_date, end_date) -> tuple[Any, Any]:
    """Resolve the effective ETL date window for one requested source update.

    Args:
        data (str): Source key selected by the caller.
        types (str): Update mode, for example scheduled or manual/custom.
        start_date: Optional explicit start date from the request payload.
        end_date: Optional explicit end date from the request payload.

    Returns:
        tuple[Any, Any]: Normalized ``(start_date, end_date)`` pair, or
        ``(None, None)`` for sources that do not operate on date windows.
    """
    if data == "unique_campaign":
        return None, None
    return resolve_date_window(types, start_date, end_date)


async def execute_update_job(
    *,
    run_id: str,
    data: str,
    types: str,
    start_date,
    end_date,
) -> dict[str, Any]:
    """Execute one ETL task and persist lifecycle status into ``etl_run``.

    Args:
        run_id (str): Existing ETL run identifier that tracks job status.
        data (str): Source key to process.
        types (str): Trigger mode that influences date-window resolution.
        start_date: Optional requested start date.
        end_date: Optional requested end date.

    Returns:
        dict[str, Any]: Structured result payload describing whether the job
        succeeded and the message or error captured for the run.
    """
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

    async def _mark_success(message: str) -> None:
        async with sqlite_async_session() as status_session:
            await finish_run(session=status_session, run_id=run_id, message=message)

    async def _mark_failed(error_detail: str) -> None:
        async with sqlite_async_session() as status_session:
            await fail_run(session=status_session, run_id=run_id, error_detail=error_detail)

    async with sqlite_async_session() as session:
        try:
            gsheet = GoogleSheetApi()
            if data == "unique_campaign":
                message = await rebuild_unique_campaign(session=session)
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
            elif data == "ga4_daily_metrics":
                message = await gsheet.ga4_daily_metrics(
                    types=types,
                    start_date=start_date,
                    end_date=end_date,
                    session=session,
                    run_id=run_id,
                )
            elif data == "first_deposit":
                message = await gsheet.first_deposit(
                    types=types,
                    start_date=start_date,
                    end_date=end_date,
                    session=session,
                    run_id=run_id,
                )
            else:
                raise HTTPException(status_code=404, detail="Please chose one data to update!")

            if not message:
                raise HTTPException(status_code=404, detail="Something is error, data update is failed!")

            await _mark_success(message=message)
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
            return {"success": True, "run_id": run_id, "source": data, "message": message}
        except HTTPException as error:
            error_detail = str(error.detail)
            await _mark_failed(error_detail=error_detail)
            logger.error(
                json.dumps(
                    {
                        "event": "etl_background_job_failed_http",
                        "run_id": run_id,
                        "source": data,
                        "status": "failed",
                        "error": error_detail,
                    },
                    default=str,
                )
            )
            return {"success": False, "run_id": run_id, "source": data, "error": error_detail}
        except Exception as error:
            error_detail = str(error)
            await _mark_failed(error_detail=error_detail)
            logger.error(
                json.dumps(
                    {
                        "event": "etl_background_job_failed",
                        "run_id": run_id,
                        "source": data,
                        "status": "failed",
                        "error": error_detail,
                    },
                    default=str,
                )
            )
            return {"success": False, "run_id": run_id, "source": data, "error": error_detail}


async def trigger_and_wait_update_job(
    *,
    data: str,
    types: str = "auto",
    start_date=None,
    end_date=None,
    triggered_by: str | None = None,
) -> dict[str, Any]:
    """Create an ``etl_run`` row, execute the ETL, and return the final status."""
    window_start, window_end = resolve_run_window(
        data=data,
        types=types,
        start_date=start_date,
        end_date=end_date,
    )
    async with sqlite_async_session() as session:
        run_id = await start_run(
            session=session,
            source=data,
            mode=types,
            window_start=window_start,
            window_end=window_end,
            triggered_by=triggered_by,
        )
    return await execute_update_job(
        run_id=run_id,
        data=data,
        types=types,
        start_date=start_date,
        end_date=end_date,
    )
