"""Shared ETL job runners for API-triggered and scheduler-triggered updates."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from time import perf_counter
from typing import Any

from fastapi import HTTPException

from sqlalchemy import func, select

from app.db.models.external_api import (
    Campaign,
    DailyRegister,
    DataDepo,
    DataMsDeposit,
    FacebookAds,
    FacebookPageInsights,
    FacebookPageMediaInsights,
    Ga4DailyMetrics,
    GoogleAds,
    InstagramInsights,
    InstagramMediaInsights,
    TikTokAds,
    YouTubeDailyInsight,
    YouTubeMediaInsight,
)
from app.db.session import sqlite_async_session
from app.etl.load import rebuild_unique_campaign
from app.etl.pipelines import GoogleSheetApi
from app.etl.run_report import build_quality_report
from app.etl.transform import resolve_date_window
from app.utils.analytics_cache import clear_campaign_analytics_cache
from app.utils.etl_run_utils import (
    cleanup_stale_runs,
    complete_run,
    fail_run,
    mark_run_running,
    start_run,
)

logger = logging.getLogger(__name__)

DEFAULT_SCHEDULED_SOURCES: tuple[str, ...] = (
    "google_ads",
    "facebook_ads",
    "tiktok_ads",
    "unique_campaign",
    "ga4_daily_metrics",
    "instagram_insights",
    "instagram_media_insights",
    "youtube_daily_insight",
    "youtube_media_insight",
    "facebook_page_insights",
    "facebook_page_media_insights",
    "daily_register",
    "first_deposit",
    "ms_deposit",
)

PipelineExecutor = Callable[[GoogleSheetApi, Any, str, Any, Any, str], Awaitable[str]]


async def _run_unique_campaign(
    _gsheet: GoogleSheetApi,
    session,
    _types: str,
    _start_date,
    _end_date,
    _run_id: str,
) -> str:
    return await rebuild_unique_campaign(session=session)


async def _run_google_ads(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.campaign_ads(
        types=types,
        range_name="'Google Ads Campaign'!A:I",
        start_date=start_date,
        end_date=end_date,
        session=session,
        classes=GoogleAds,
        run_id=run_id,
    )


async def _run_facebook_ads(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.campaign_ads(
        types=types,
        range_name="'Meta Ads Campaign'!A:I",
        start_date=start_date,
        end_date=end_date,
        session=session,
        classes=FacebookAds,
        run_id=run_id,
    )


async def _run_tiktok_ads(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.campaign_ads(
        types=types,
        range_name="'TikTok Ads Campaign'!A:I",
        start_date=start_date,
        end_date=end_date,
        session=session,
        classes=TikTokAds,
        run_id=run_id,
    )


async def _run_ga4_daily_metrics(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.ga4_daily_metrics(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


async def _run_daily_register(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.daily_register(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


async def _run_instagram_insights(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.instagram_insights(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


async def _run_instagram_media_insights(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.instagram_media_insights(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


async def _run_youtube_daily_insight(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.youtube_daily_insight(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


async def _run_youtube_media_insight(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.youtube_media_insight(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


async def _run_facebook_page_insights(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.facebook_page_insights(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


async def _run_facebook_page_media_insights(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.facebook_page_media_insights(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


async def _run_first_deposit(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.first_deposit(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


async def _run_ms_deposit(
    gsheet: GoogleSheetApi,
    session,
    types: str,
    start_date,
    end_date,
    run_id: str,
) -> str:
    return await gsheet.ms_deposit(
        types=types,
        start_date=start_date,
        end_date=end_date,
        session=session,
        run_id=run_id,
    )


PIPELINE_EXECUTORS: dict[str, PipelineExecutor] = {
    "unique_campaign": _run_unique_campaign,
    "google_ads": _run_google_ads,
    "facebook_ads": _run_facebook_ads,
    "tiktok_ads": _run_tiktok_ads,
    "ga4_daily_metrics": _run_ga4_daily_metrics,
    "instagram_insights": _run_instagram_insights,
    "instagram_media_insights": _run_instagram_media_insights,
    "youtube_daily_insight": _run_youtube_daily_insight,
    "youtube_media_insight": _run_youtube_media_insight,
    "facebook_page_insights": _run_facebook_page_insights,
    "facebook_page_media_insights": _run_facebook_page_media_insights,
    "daily_register": _run_daily_register,
    "first_deposit": _run_first_deposit,
    "ms_deposit": _run_ms_deposit,
}

SOURCE_MODELS = {
    "unique_campaign": Campaign,
    "google_ads": GoogleAds,
    "facebook_ads": FacebookAds,
    "tiktok_ads": TikTokAds,
    "ga4_daily_metrics": Ga4DailyMetrics,
    "instagram_insights": InstagramInsights,
    "instagram_media_insights": InstagramMediaInsights,
    "youtube_daily_insight": YouTubeDailyInsight,
    "youtube_media_insight": YouTubeMediaInsight,
    "facebook_page_insights": FacebookPageInsights,
    "facebook_page_media_insights": FacebookPageMediaInsights,
    "daily_register": DailyRegister,
    "first_deposit": DataDepo,
    "ms_deposit": DataMsDeposit,
}


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


async def count_source_rows(session, source: str) -> int | None:
    """Count current rows in the target table for one ETL source."""
    model = SOURCE_MODELS.get(source)
    if model is None:
        return None
    result = await session.execute(select(func.count()).select_from(model))
    return int(result.scalar_one())


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

    started_perf = perf_counter()

    def _duration_ms() -> int:
        return int((perf_counter() - started_perf) * 1000)

    async def _mark_success(
        message: str,
        *,
        duration_ms: int,
        rows_loaded: int | None,
    ) -> None:
        quality_report = build_quality_report(
            source=data,
            status="success",
            message=message,
            rows_loaded=rows_loaded,
            duration_ms=duration_ms,
        )
        async with sqlite_async_session() as status_session:
            await complete_run(
                session=status_session,
                run_id=run_id,
                message=message,
                rows_loaded=rows_loaded,
                duration_ms=duration_ms,
                quality_report=quality_report,
            )

    async def _mark_failed(error_detail: str, *, duration_ms: int) -> None:
        quality_report = build_quality_report(
            source=data,
            status="failed",
            error_detail=error_detail,
            duration_ms=duration_ms,
        )
        async with sqlite_async_session() as status_session:
            await fail_run(
                session=status_session,
                run_id=run_id,
                error_detail=error_detail,
                duration_ms=duration_ms,
                quality_report=quality_report,
            )

    async def _mark_running() -> None:
        async with sqlite_async_session() as status_session:
            await mark_run_running(session=status_session, run_id=run_id)

    async with sqlite_async_session() as session:
        try:
            await _mark_running()
            gsheet = GoogleSheetApi()
            executor = PIPELINE_EXECUTORS.get(data)
            if executor is None:
                raise HTTPException(status_code=404, detail="Please chose one data to update!")
            message = await executor(gsheet, session, types, start_date, end_date, run_id)

            if not message:
                raise HTTPException(status_code=404, detail="Something is error, data update is failed!")

            duration_ms = _duration_ms()
            rows_loaded = await count_source_rows(session=session, source=data)
            await _mark_success(
                message=message,
                duration_ms=duration_ms,
                rows_loaded=rows_loaded,
            )
            if data in {
                "google_ads",
                "facebook_ads",
                "tiktok_ads",
                "daily_register",
                "first_deposit",
                "ms_deposit",
                "unique_campaign",
                "instagram_insights",
                "instagram_media_insights",
                "youtube_daily_insight",
                "youtube_media_insight",
                "facebook_page_insights",
                "facebook_page_media_insights",
            }:
                clear_campaign_analytics_cache()
            logger.info(
                json.dumps(
                    {
                        "event": "etl_background_job_completed",
                        "run_id": run_id,
                        "source": data,
                        "status": "success",
                        "message": message,
                        "rows_loaded": rows_loaded,
                        "duration_ms": duration_ms,
                    },
                    default=str,
                )
            )
            return {
                "success": True,
                "run_id": run_id,
                "source": data,
                "message": message,
                "rows_loaded": rows_loaded,
                "duration_ms": duration_ms,
            }
        except HTTPException as error:
            error_detail = str(error.detail)
            duration_ms = _duration_ms()
            await _mark_failed(error_detail=error_detail, duration_ms=duration_ms)
            logger.error(
                json.dumps(
                    {
                        "event": "etl_background_job_failed_http",
                        "run_id": run_id,
                        "source": data,
                        "status": "failed",
                        "error": error_detail,
                        "duration_ms": duration_ms,
                    },
                    default=str,
                )
            )
            return {
                "success": False,
                "run_id": run_id,
                "source": data,
                "error": error_detail,
                "duration_ms": duration_ms,
            }
        except Exception as error:
            error_detail = str(error)
            duration_ms = _duration_ms()
            await _mark_failed(error_detail=error_detail, duration_ms=duration_ms)
            logger.error(
                json.dumps(
                    {
                        "event": "etl_background_job_failed",
                        "run_id": run_id,
                        "source": data,
                        "status": "failed",
                        "error": error_detail,
                        "duration_ms": duration_ms,
                    },
                    default=str,
                )
            )
            return {
                "success": False,
                "run_id": run_id,
                "source": data,
                "error": error_detail,
                "duration_ms": duration_ms,
            }


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
        await cleanup_stale_runs(session=session)
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
