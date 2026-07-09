"""Reusable date-window ETL pipeline orchestration primitives."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.etl.transform import dedupe_ads_dataframe, resolve_date_window


@dataclass(frozen=True)
class DateWindowPipelineSpec:
    """Configuration for one extract-stage-transform-load date-window pipeline."""

    label: str
    source: str
    empty_metric_name: str
    date_column: str
    auto_skip_model: type
    extract: Callable[[Any, Any], Awaitable[list]]
    stage: Callable[[AsyncSession, list, str | None], Awaitable[int]]
    parse: Callable[[list], Any]
    validate: Callable[[Any], None]
    build_rows: Callable[[Any, Any], list[dict]]
    delete_window: Callable[[AsyncSession, Any, Any], Awaitable[int]]
    load_rows: Callable[[AsyncSession, list[dict]], Awaitable[None]]
    user_empty_message: str = "No data found from source."
    user_window_empty_message: str = "No data found for selected date range."
    user_success_message: str = "Data is being updated!"
    user_already_updated_message: str = "Data is already updated!"
    resolve_window: Callable[[str, Any, Any], tuple[Any, Any]] | None = None


class DateWindowPipelineRunner:
    """Shared runner for ETL sources that replace one reporting date window."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def _log_event(self, event: str, **fields) -> None:
        """Emit one structured ETL log event."""
        payload = {"event": event, **fields}
        self.logger.info(json.dumps(payload, default=str))

    def _resolve_date_window(self, types: str, start_date, end_date):
        """Resolve ETL date window from update mode and payload values."""
        return resolve_date_window(types, start_date, end_date)

    async def _run_date_window_pipeline(
        self,
        *,
        spec: DateWindowPipelineSpec,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Execute the shared date-window ETL orchestration."""
        started_at = perf_counter()
        if spec.resolve_window is not None:
            target_start, target_end = spec.resolve_window(types, start_date, end_date)
        else:
            target_start, target_end = self._resolve_date_window(types, start_date, end_date)
        self._log_event(
            f"etl_{spec.label}_started",
            run_id=run_id,
            source=spec.source,
            types=types,
            target_start=target_start,
            target_end=target_end,
        )

        try:
            if types == "auto":
                existing_rows = await session.execute(
                    select(spec.auto_skip_model.id).where(
                        getattr(spec.auto_skip_model, spec.date_column).between(
                            target_start,
                            target_end,
                        )
                    )
                )
                if existing_rows.first():
                    self._log_event(
                        f"etl_{spec.label}_skipped",
                        run_id=run_id,
                        source=spec.source,
                        reason="already_updated",
                        duration_sec=round(perf_counter() - started_at, 3),
                    )
                    return spec.user_already_updated_message

            raw_rows = await spec.extract(target_start, target_end)
            staged_count = await spec.stage(session, raw_rows, run_id)
            await session.commit()

            df = spec.parse(raw_rows)
            raw_count = self._raw_count(raw_rows)
            if df.empty:
                deleted_count = await self._replace_window_with_rows(
                    session=session,
                    delete_window=spec.delete_window,
                    load_rows=spec.load_rows,
                    rows=[],
                    target_start=target_start,
                    target_end=target_end,
                )
                self._log_event(
                    f"etl_{spec.label}_source_empty_window_replaced",
                    run_id=run_id,
                    source=spec.source,
                    raw_count=raw_count,
                    staged_count=staged_count,
                    deleted_count=deleted_count,
                    duration_sec=round(perf_counter() - started_at, 3),
                )
                return spec.user_empty_message

            df = df[(df[spec.date_column] >= target_start) & (df[spec.date_column] <= target_end)]
            filtered_count = len(df)
            if df.empty:
                empty_window_fields = {
                    "run_id": run_id,
                    "source": spec.source,
                    "raw_count": raw_count,
                    "staged_count": staged_count,
                    "duration_sec": round(perf_counter() - started_at, 3),
                }
                if spec.label == "campaign_ads":
                    empty_window_fields.update(
                        {
                            "dedupe_applied": False,
                            "dedupe_dropped_count": 0,
                        }
                    )
                self._log_event(
                    f"etl_{spec.label}_no_rows_in_window",
                    **empty_window_fields,
                )
                deleted_count = await self._replace_window_with_rows(
                    session=session,
                    delete_window=spec.delete_window,
                    load_rows=spec.load_rows,
                    rows=[],
                    target_start=target_start,
                    target_end=target_end,
                )
                self._log_event(
                    f"etl_{spec.label}_window_replaced_empty",
                    run_id=run_id,
                    source=spec.source,
                    deleted_count=deleted_count,
                    duration_sec=round(perf_counter() - started_at, 3),
                )
                return spec.user_window_empty_message

            dedupe_applied = False
            dedupe_dropped_count = 0
            if spec.label == "campaign_ads":
                df, dedupe_dropped_count = dedupe_ads_dataframe(df)
                dedupe_applied = dedupe_dropped_count > 0

            spec.validate(df)
            rows = spec.build_rows(df, datetime.now().date())
            deleted_count = await self._replace_window_with_rows(
                session=session,
                delete_window=spec.delete_window,
                load_rows=spec.load_rows,
                rows=rows,
                target_start=target_start,
                target_end=target_end,
            )
            completed_fields = {
                "run_id": run_id,
                "source": spec.source,
                "raw_count": raw_count,
                "filtered_count": filtered_count,
                "staged_count": staged_count,
                "deleted_count": deleted_count,
                "loaded_count": len(rows),
                "duration_sec": round(perf_counter() - started_at, 3),
            }
            if spec.label == "campaign_ads":
                completed_fields.update(
                    {
                        "dedupe_applied": dedupe_applied,
                        "dedupe_dropped_count": dedupe_dropped_count,
                    }
                )
            self._log_event(f"etl_{spec.label}_completed", **completed_fields)
            return spec.user_success_message
        except HTTPException:
            self._log_event(
                f"etl_{spec.label}_failed_http",
                run_id=run_id,
                source=spec.source,
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise
        except ValueError as error:
            self._log_event(
                f"etl_{spec.label}_failed_dq",
                run_id=run_id,
                source=spec.source,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            self._log_event(
                f"etl_{spec.label}_failed",
                run_id=run_id,
                source=spec.source,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(500, f"{spec.empty_metric_name} error: {str(error)}") from error

    @staticmethod
    def _raw_count(raw_rows: list) -> int:
        """Count source rows the same way ETL completion logs expect."""
        if not raw_rows:
            return 0
        if isinstance(raw_rows[0], dict):
            return len(raw_rows)
        return max(len(raw_rows) - 1, 0)

    @staticmethod
    async def _replace_window_with_rows(
        *,
        session: AsyncSession,
        delete_window: Callable[[AsyncSession, Any, Any], Awaitable[int]],
        load_rows: Callable[[AsyncSession, list[dict]], Awaitable[None]],
        rows: list[dict],
        target_start,
        target_end,
    ) -> int:
        """Replace one reporting window in a single final-load transaction."""
        try:
            deleted_count = await delete_window(session, target_start, target_end)
            await load_rows(session, rows)
            await session.commit()
            return deleted_count
        except Exception:
            await session.rollback()
            raise
