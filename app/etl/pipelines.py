"""Pipeline orchestration for external API ETL."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from time import perf_counter

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import DataDepo, Ga4DailyMetrics, TikTokAds
from app.etl.extract import ExternalApiExtractor
from app.etl.load import (
    build_ads_rows,
    build_depo_rows,
    build_ga4_rows,
    upsert_ads_rows,
    upsert_depo_rows,
    upsert_ga4_rows,
)
from app.etl.quality import validate_ads_dataframe, validate_depo_dataframe, validate_ga4_dataframe
from app.etl.staging import stage_ads_raw, stage_depo_raw, stage_ga4_raw
from app.etl.transform import (
    aggregate_ads_dataframe,
    normalize_columns,
    normalize_date,
    parse_ads_dataframe,
    parse_depo_dataframe,
    parse_ga4_dataframe,
    resolve_date_window,
)


class GoogleSheetApi:
    """Orchestrate ETL pipelines for external spreadsheet/API sources.

    This service coordinates end-to-end ETL flow per source:
        - resolve date window,
        - extract raw payload,
        - stage raw payload for auditability,
        - transform and validate data quality,
        - upsert into final tables,
        - emit structured log events and ETL status messages.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.extractor = ExternalApiExtractor()
        self.service = self.extractor.service
        self.sheet_id = self.extractor.sheet_id
        self.depo_source_url = self.extractor.depo_source_url

    def _log_event(self, event: str, **fields) -> None:
        """Emit one structured ETL log event.

        Args:
            event (str): Event name identifier.
            **fields: Additional event metadata serialized as JSON.

        Returns:
            None: Writes to configured logger as side effect.
        """
        payload = {"event": event, **fields}
        self.logger.info(json.dumps(payload, default=str))

    @staticmethod
    def _normalize_date(value):
        """Normalize one date-like value into ``datetime.date``."""
        return normalize_date(value)

    def _resolve_date_window(self, types: str, start_date, end_date):
        """Resolve ETL date window from update mode and payload values."""
        return resolve_date_window(types, start_date, end_date)

    async def _fetch_json_url(self, url: str) -> list:
        """Fetch raw JSON array payload from a URL source."""
        return await self.extractor.fetch_json_url(url)

    async def _fetch_sheet_values(self, range_name: str) -> list:
        """Fetch raw tabular rows from a Google Sheets range."""
        return await self.extractor.fetch_sheet_values(range_name)

    async def _fetch_ga4_daily_metrics(self, start_date, end_date) -> list[dict]:
        """Fetch raw GA4 daily metrics for target date window."""
        return await self.extractor.fetch_ga4_daily_metrics(start_date=start_date, end_date=end_date)

    @staticmethod
    def _normalize_columns(columns: list[str]) -> list[str]:
        """Normalize raw source headers into standardized keys."""
        return normalize_columns(columns)

    @staticmethod
    def _parse_depo_dataframe(raw_data: list):
        """Parse raw deposit payload into normalized dataframe."""
        return parse_depo_dataframe(raw_data)

    @staticmethod
    def _parse_ads_dataframe(raw_rows: list):
        """Parse raw ads rows into normalized dataframe."""
        return parse_ads_dataframe(raw_rows)

    @staticmethod
    def _parse_ga4_dataframe(raw_rows: list[dict]):
        """Parse raw GA4 rows into normalized dataframe."""
        return parse_ga4_dataframe(raw_rows)

    @staticmethod
    def _build_depo_models(df, pull_date):
        """Convert deposit dataframe into ``DataDepo`` payload rows."""
        return build_depo_rows(df=df, pull_date=pull_date)

    @staticmethod
    def _build_ads_models(df, model_cls, pull_date):
        """Convert ads dataframe into ads payload rows."""
        del model_cls
        return build_ads_rows(df=df, pull_date=pull_date)

    @staticmethod
    def _build_ga4_models(df, pull_date):
        """Convert GA4 dataframe into ``ga4_daily_metrics`` payload rows."""
        return build_ga4_rows(df=df, pull_date=pull_date)

    async def data_depo(
        self,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Run deposit ETL flow and persist into ``DataDepo``.

        Args:
            session (AsyncSession): Active database session.
            start_date: Requested start date (used in manual mode).
            end_date: Requested end date (used in manual mode).
            types (str): Update mode (`auto` or `manual`).
            run_id (str | None): ETL run identifier for traceability.

        Returns:
            str: User-facing status message describing ETL outcome.

        Raises:
            fastapi.HTTPException: Raised when extraction, validation, or load fails.
        """
        started_at = perf_counter()
        try:
            target_start, target_end = self._resolve_date_window(types, start_date, end_date)
            self._log_event(
                "etl_data_depo_started",
                run_id=run_id,
                types=types,
                target_start=target_start,
                target_end=target_end,
            )

            if types == "auto":
                existing_rows = await session.execute(
                    select(DataDepo.id).where(DataDepo.tanggal_regis.between(target_start, target_end))
                )
                if existing_rows.first():
                    self._log_event(
                        "etl_data_depo_skipped",
                        run_id=run_id,
                        reason="already_updated",
                        duration_sec=round(perf_counter() - started_at, 3),
                    )
                    return "Data is already updated!"

            raw_data = await self._fetch_json_url(self.depo_source_url)
            staged_count = await stage_depo_raw(
                session=session,
                raw_data=raw_data,
                run_id=run_id,
                source="depo_source_url",
            )
            await session.commit()
            df = self._parse_depo_dataframe(raw_data)
            if df.empty:
                return "No data found from source."

            df = df[(df["tgl_regis"] >= target_start) & (df["tgl_regis"] <= target_end)]

            if df.empty:
                self._log_event(
                    "etl_data_depo_no_rows_in_window",
                    run_id=run_id,
                    raw_count=len(raw_data),
                    staged_count=staged_count,
                    duration_sec=round(perf_counter() - started_at, 3),
                )
                return "No data found for selected date range."

            validate_depo_dataframe(df)
            rows = self._build_depo_models(df=df, pull_date=datetime.now().date())
            await upsert_depo_rows(session=session, rows=rows)
            await session.commit()
            self._log_event(
                "etl_data_depo_completed",
                run_id=run_id,
                raw_count=len(raw_data),
                staged_count=staged_count,
                loaded_count=len(rows),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            return "Data is being updated!"
        except HTTPException:
            self._log_event(
                "etl_data_depo_failed_http",
                run_id=run_id,
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise
        except ValueError as error:
            self._log_event(
                "etl_data_depo_failed_dq",
                run_id=run_id,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(422, str(error))
        except Exception as error:
            self._log_event(
                "etl_data_depo_failed",
                run_id=run_id,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(500, f"Google Sheets error: {str(error)}")

    async def campaign_ads(
        self,
        range_name: str,
        session: AsyncSession,
        classes: type,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Run ads ETL flow for one Sheets range and target ads table.

        Args:
            range_name (str): Google Sheets range in A1 notation.
            session (AsyncSession): Active database session.
            classes (type): Target ads model class (`GoogleAds`, `FacebookAds`, `TikTokAds`).
            start_date: Requested start date (used in manual mode).
            end_date: Requested end date (used in manual mode).
            types (str): Update mode (`auto` or `manual`).
            run_id (str | None): ETL run identifier for traceability.

        Returns:
            str: User-facing status message describing ETL outcome.

        Raises:
            fastapi.HTTPException: Raised when extraction, validation, or load fails.
        """
        started_at = perf_counter()
        try:
            target_start, target_end = self._resolve_date_window(types, start_date, end_date)
            self._log_event(
                "etl_campaign_ads_started",
                run_id=run_id,
                source=classes.__tablename__,
                types=types,
                target_start=target_start,
                target_end=target_end,
            )

            if types == "auto":
                existing_rows = await session.execute(
                    select(classes.id).where(classes.date.between(target_start, target_end))
                )
                if existing_rows.first():
                    self._log_event(
                        "etl_campaign_ads_skipped",
                        run_id=run_id,
                        source=classes.__tablename__,
                        reason="already_updated",
                        duration_sec=round(perf_counter() - started_at, 3),
                    )
                    return "Data is already updated!"

            raw_rows = await self._fetch_sheet_values(range_name)
            staged_count = await stage_ads_raw(
                session=session,
                raw_rows=raw_rows,
                run_id=run_id,
                source=classes.__tablename__,
                range_name=range_name,
            )
            await session.commit()
            df = self._parse_ads_dataframe(raw_rows)
            if df.empty:
                return "No data found from source."

            df = df[(df["date"] >= target_start) & (df["date"] <= target_end)]
            filtered_count = len(df)
            aggregated_applied = False

            if df.empty:
                self._log_event(
                    "etl_campaign_ads_no_rows_in_window",
                    run_id=run_id,
                    source=classes.__tablename__,
                    raw_count=max(len(raw_rows) - 1, 0),
                    staged_count=staged_count,
                    aggregated_applied=aggregated_applied,
                    duration_sec=round(perf_counter() - started_at, 3),
                )
                return "No data found for selected date range."

            aggregated_count = len(df)
            if classes is TikTokAds:
                df = aggregate_ads_dataframe(df)
                aggregated_count = len(df)
                aggregated_applied = True
            validate_ads_dataframe(df)
            rows = self._build_ads_models(df=df, model_cls=classes, pull_date=datetime.now().date())
            await upsert_ads_rows(session=session, model_cls=classes, rows=rows)
            await session.commit()
            self._log_event(
                "etl_campaign_ads_completed",
                run_id=run_id,
                source=classes.__tablename__,
                raw_count=max(len(raw_rows) - 1, 0),
                filtered_count=filtered_count,
                aggregated_count=aggregated_count,
                aggregated_applied=aggregated_applied,
                staged_count=staged_count,
                loaded_count=len(rows),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            return "Data is being updated!"
        except HTTPException:
            self._log_event(
                "etl_campaign_ads_failed_http",
                run_id=run_id,
                source=classes.__tablename__,
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise
        except ValueError as error:
            self._log_event(
                "etl_campaign_ads_failed_dq",
                run_id=run_id,
                source=classes.__tablename__,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(422, str(error))
        except Exception as error:
            self._log_event(
                "etl_campaign_ads_failed",
                run_id=run_id,
                source=classes.__tablename__,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(500, f"Google Sheets error: {str(error)}")

    async def ga4_daily_metrics(
        self,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Run GA4 ETL flow and persist into ``ga4_daily_metrics``.

        Args:
            session (AsyncSession): Active database session.
            start_date: Requested start date (used in manual mode).
            end_date: Requested end date (used in manual mode).
            types (str): Update mode (`auto` or `manual`).
            run_id (str | None): ETL run identifier for traceability.

        Returns:
            str: User-facing status message describing ETL outcome.

        Raises:
            fastapi.HTTPException: Raised when extraction, validation, or load fails.
        """
        started_at = perf_counter()
        try:
            target_start, target_end = self._resolve_date_window(types, start_date, end_date)
            self._log_event(
                "etl_ga4_started",
                run_id=run_id,
                types=types,
                target_start=target_start,
                target_end=target_end,
            )

            if types == "auto":
                existing_rows = await session.execute(
                    select(Ga4DailyMetrics.id).where(Ga4DailyMetrics.date.between(target_start, target_end))
                )
                if existing_rows.first():
                    self._log_event(
                        "etl_ga4_skipped",
                        run_id=run_id,
                        reason="already_updated",
                        duration_sec=round(perf_counter() - started_at, 3),
                    )
                    return "Data is already updated!"

            raw_rows = await self._fetch_ga4_daily_metrics(start_date=target_start, end_date=target_end)
            staged_count = await stage_ga4_raw(
                session=session,
                raw_rows=raw_rows,
                run_id=run_id,
                source="ga4_daily_metrics",
            )
            await session.commit()
            df = self._parse_ga4_dataframe(raw_rows)
            if df.empty:
                return "No data found from source."

            df = df[(df["date"] >= target_start) & (df["date"] <= target_end)]
            if df.empty:
                self._log_event(
                    "etl_ga4_no_rows_in_window",
                    run_id=run_id,
                    raw_count=len(raw_rows),
                    staged_count=staged_count,
                    duration_sec=round(perf_counter() - started_at, 3),
                )
                return "No data found for selected date range."

            validate_ga4_dataframe(df)
            rows = self._build_ga4_models(df=df, pull_date=datetime.now().date())
            await upsert_ga4_rows(session=session, rows=rows)
            await session.commit()
            self._log_event(
                "etl_ga4_completed",
                run_id=run_id,
                raw_count=len(raw_rows),
                staged_count=staged_count,
                loaded_count=len(rows),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            return "Data is being updated!"
        except HTTPException:
            self._log_event(
                "etl_ga4_failed_http",
                run_id=run_id,
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise
        except ValueError as error:
            self._log_event(
                "etl_ga4_failed_dq",
                run_id=run_id,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(422, str(error))
        except Exception as error:
            self._log_event(
                "etl_ga4_failed",
                run_id=run_id,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(500, f"GA4 error: {str(error)}")
