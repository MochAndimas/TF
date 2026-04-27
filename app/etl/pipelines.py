"""Pipeline orchestration for external API ETL."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from time import perf_counter

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import DailyRegister, DataDepo, Ga4DailyMetrics
from app.etl.extract import ExternalApiExtractor
from app.etl.load import (
    build_ads_rows,
    build_daily_register_rows,
    build_first_deposit_rows,
    build_ga4_rows,
    upsert_ads_rows,
    upsert_daily_register_rows,
    upsert_first_deposit_rows,
    upsert_ga4_rows,
)
from app.etl.quality import (
    validate_ads_dataframe,
    validate_daily_register_dataframe,
    validate_first_deposit_dataframe,
    validate_ga4_dataframe,
)
from app.etl.staging import stage_ads_raw, stage_first_deposit_raw, stage_ga4_raw
from app.etl.transform import (
    dedupe_ads_dataframe,
    parse_ads_dataframe,
    parse_daily_register_dataframe,
    parse_first_deposit_dataframe,
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

    def _resolve_date_window(self, types: str, start_date, end_date):
        """Resolve ETL date window from update mode and payload values."""
        return resolve_date_window(types, start_date, end_date)

    async def _fetch_sheet_values(self, range_name: str) -> list:
        """Fetch raw tabular rows from a Google Sheets range.

        Args:
            range_name (str): Source range in A1 notation.

        Returns:
            list: Raw values payload returned by the Sheets extractor.
        """
        return await self.extractor.fetch_sheet_values(range_name)

    async def _fetch_google_ads_metrics(self, start_date, end_date) -> list[dict]:
        """Fetch raw Google Ads API metrics for the requested ETL window."""
        return await self.extractor.fetch_google_ads_metrics(start_date=start_date, end_date=end_date)

    async def _fetch_facebook_ads_metrics(self, start_date, end_date) -> list[dict]:
        """Fetch raw Meta Ads API metrics for the requested ETL window."""
        return await self.extractor.fetch_facebook_ads_metrics(start_date=start_date, end_date=end_date)

    async def _fetch_ga4_daily_metrics(self, start_date, end_date) -> list[dict]:
        """Fetch raw GA4 daily metrics for the requested ETL window.

        Args:
            start_date: Inclusive start date for the GA4 query.
            end_date: Inclusive end date for the GA4 query.

        Returns:
            list[dict]: Raw GA4 report rows normalized by the extractor.
        """
        return await self.extractor.fetch_ga4_daily_metrics(start_date=start_date, end_date=end_date)

    async def _fetch_first_deposit_records(self) -> list[dict]:
        """Fetch raw first-deposit rows from the configured external endpoint.

        Returns:
            list[dict]: JSON records returned by the first-deposit extractor.
        """
        return await self.extractor.fetch_first_deposit_records()

    async def _fetch_daily_register_rows(self) -> list:
        """Fetch raw daily registration rows from the configured Google Sheet."""
        return await self.extractor.fetch_daily_register_rows()

    @staticmethod
    def _parse_ads_dataframe(raw_rows: list):
        """Parse raw ads payload rows into a normalized dataframe.

        Args:
            raw_rows (list): Raw rows extracted from Google Sheets.

        Returns:
            pd.DataFrame: Standardized dataframe used by downstream DQ/load steps.
        """
        return parse_ads_dataframe(raw_rows)

    @staticmethod
    def _parse_ga4_dataframe(raw_rows: list[dict]):
        """Parse raw GA4 API rows into a normalized dataframe.

        Args:
            raw_rows (list[dict]): Extractor-normalized GA4 report rows.

        Returns:
            pd.DataFrame: Daily ``date + source`` dataframe for validation/load.
        """
        return parse_ga4_dataframe(raw_rows)

    @staticmethod
    def _parse_first_deposit_dataframe(raw_rows: list[dict]):
        """Parse raw first-deposit API rows into a normalized dataframe.

        Args:
            raw_rows (list[dict]): Raw JSON records fetched from the deposit API.

        Returns:
            pd.DataFrame: Normalized dataframe suitable for first-deposit DQ and
            upsert into ``data_depo``.
        """
        return parse_first_deposit_dataframe(raw_rows)

    @staticmethod
    def _parse_daily_register_dataframe(raw_rows: list):
        """Parse raw daily register rows into a normalized dataframe."""
        return parse_daily_register_dataframe(raw_rows)

    @staticmethod
    def _build_ads_models(df, model_cls, pull_date):
        """Convert normalized ads dataframe into insert payload rows.

        Args:
            df: Validated ads dataframe.
            model_cls: Unused placeholder to keep helper signature aligned with
                other ETL builder helpers.
            pull_date: ETL pull date recorded on loaded rows.

        Returns:
            list[dict]: Ads load payload rows.
        """
        del model_cls
        return build_ads_rows(df=df, pull_date=pull_date)

    @staticmethod
    def _build_ga4_models(df, pull_date):
        """Convert validated GA4 dataframe into load payload rows.

        Args:
            df: Validated GA4 dataframe.
            pull_date: ETL pull date recorded on loaded rows.

        Returns:
            list[dict]: ``ga4_daily_metrics`` payload rows.
        """
        return build_ga4_rows(df=df, pull_date=pull_date)

    @staticmethod
    def _build_first_deposit_models(df, pull_date):
        """Convert validated first-deposit dataframe into load payload rows.

        Args:
            df: Validated first-deposit dataframe.
            pull_date: ETL pull date recorded on loaded rows.

        Returns:
            list[dict]: ``data_depo`` payload rows for idempotent upsert.
        """
        return build_first_deposit_rows(df=df, pull_date=pull_date)

    @staticmethod
    def _build_daily_register_models(df, pull_date):
        """Convert validated daily register dataframe into load payload rows."""
        return build_daily_register_rows(df=df, pull_date=pull_date)

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

            source_name = classes.__tablename__
            api_range_name_map = {
                "google_ads": "google_ads_api",
                "facebook_ads": "meta_ads_api",
            }
            if source_name == "google_ads":
                raw_rows = await self._fetch_google_ads_metrics(start_date=target_start, end_date=target_end)
            elif source_name == "facebook_ads":
                raw_rows = await self._fetch_facebook_ads_metrics(start_date=target_start, end_date=target_end)
            else:
                raw_rows = await self._fetch_sheet_values(range_name)
            staged_count = await stage_ads_raw(
                session=session,
                raw_rows=raw_rows,
                run_id=run_id,
                source=source_name,
                range_name=api_range_name_map.get(source_name, range_name),
            )
            await session.commit()
            df = self._parse_ads_dataframe(raw_rows)
            if df.empty:
                return "No data found from source."

            df = df[(df["date"] >= target_start) & (df["date"] <= target_end)]
            filtered_count = len(df)

            if df.empty:
                self._log_event(
                    "etl_campaign_ads_no_rows_in_window",
                    run_id=run_id,
                    source=classes.__tablename__,
                    raw_count=len(raw_rows) if isinstance(raw_rows[0], dict) else max(len(raw_rows) - 1, 0),
                    staged_count=staged_count,
                    dedupe_applied=False,
                    dedupe_dropped_count=0,
                    duration_sec=round(perf_counter() - started_at, 3),
                )
                return "No data found for selected date range."

            df, dedupe_dropped_count = dedupe_ads_dataframe(df)
            dedupe_applied = dedupe_dropped_count > 0
            validate_ads_dataframe(df)
            rows = self._build_ads_models(df=df, model_cls=classes, pull_date=datetime.now().date())
            await upsert_ads_rows(session=session, model_cls=classes, rows=rows)
            await session.commit()
            self._log_event(
                "etl_campaign_ads_completed",
                run_id=run_id,
                source=classes.__tablename__,
                raw_count=len(raw_rows) if isinstance(raw_rows[0], dict) else max(len(raw_rows) - 1, 0),
                filtered_count=filtered_count,
                dedupe_applied=dedupe_applied,
                dedupe_dropped_count=dedupe_dropped_count,
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
            raise HTTPException(500, f"Ads source error: {str(error)}")

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

    async def daily_register(
        self,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Run daily registration ETL flow into ``daily_register``."""
        started_at = perf_counter()
        try:
            target_start, target_end = self._resolve_date_window(types, start_date, end_date)
            self._log_event(
                "etl_daily_register_started",
                run_id=run_id,
                types=types,
                target_start=target_start,
                target_end=target_end,
            )

            if types == "auto":
                existing_rows = await session.execute(
                    select(DailyRegister.id).where(DailyRegister.date.between(target_start, target_end))
                )
                if existing_rows.first():
                    self._log_event(
                        "etl_daily_register_skipped",
                        run_id=run_id,
                        reason="already_updated",
                        duration_sec=round(perf_counter() - started_at, 3),
                    )
                    return "Data is already updated!"

            raw_rows = await self._fetch_daily_register_rows()
            staged_count = await stage_ads_raw(
                session=session,
                raw_rows=raw_rows,
                run_id=run_id,
                source="daily_register",
                range_name=self.extractor.daily_regis_sheet_range,
            )
            await session.commit()
            df = self._parse_daily_register_dataframe(raw_rows)
            if df.empty:
                return "No data found from source."

            df = df[(df["date"] >= target_start) & (df["date"] <= target_end)]
            filtered_count = len(df)
            if df.empty:
                self._log_event(
                    "etl_daily_register_no_rows_in_window",
                    run_id=run_id,
                    raw_count=max(len(raw_rows) - 1, 0),
                    staged_count=staged_count,
                    duration_sec=round(perf_counter() - started_at, 3),
                )
                return "No data found for selected date range."

            validate_daily_register_dataframe(df)
            rows = self._build_daily_register_models(df=df, pull_date=datetime.now().date())
            await upsert_daily_register_rows(session=session, rows=rows)
            await session.commit()
            self._log_event(
                "etl_daily_register_completed",
                run_id=run_id,
                raw_count=max(len(raw_rows) - 1, 0),
                staged_count=staged_count,
                filtered_count=filtered_count,
                loaded_count=len(rows),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            return "Data is being updated!"
        except HTTPException:
            self._log_event(
                "etl_daily_register_failed_http",
                run_id=run_id,
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise
        except ValueError as error:
            self._log_event(
                "etl_daily_register_failed_dq",
                run_id=run_id,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(422, str(error))
        except Exception as error:
            self._log_event(
                "etl_daily_register_failed",
                run_id=run_id,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(500, f"Daily register error: {str(error)}")

    async def first_deposit(
        self,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Run the end-to-end first-deposit ETL flow into ``data_depo``.

        This pipeline follows the same orchestration pattern as ads and GA4:
        resolve window, extract JSON payload, stage raw records, transform,
        validate, and finally upsert the normalized rows into the target table.

        Args:
            session (AsyncSession): Active database session.
            start_date: Requested manual start date.
            end_date: Requested manual end date.
            types (str): Update mode, either ``auto`` or ``manual``.
            run_id (str | None): ETL run identifier used for traceability.

        Returns:
            str: User-facing ETL status message.

        Raises:
            fastapi.HTTPException: Raised when extraction, DQ, or load fails.
        """
        started_at = perf_counter()
        try:
            target_start, target_end = self._resolve_date_window(types, start_date, end_date)
            self._log_event(
                "etl_first_deposit_started",
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
                        "etl_first_deposit_skipped",
                        run_id=run_id,
                        reason="already_updated",
                        duration_sec=round(perf_counter() - started_at, 3),
                    )
                    return "Data is already updated!"

            raw_rows = await self._fetch_first_deposit_records()
            staged_count = await stage_first_deposit_raw(
                session=session,
                raw_rows=raw_rows,
                run_id=run_id,
                source="first_deposit",
            )
            await session.commit()
            df = self._parse_first_deposit_dataframe(raw_rows)
            if df.empty:
                return "No data found from source."

            df = df[(df["tanggal_regis"] >= target_start) & (df["tanggal_regis"] <= target_end)]
            filtered_count = len(df)
            if df.empty:
                self._log_event(
                    "etl_first_deposit_no_rows_in_window",
                    run_id=run_id,
                    raw_count=len(raw_rows),
                    staged_count=staged_count,
                    duration_sec=round(perf_counter() - started_at, 3),
                )
                return "No data found for selected date range."

            validate_first_deposit_dataframe(df)
            rows = self._build_first_deposit_models(df=df, pull_date=datetime.now().date())
            await upsert_first_deposit_rows(session=session, rows=rows)
            await session.commit()
            self._log_event(
                "etl_first_deposit_completed",
                run_id=run_id,
                raw_count=len(raw_rows),
                staged_count=staged_count,
                filtered_count=filtered_count,
                loaded_count=len(rows),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            return "Data is being updated!"
        except HTTPException:
            self._log_event(
                "etl_first_deposit_failed_http",
                run_id=run_id,
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise
        except ValueError as error:
            self._log_event(
                "etl_first_deposit_failed_dq",
                run_id=run_id,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(422, str(error))
        except Exception as error:
            self._log_event(
                "etl_first_deposit_failed",
                run_id=run_id,
                error=str(error),
                duration_sec=round(perf_counter() - started_at, 3),
            )
            raise HTTPException(500, f"First deposit error: {str(error)}")
