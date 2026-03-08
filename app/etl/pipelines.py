"""Pipeline orchestration for external API ETL."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from time import perf_counter

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import DataDepo, TikTokAds
from app.etl.extract import ExternalApiExtractor
from app.etl.load import build_ads_rows, build_depo_rows, upsert_ads_rows, upsert_depo_rows
from app.etl.quality import validate_ads_dataframe, validate_depo_dataframe
from app.etl.staging import stage_ads_raw, stage_depo_raw
from app.etl.transform import (
    aggregate_ads_dataframe,
    normalize_columns,
    normalize_date,
    parse_ads_dataframe,
    parse_depo_dataframe,
    resolve_date_window,
)


class GoogleSheetApi:
    """Compatibility ETL pipeline for external spreadsheet/API data."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.extractor = ExternalApiExtractor()
        self.service = self.extractor.service
        self.sheet_id = self.extractor.sheet_id
        self.depo_source_url = self.extractor.depo_source_url

    def _log_event(self, event: str, **fields) -> None:
        payload = {"event": event, **fields}
        self.logger.info(json.dumps(payload, default=str))

    @staticmethod
    def _normalize_date(value):
        return normalize_date(value)

    def _resolve_date_window(self, types: str, start_date, end_date):
        return resolve_date_window(types, start_date, end_date)

    async def _fetch_json_url(self, url: str) -> list:
        return await self.extractor.fetch_json_url(url)

    async def _fetch_sheet_values(self, range_name: str) -> list:
        return await self.extractor.fetch_sheet_values(range_name)

    @staticmethod
    def _normalize_columns(columns: list[str]) -> list[str]:
        return normalize_columns(columns)

    @staticmethod
    def _parse_depo_dataframe(raw_data: list):
        return parse_depo_dataframe(raw_data)

    @staticmethod
    def _parse_ads_dataframe(raw_rows: list):
        return parse_ads_dataframe(raw_rows)

    @staticmethod
    def _build_depo_models(df, pull_date):
        return build_depo_rows(df=df, pull_date=pull_date)

    @staticmethod
    def _build_ads_models(df, model_cls, pull_date):
        del model_cls
        return build_ads_rows(df=df, pull_date=pull_date)

    async def data_depo(
        self,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Fetch deposit payload, filter by date, and persist into ``DataDepo``."""
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
        """Fetch ads payload from Sheets, filter by date, and persist into table."""
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
