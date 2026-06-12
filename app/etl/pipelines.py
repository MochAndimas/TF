"""Pipeline orchestration for external API ETL."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import (
    DailyRegister,
    DataDepo,
    DataMsDeposit,
    Ga4DailyMetrics,
    InstagramInsights,
    InstagramMediaInsights,
)
from app.etl.extract import ExternalApiExtractor
from app.etl.load import (
    build_ads_rows,
    build_daily_register_rows,
    build_first_deposit_rows,
    build_ms_deposit_rows,
    build_ga4_rows,
    build_instagram_insights_rows,
    build_instagram_media_insights_rows,
    delete_first_deposit_rows_in_window,
    delete_ms_deposit_rows_in_window,
    delete_rows_in_date_window,
    upsert_ads_rows,
    upsert_daily_register_rows,
    upsert_first_deposit_rows,
    upsert_ms_deposit_rows,
    upsert_ga4_rows,
    upsert_instagram_insights_rows,
    upsert_instagram_media_insights_rows,
)
from app.etl.quality import (
    validate_ads_dataframe,
    validate_daily_register_dataframe,
    validate_first_deposit_dataframe,
    validate_ms_deposit_dataframe,
    validate_ga4_dataframe,
    validate_instagram_insights_dataframe,
    validate_instagram_media_insights_dataframe,
)
from app.etl.pipeline_core import DateWindowPipelineRunner, DateWindowPipelineSpec
from app.etl.staging import (
    stage_ads_raw,
    stage_first_deposit_raw,
    stage_ga4_raw,
    stage_instagram_insights_raw,
    stage_instagram_media_insights_raw,
    stage_ms_deposit_raw,
)
from app.etl.transform import (
    parse_ads_dataframe,
    parse_daily_register_dataframe,
    parse_first_deposit_dataframe,
    parse_ms_deposit_dataframe,
    parse_ga4_dataframe,
    parse_instagram_insights_dataframe,
    parse_instagram_media_insights_dataframe,
)


class GoogleSheetApi(DateWindowPipelineRunner):
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
        super().__init__()
        self.extractor = ExternalApiExtractor()
        self.service = self.extractor.service
        self.sheet_id = self.extractor.sheet_id

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

    async def _fetch_instagram_insights(self, start_date, end_date) -> list[dict]:
        """Fetch raw Instagram daily insight metrics for the requested ETL window."""
        return await self.extractor.fetch_instagram_insights(start_date=start_date, end_date=end_date)

    async def _fetch_instagram_media_insights(self, start_date, end_date) -> list[dict]:
        """Fetch raw Instagram post/reels media insight metrics for the requested ETL window."""
        return await self.extractor.fetch_instagram_media_insights(start_date=start_date, end_date=end_date)

    async def _fetch_first_deposit_records(self) -> list[dict]:
        """Fetch raw first-deposit rows from the configured external endpoint.

        Returns:
            list[dict]: JSON records returned by the first-deposit extractor.
        """
        return await self.extractor.fetch_first_deposit_records()

    async def _fetch_ms_deposit_records(self) -> list[dict]:
        """Fetch raw MS1 deposit/activity rows from the configured sheet."""
        return await self.extractor.fetch_ms_deposit_records()

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
    def _parse_ms_deposit_dataframe(raw_rows: list[dict]):
        """Parse raw MS1 deposit/activity rows into a normalized dataframe."""
        return parse_ms_deposit_dataframe(raw_rows)

    @staticmethod
    def _parse_daily_register_dataframe(raw_rows: list):
        """Parse raw daily register rows into a normalized dataframe."""
        return parse_daily_register_dataframe(raw_rows)

    @staticmethod
    def _parse_instagram_insights_dataframe(raw_rows: list[dict]):
        """Parse raw Instagram insights into a normalized dataframe."""
        return parse_instagram_insights_dataframe(raw_rows)

    @staticmethod
    def _parse_instagram_media_insights_dataframe(raw_rows: list[dict]):
        """Parse raw Instagram media insights into a normalized dataframe."""
        return parse_instagram_media_insights_dataframe(raw_rows)

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
    def _build_ms_deposit_models(df, pull_date):
        """Convert validated MS1 deposit/activity rows into load payloads."""
        return build_ms_deposit_rows(df=df, pull_date=pull_date)

    @staticmethod
    def _build_daily_register_models(df, pull_date):
        """Convert validated daily register dataframe into load payload rows."""
        return build_daily_register_rows(df=df, pull_date=pull_date)

    @staticmethod
    def _build_instagram_insights_models(df, pull_date):
        """Convert validated Instagram insights dataframe into load payload rows."""
        return build_instagram_insights_rows(df=df, pull_date=pull_date)

    @staticmethod
    def _build_instagram_media_insights_models(df, pull_date):
        """Convert validated Instagram media insights dataframe into load payload rows."""
        return build_instagram_media_insights_rows(df=df, pull_date=pull_date)

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
        source_name = classes.__tablename__
        api_range_name_map = {
            "google_ads": "google_ads_api",
            "facebook_ads": "meta_ads_api",
        }

        async def extract(target_start, target_end):
            if source_name == "google_ads":
                return await self._fetch_google_ads_metrics(start_date=target_start, end_date=target_end)
            if source_name == "facebook_ads":
                return await self._fetch_facebook_ads_metrics(start_date=target_start, end_date=target_end)
            return await self._fetch_sheet_values(range_name)

        async def stage(session_: AsyncSession, raw_rows: list, run_id_: str | None) -> int:
            return await stage_ads_raw(
                session=session_,
                raw_rows=raw_rows,
                run_id=run_id_,
                source=source_name,
                range_name=api_range_name_map.get(source_name, range_name),
            )

        async def delete_window(session_: AsyncSession, target_start, target_end) -> int:
            return await delete_rows_in_date_window(
                session=session_,
                model_cls=classes,
                window_start=target_start,
                window_end=target_end,
            )

        async def load_rows(session_: AsyncSession, rows: list[dict]) -> None:
            await upsert_ads_rows(session=session_, model_cls=classes, rows=rows)

        spec = DateWindowPipelineSpec(
            label="campaign_ads",
            source=source_name,
            empty_metric_name="Ads source",
            date_column="date",
            auto_skip_model=classes,
            extract=extract,
            stage=stage,
            parse=self._parse_ads_dataframe,
            validate=validate_ads_dataframe,
            build_rows=lambda df, pull_date: self._build_ads_models(
                df=df,
                model_cls=classes,
                pull_date=pull_date,
            ),
            delete_window=delete_window,
            load_rows=load_rows,
        )
        return await self._run_date_window_pipeline(
            spec=spec,
            session=session,
            start_date=start_date,
            end_date=end_date,
            types=types,
            run_id=run_id,
        )

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
        async def extract(target_start, target_end):
            return await self._fetch_ga4_daily_metrics(start_date=target_start, end_date=target_end)

        async def stage(session_: AsyncSession, raw_rows: list, run_id_: str | None) -> int:
            return await stage_ga4_raw(
                session=session_,
                raw_rows=raw_rows,
                run_id=run_id_,
                source="ga4_daily_metrics",
            )

        async def delete_window(session_: AsyncSession, target_start, target_end) -> int:
            return await delete_rows_in_date_window(
                session=session_,
                model_cls=Ga4DailyMetrics,
                window_start=target_start,
                window_end=target_end,
            )

        spec = DateWindowPipelineSpec(
            label="ga4",
            source="ga4_daily_metrics",
            empty_metric_name="GA4",
            date_column="date",
            auto_skip_model=Ga4DailyMetrics,
            extract=extract,
            stage=stage,
            parse=self._parse_ga4_dataframe,
            validate=validate_ga4_dataframe,
            build_rows=self._build_ga4_models,
            delete_window=delete_window,
            load_rows=upsert_ga4_rows,
        )
        return await self._run_date_window_pipeline(
            spec=spec,
            session=session,
            start_date=start_date,
            end_date=end_date,
            types=types,
            run_id=run_id,
        )

    async def daily_register(
        self,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Run daily registration ETL flow into ``daily_register``."""
        async def extract(_target_start, _target_end):
            return await self._fetch_daily_register_rows()

        async def stage(session_: AsyncSession, raw_rows: list, run_id_: str | None) -> int:
            return await stage_ads_raw(
                session=session_,
                raw_rows=raw_rows,
                run_id=run_id_,
                source="daily_register",
                range_name=self.extractor.daily_regis_sheet_range,
            )

        async def delete_window(session_: AsyncSession, target_start, target_end) -> int:
            return await delete_rows_in_date_window(
                session=session_,
                model_cls=DailyRegister,
                window_start=target_start,
                window_end=target_end,
            )

        spec = DateWindowPipelineSpec(
            label="daily_register",
            source="daily_register",
            empty_metric_name="Daily register",
            date_column="date",
            auto_skip_model=DailyRegister,
            extract=extract,
            stage=stage,
            parse=self._parse_daily_register_dataframe,
            validate=validate_daily_register_dataframe,
            build_rows=self._build_daily_register_models,
            delete_window=delete_window,
            load_rows=upsert_daily_register_rows,
        )
        return await self._run_date_window_pipeline(
            spec=spec,
            session=session,
            start_date=start_date,
            end_date=end_date,
            types=types,
            run_id=run_id,
        )

    async def instagram_insights(
        self,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Run Instagram Insights ETL flow into ``instagram_insights``."""
        existing_total_followers: dict = {}
        snapshot_date = None

        async def extract(target_start, target_end):
            return await self._fetch_instagram_insights(start_date=target_start, end_date=target_end)

        async def stage(session_: AsyncSession, raw_rows: list, run_id_: str | None) -> int:
            return await stage_instagram_insights_raw(
                session=session_,
                raw_rows=raw_rows,
                run_id=run_id_,
                source="instagram_insights",
            )

        async def delete_window(session_: AsyncSession, target_start, target_end) -> int:
            nonlocal existing_total_followers, snapshot_date
            snapshot_date = target_end if target_start == target_end else None
            existing_rows = await session_.execute(
                select(
                    InstagramInsights.date,
                    InstagramInsights.total_followers,
                ).where(InstagramInsights.date.between(target_start, target_end))
            )
            existing_total_followers = {
                row.date: int(row.total_followers or 0)
                for row in existing_rows
                if int(row.total_followers or 0) > 0
            }
            return await delete_rows_in_date_window(
                session=session_,
                model_cls=InstagramInsights,
                window_start=target_start,
                window_end=target_end,
            )

        async def load_rows(session_: AsyncSession, rows: list[dict]) -> None:
            safe_rows = []
            for row in rows:
                safe_row = dict(row)
                row_date = safe_row.get("date")
                if row_date in existing_total_followers:
                    safe_row["total_followers"] = existing_total_followers[row_date]
                elif snapshot_date is not None and row_date != snapshot_date:
                    safe_row["total_followers"] = 0
                safe_rows.append(safe_row)
            await upsert_instagram_insights_rows(session=session_, rows=safe_rows)

        spec = DateWindowPipelineSpec(
            label="instagram_insights",
            source="instagram_insights",
            empty_metric_name="Instagram insights",
            date_column="date",
            auto_skip_model=InstagramInsights,
            extract=extract,
            stage=stage,
            parse=self._parse_instagram_insights_dataframe,
            validate=validate_instagram_insights_dataframe,
            build_rows=self._build_instagram_insights_models,
            delete_window=delete_window,
            load_rows=load_rows,
        )
        return await self._run_date_window_pipeline(
            spec=spec,
            session=session,
            start_date=start_date,
            end_date=end_date,
            types=types,
            run_id=run_id,
        )

    async def instagram_media_insights(
        self,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Run Instagram post/reels media Insights ETL into ``instagram_media_insights``."""
        async def extract(target_start, target_end):
            return await self._fetch_instagram_media_insights(start_date=target_start, end_date=target_end)

        async def stage(session_: AsyncSession, raw_rows: list, run_id_: str | None) -> int:
            return await stage_instagram_media_insights_raw(
                session=session_,
                raw_rows=raw_rows,
                run_id=run_id_,
                source="instagram_media_insights",
            )

        async def delete_window(session_: AsyncSession, target_start, target_end) -> int:
            return await delete_rows_in_date_window(
                session=session_,
                model_cls=InstagramMediaInsights,
                window_start=target_start,
                window_end=target_end,
            )

        spec = DateWindowPipelineSpec(
            label="instagram_media_insights",
            source="instagram_media_insights",
            empty_metric_name="Instagram media insights",
            date_column="date",
            auto_skip_model=InstagramMediaInsights,
            extract=extract,
            stage=stage,
            parse=self._parse_instagram_media_insights_dataframe,
            validate=validate_instagram_media_insights_dataframe,
            build_rows=self._build_instagram_media_insights_models,
            delete_window=delete_window,
            load_rows=upsert_instagram_media_insights_rows,
        )
        return await self._run_date_window_pipeline(
            spec=spec,
            session=session,
            start_date=start_date,
            end_date=end_date,
            types=types,
            run_id=run_id,
        )

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
        async def extract(_target_start, _target_end):
            return await self._fetch_first_deposit_records()

        async def stage(session_: AsyncSession, raw_rows: list, run_id_: str | None) -> int:
            return await stage_first_deposit_raw(
                session=session_,
                raw_rows=raw_rows,
                run_id=run_id_,
                source="first_deposit",
            )

        async def delete_window(session_: AsyncSession, target_start, target_end) -> int:
            return await delete_first_deposit_rows_in_window(
                session=session_,
                window_start=target_start,
                window_end=target_end,
            )

        spec = DateWindowPipelineSpec(
            label="first_deposit",
            source="first_deposit",
            empty_metric_name="First deposit",
            date_column="tanggal_regis",
            auto_skip_model=DataDepo,
            extract=extract,
            stage=stage,
            parse=self._parse_first_deposit_dataframe,
            validate=validate_first_deposit_dataframe,
            build_rows=self._build_first_deposit_models,
            delete_window=delete_window,
            load_rows=upsert_first_deposit_rows,
        )
        return await self._run_date_window_pipeline(
            spec=spec,
            session=session,
            start_date=start_date,
            end_date=end_date,
            types=types,
            run_id=run_id,
        )

    async def ms_deposit(
        self,
        session: AsyncSession,
        start_date=None,
        end_date=None,
        types: str = "auto",
        run_id: str | None = None,
    ) -> str:
        """Run the MS1 deposit/activity ETL flow into ``data_ms_deposit``."""
        async def extract(_target_start, _target_end):
            return await self._fetch_ms_deposit_records()

        async def stage(session_: AsyncSession, raw_rows: list, run_id_: str | None) -> int:
            return await stage_ms_deposit_raw(
                session=session_,
                raw_rows=raw_rows,
                run_id=run_id_,
                source="ms_deposit",
            )

        async def delete_window(session_: AsyncSession, target_start, target_end) -> int:
            return await delete_ms_deposit_rows_in_window(
                session=session_,
                window_start=target_start,
                window_end=target_end,
            )

        spec = DateWindowPipelineSpec(
            label="ms_deposit",
            source="ms_deposit",
            empty_metric_name="MS deposit",
            date_column="last_activity",
            auto_skip_model=DataMsDeposit,
            extract=extract,
            stage=stage,
            parse=self._parse_ms_deposit_dataframe,
            validate=validate_ms_deposit_dataframe,
            build_rows=self._build_ms_deposit_models,
            delete_window=delete_window,
            load_rows=upsert_ms_deposit_rows,
        )
        return await self._run_date_window_pipeline(
            spec=spec,
            session=session,
            start_date=start_date,
            end_date=end_date,
            types=types,
            run_id=run_id,
        )
