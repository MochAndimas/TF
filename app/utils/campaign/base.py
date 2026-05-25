"""Shared data loading and common analytics for campaign services."""

from __future__ import annotations

import asyncio
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from app.db.models.external_api import Campaign, DailyRegister, FacebookAds, GoogleAds, TikTokAds
from app.utils.campaign.allocator import CampaignLeadAllocator
from app.utils.campaign.cache_adapter import CampaignCacheAdapter
from app.utils.campaign.frame_builder import CampaignFrameBuilder
from app.utils.campaign.repository import CampaignRepository
from app.utils.campaign.serializer import CampaignSerializer
from app.utils.campaign.types import AdsModel


class CampaignDataBase:
    """Base campaign analytics service with shared loading and aggregations."""

    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.cache_adapter = CampaignCacheAdapter()
        self.serializer = CampaignSerializer()
        self.allocator = CampaignLeadAllocator()
        self.repository = CampaignRepository(
            execute_query=self._execute_query,
            cache=self.cache_adapter,
            serializer=self.serializer,
        )
        self.frame_builder = CampaignFrameBuilder()
        self.df_google = pd.DataFrame()
        self.df_facebook = pd.DataFrame()
        self.df_tiktok = pd.DataFrame()
        self.df_depo = pd.DataFrame()
        self._query_lock = asyncio.Lock()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: date, to_date: date):
        instance = cls(session, from_date, to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        self.df_google = await self._read_ads_db(GoogleAds)
        self.df_facebook = await self._read_ads_db(FacebookAds)
        self.df_tiktok = await self._read_ads_db(TikTokAds)
        self.df_depo = await self._read_depo_db()

    async def _read_ads_db(self, model: type[AdsModel]) -> pd.DataFrame:
        return await self._read_ads_db_with_range(model=model, from_date=self.from_date, to_date=self.to_date)

    async def _read_ads_db_with_range(self, model: type[AdsModel], from_date: date, to_date: date) -> pd.DataFrame:
        cache_key = self.cache_adapter.make_key("ads", model.__tablename__, from_date, to_date)
        cached = self.cache_adapter.get(cache_key)
        if cached is not None:
            return cached

        query = (
            select(
                model.date.label("date"),
                model.campaign_id.label("campaign_id"),
                model.campaign_name.label("campaign_name"),
                model.ad_group.label("ad_group"),
                model.ad_name.label("ad_name"),
                Campaign.ad_source.label("campaign_source"),
                Campaign.ad_type.label("campaign_type"),
                func.sum(model.cost).label("cost"),
                func.sum(model.impressions).label("impressions"),
                func.sum(model.clicks).label("clicks"),
            )
            .join(model.campaign)
            .filter(func.date(model.date).between(from_date, to_date))
            .group_by(
                model.date,
                model.campaign_id,
                model.campaign_name,
                model.ad_group,
                model.ad_name,
                Campaign.ad_source,
                Campaign.ad_type,
            )
        )
        result = await self._execute_query(query)
        rows = result.fetchall()
        if not rows:
            return self.cache_adapter.set(cache_key, self.serializer.empty_ads_frame(self.to_date))

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = await self._attach_register_leads(df=df, from_date=from_date, to_date=to_date)
        return self.cache_adapter.set(cache_key, df)

    async def _read_daily_register_db(self, from_date: date, to_date: date) -> pd.DataFrame:
        return await self.repository.read_daily_register(from_date=from_date, to_date=to_date)

    async def _read_daily_login_db(self, from_date: date, to_date: date) -> pd.DataFrame:
        return await self.repository.read_daily_login(from_date=from_date, to_date=to_date)

    async def _read_daily_activity_db(self, from_date: date, to_date: date, lead_metric: str = "register") -> pd.DataFrame:
        metric_key = lead_metric.strip().lower()
        if metric_key == "login":
            return await self._read_daily_login_db(from_date=from_date, to_date=to_date)
        return await self._read_daily_register_db(from_date=from_date, to_date=to_date)

    async def _attach_activity_leads(
        self,
        df: pd.DataFrame,
        from_date: date,
        to_date: date,
        lead_metric: str = "register",
    ) -> pd.DataFrame:
        if df.empty:
            df["leads"] = 0.0
            return df

        activity_df = await self._read_daily_activity_db(
            from_date=from_date,
            to_date=to_date,
            lead_metric=lead_metric,
        )
        if activity_df.empty:
            df["leads"] = 0.0
            return df

        return self.allocator.attach_activity_leads(df=df, activity_df=activity_df)

    async def _attach_register_leads(self, df: pd.DataFrame, from_date: date, to_date: date) -> pd.DataFrame:
        return await self._attach_activity_leads(
            df=df,
            from_date=from_date,
            to_date=to_date,
            lead_metric="register",
        )

    async def _read_depo_db(self) -> pd.DataFrame:
        return await self.repository.read_depo(from_date=self.from_date, to_date=self.to_date)

    async def _execute_query(self, query: Select):
        async with self._query_lock:
            return await self.session.execute(query)

    def _ads_frame_map(self) -> dict[str, pd.DataFrame]:
        return {"google": self.df_google, "facebook": self.df_facebook, "tiktok": self.df_tiktok}

    @staticmethod
    def _ads_model_map() -> dict[str, type[AdsModel]]:
        return {"google": GoogleAds, "facebook": FacebookAds, "tiktok": TikTokAds}

    def _resolve_source_model(self, source: str) -> type[AdsModel]:
        model_map = self._ads_model_map()
        if source not in model_map:
            supported_sources = ", ".join(sorted(model_map.keys()))
            raise ValueError(f"Unsupported ads source '{source}'. Supported sources: {supported_sources}.")
        return model_map[source]

    def _resolve_source_frame(self, source: str) -> pd.DataFrame:
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{source}'. Supported sources: {supported_sources}.")
        return frames[source]

    async def _load_ads_source_frame(
        self,
        *,
        source: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Load one ads source dataframe for the requested date range."""
        frame = self._resolve_source_frame(source)
        if from_date < self.from_date or to_date > self.to_date:
            return await self._read_ads_db_with_range(
                model=self._resolve_source_model(source),
                from_date=from_date,
                to_date=to_date,
            )
        return frame

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float | None:
        return CampaignLeadAllocator.growth_percentage(current_value, previous_value)

    @staticmethod
    def _serialize_daily_rows(daily: pd.DataFrame) -> list[dict[str, object]]:
        return CampaignSerializer.serialize_daily_rows(daily)

    async def _ads_daily_dataframe(self, data: str, from_date: date, to_date: date) -> pd.DataFrame:
        source = data.strip().lower()
        df = await self._load_ads_source_frame(source=source, from_date=from_date, to_date=to_date)
        return self.frame_builder.ads_daily_frame(df=df, from_date=from_date, to_date=to_date)

    async def _ads_daily_dimension_dataframe(
        self,
        *,
        data: str,
        dimension: str,
        from_date: date,
        to_date: date,
        campaign_type: str | None = None,
        include_leads: bool = False,
    ) -> pd.DataFrame:
        """Build daily per-dimension ads metrics with optional campaign type filter."""
        source = data.strip().lower()
        df = await self._load_ads_source_frame(source=source, from_date=from_date, to_date=to_date)
        return self.frame_builder.ads_daily_dimension_frame(
            df=df,
            dimension=dimension,
            from_date=from_date,
            to_date=to_date,
            campaign_type=campaign_type,
            include_leads=include_leads,
        )

    async def _ads_performance_dataframe(
        self,
        data: str,
        from_date: date,
        to_date: date,
        dimension: str,
        ad_type: str | None = None,
    ) -> pd.DataFrame:
        base = await self._ads_base_details_dataframe(data=data, from_date=from_date, to_date=to_date, ad_type=ad_type)
        return self.frame_builder.ads_performance_frame(base=base, dimension=dimension)

    async def _ads_metrics_from_sql(self, model: type[AdsModel], from_date: date, to_date: date, ad_type: str | None = None) -> dict[str, float]:
        cache_key = self.cache_adapter.make_key("metrics", model.__tablename__, ad_type or "all", from_date, to_date)
        cached = self.cache_adapter.get(cache_key)
        if cached is not None:
            return self.serializer.normalize_ads_metrics_payload(cached.iloc[0].to_dict())

        filters = [model.date.between(from_date, to_date)]
        if ad_type:
            filters.append(Campaign.ad_type == ad_type)

        ads_by_campaign = (
            select(
                model.date.label("date"),
                model.campaign_id.label("campaign_id"),
                func.sum(model.cost).label("cost"),
                func.sum(model.impressions).label("impressions"),
                func.sum(model.clicks).label("clicks"),
            )
            .join(model.campaign)
            .where(*filters)
            .group_by(model.date, model.campaign_id)
            .subquery()
        )
        query = (
            select(
                func.coalesce(func.sum(ads_by_campaign.c.impressions), 0).label("impressions"),
                func.coalesce(func.sum(ads_by_campaign.c.clicks), 0).label("clicks"),
                func.coalesce(func.sum(ads_by_campaign.c.cost), 0.0).label("cost"),
                func.coalesce(func.sum(DailyRegister.total_regis), 0).label("leads"),
            )
            .select_from(ads_by_campaign)
            .outerjoin(
                DailyRegister,
                and_(
                    DailyRegister.date == ads_by_campaign.c.date,
                    DailyRegister.campaign_id == ads_by_campaign.c.campaign_id,
                ),
            )
        )
        row = (await self._execute_query(query)).one()
        leads_total = float(row.leads or 0)
        cost_total = float(row.cost or 0.0)
        payload = {
            "impressions": int(row.impressions or 0),
            "clicks": int(row.clicks or 0),
            "cost": cost_total,
            "leads": int(leads_total),
            "cost_leads": round(cost_total / leads_total, 2) if leads_total else 0.0,
        }
        cached_payload = self.cache_adapter.set(cache_key, pd.DataFrame([payload]))
        return self.serializer.normalize_ads_metrics_payload(cached_payload.iloc[0].to_dict())

    async def _ads_base_details_dataframe(self, data: str, from_date: date, to_date: date, ad_type: str | None = None) -> pd.DataFrame:
        source = data.strip().lower()
        model = self._resolve_source_model(source)
        return await self.repository.read_ads_base_details(
            model=model,
            from_date=from_date,
            to_date=to_date,
            ad_type=ad_type,
        )

    async def ads_metrics(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, float]:
        source = data.strip().lower()
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        return await self._ads_metrics_from_sql(model=self._resolve_source_model(source), from_date=start_date, to_date=end_date)

    async def ads_metrics_with_growth(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        current_from = from_date or self.from_date
        current_to = to_date or self.to_date
        if current_from > current_to:
            raise ValueError("from_date cannot be after to_date.")

        previous_from, previous_to = self._previous_period_range(current_from, current_to)
        current_metrics = await self.ads_metrics(data=data, from_date=current_from, to_date=current_to)
        previous_metrics = await self.ads_metrics(data=data, from_date=previous_from, to_date=previous_to)
        growth = {
            metric: self._growth_percentage(float(current_metrics[metric]), float(previous_metrics[metric]))
            for metric in ("impressions", "clicks", "cost", "leads", "cost_leads")
        }
        return {
            "current_period": {"from_date": current_from.isoformat(), "to_date": current_to.isoformat(), "metrics": current_metrics},
            "previous_period": {"from_date": previous_from.isoformat(), "to_date": previous_to.isoformat(), "metrics": previous_metrics},
            "growth_percentage": growth,
        }

    async def ads_campaign_details_table(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        details_df = await self._ads_base_details_dataframe(data=data, from_date=start_date, to_date=end_date)
        rows = await asyncio.to_thread(lambda: details_df.to_dict(orient="records"))
        return {"source": data.strip().lower(), "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows}
