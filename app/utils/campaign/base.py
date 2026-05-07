"""Shared data loading and common analytics for campaign services."""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from app.db.models.external_api import Campaign, DailyRegister, DataDepo, FacebookAds, GoogleAds, TikTokAds
from app.utils.analytics_cache import (
    CAMPAIGN_CACHE_PREFIX,
    analytics_dataframe_cache_enabled,
    campaign_dataframe_cache,
)
from app.utils.dataframe_cache import CacheKey

AdsModel = GoogleAds | FacebookAds | TikTokAds


class CampaignDataBase:
    """Base campaign analytics service with shared loading and aggregations."""

    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
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
        cache_key = self._campaign_cache_key("ads", model.__tablename__, from_date, to_date)
        cached = self._get_cached_dataframe(cache_key)
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
            return self._set_cached_dataframe(cache_key, self._empty_ads_frame())

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = await self._attach_register_leads(df=df, from_date=from_date, to_date=to_date)
        return self._set_cached_dataframe(cache_key, df)

    async def _read_daily_register_db(self, from_date: date, to_date: date) -> pd.DataFrame:
        cache_key = self._campaign_cache_key("daily_register", from_date, to_date)
        cached = self._get_cached_dataframe(cache_key)
        if cached is not None:
            return cached

        query = (
            select(
                DailyRegister.date.label("date"),
                DailyRegister.campaign_id.label("campaign_id"),
                func.sum(DailyRegister.total_regis).label("leads"),
            )
            .where(DailyRegister.date.between(from_date, to_date))
            .group_by(DailyRegister.date, DailyRegister.campaign_id)
        )
        result = await self._execute_query(query)
        rows = result.fetchall()
        if not rows:
            return self._set_cached_dataframe(
                cache_key,
                pd.DataFrame(columns=["date", "campaign_id", "leads"]),
            )
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["campaign_id"] = df["campaign_id"].astype(str)
        df["leads"] = pd.to_numeric(df["leads"], errors="coerce").fillna(0.0)
        return self._set_cached_dataframe(cache_key, df)

    async def _attach_register_leads(self, df: pd.DataFrame, from_date: date, to_date: date) -> pd.DataFrame:
        if df.empty:
            df["leads"] = 0.0
            return df

        regis_df = await self._read_daily_register_db(from_date=from_date, to_date=to_date)
        if regis_df.empty:
            df["leads"] = 0.0
            return df

        merged = df.merge(regis_df, on=["date", "campaign_id"], how="left")
        merged["leads"] = pd.to_numeric(merged["leads"], errors="coerce").fillna(0.0)
        merged["cost"] = pd.to_numeric(merged["cost"], errors="coerce").fillna(0.0)
        group_keys = ["date", "campaign_id"]
        merged["_row_count"] = merged.groupby(group_keys)["campaign_id"].transform("size")
        merged["_cost_total"] = merged.groupby(group_keys)["cost"].transform("sum")
        merged["leads"] = merged.apply(
            lambda row: (
                float(row["leads"]) * (float(row["cost"]) / float(row["_cost_total"]))
                if float(row["_cost_total"]) > 0
                else float(row["leads"]) / float(row["_row_count"])
            ),
            axis=1,
        )
        return merged.drop(columns=["_row_count", "_cost_total"])

    async def _read_depo_db(self) -> pd.DataFrame:
        cache_key = self._campaign_cache_key("depo", self.from_date, self.to_date)
        cached = self._get_cached_dataframe(cache_key)
        if cached is not None:
            return cached

        query = (
            select(
                DataDepo.tanggal_regis.label("tanggal_regis"),
                DataDepo.campaign_id.label("campaign_id"),
                Campaign.campaign_name.label("campaign_name"),
                Campaign.ad_source.label("campaign_source"),
                Campaign.ad_type.label("campaign_type"),
                DataDepo.user_status.label("user_status"),
                DataDepo.email.label("email"),
                DataDepo.first_depo.label("first_depo"),
            )
            .join(DataDepo.campaign)
            .filter(DataDepo.tanggal_regis.between(self.from_date, self.to_date))
        )
        result = await self._execute_query(query)
        rows = result.fetchall()
        if not rows:
            return self._set_cached_dataframe(cache_key, self._empty_depo_frame())

        df = pd.DataFrame(rows)
        df["tanggal_regis"] = pd.to_datetime(df["tanggal_regis"]).dt.date
        return self._set_cached_dataframe(cache_key, df)

    @staticmethod
    def _campaign_cache_key(*parts: object) -> CacheKey:
        normalized = tuple(part.isoformat() if isinstance(part, date) else part for part in parts)
        return CAMPAIGN_CACHE_PREFIX + normalized

    @staticmethod
    def _get_cached_dataframe(cache_key: CacheKey) -> pd.DataFrame | None:
        if not analytics_dataframe_cache_enabled():
            return None
        return campaign_dataframe_cache.get(cache_key)

    @staticmethod
    def _set_cached_dataframe(cache_key: CacheKey, df: pd.DataFrame) -> pd.DataFrame:
        if not analytics_dataframe_cache_enabled():
            return df
        return campaign_dataframe_cache.set(cache_key, df)

    def _empty_ads_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": [self.to_date],
                "campaign_id": ["No data"],
                "campaign_name": ["No data"],
                "ad_group": ["No data"],
                "ad_name": ["No data"],
                "campaign_source": ["No data"],
                "campaign_type": ["No data"],
                "cost": [0],
                "impressions": [0],
                "clicks": [0],
                "leads": [0],
            }
        )

    def _empty_depo_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "tanggal_regis": [self.to_date],
                "campaign_id": ["No data"],
                "campaign_name": ["No data"],
                "campaign_source": ["No data"],
                "campaign_type": ["No data"],
                "user_status": ["No data"],
                "email": ["No data"],
                "first_depo": [0.0],
            }
        )

    async def _execute_query(self, query: Select):
        async with self._query_lock:
            return await self.session.execute(query)

    def _ads_frame_map(self) -> dict[str, pd.DataFrame]:
        return {"google": self.df_google, "facebook": self.df_facebook, "tiktok": self.df_tiktok}

    @staticmethod
    def _ads_model_map() -> dict[str, type[AdsModel]]:
        return {"google": GoogleAds, "facebook": FacebookAds, "tiktok": TikTokAds}

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float | None:
        if previous_value == 0:
            if current_value == 0:
                return 0.0
            return 100.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    @staticmethod
    def _serialize_daily_rows(daily: pd.DataFrame) -> list[dict[str, object]]:
        if daily.empty:
            return []
        serializable = daily.copy()
        serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
        return serializable.to_dict(orient="records")

    async def _ads_daily_dataframe(self, data: str, from_date: date, to_date: date) -> pd.DataFrame:
        source = data.strip().lower()
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        df = frames[source]
        if from_date < self.from_date or to_date > self.to_date:
            df = await self._read_ads_db_with_range(model=self._ads_model_map()[source], from_date=from_date, to_date=to_date)

        empty = ["date", "cost", "clicks", "leads", "cost_leads", "clicks_leads"]
        if df.empty:
            return pd.DataFrame(columns=empty)

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        if filtered.empty:
            return pd.DataFrame(columns=empty)

        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        if filtered.empty:
            return pd.DataFrame(columns=empty)

        for column in ("cost", "clicks", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        daily = filtered.groupby("date", as_index=False)[["cost", "clicks", "leads"]].sum().sort_values("date")
        daily["cost_leads"] = daily.apply(
            lambda row: round(float(row["cost"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0,
            axis=1,
        )
        daily["clicks_leads"] = daily.apply(
            lambda row: round(float(row["clicks"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0,
            axis=1,
        )
        return daily

    async def _ads_performance_dataframe(
        self,
        data: str,
        from_date: date,
        to_date: date,
        dimension: str,
        ad_type: str | None = None,
    ) -> pd.DataFrame:
        base = await self._ads_base_details_dataframe(data=data, from_date=from_date, to_date=to_date, ad_type=ad_type)
        columns = [
            "campaign_source",
            "dimension_name",
            "spend",
            "impressions",
            "clicks",
            "leads",
            "click_to_leads_pct",
            "ctr_pct",
            "cpc",
            "cpm",
            "cost_leads",
        ]
        if base.empty:
            return pd.DataFrame(columns=columns)

        base[dimension] = base[dimension].fillna("N/A").replace("", "N/A")
        grouped = (
            base.groupby(["campaign_source", dimension], as_index=False)[["impressions", "clicks", "spend", "leads"]]
            .sum()
            .sort_values("spend", ascending=False)
        )
        grouped = grouped.rename(columns={dimension: "dimension_name"})
        grouped["click_to_leads_pct"] = grouped.apply(
            lambda row: round((float(row["leads"]) / float(row["clicks"])) * 100, 2) if float(row["clicks"]) else 0.0,
            axis=1,
        )
        grouped["ctr_pct"] = grouped.apply(
            lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2) if float(row["impressions"]) else 0.0,
            axis=1,
        )
        grouped["cpc"] = grouped.apply(
            lambda row: round(float(row["spend"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0,
            axis=1,
        )
        grouped["cpm"] = grouped.apply(
            lambda row: round((float(row["spend"]) / float(row["impressions"])) * 1000, 2) if float(row["impressions"]) else 0.0,
            axis=1,
        )
        grouped["cost_leads"] = grouped.apply(
            lambda row: round(float(row["spend"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0,
            axis=1,
        )
        return grouped[columns]

    async def _ads_metrics_from_sql(self, model: type[AdsModel], from_date: date, to_date: date, ad_type: str | None = None) -> dict[str, float]:
        cache_key = self._campaign_cache_key("metrics", model.__tablename__, ad_type or "all", from_date, to_date)
        cached = self._get_cached_dataframe(cache_key)
        if cached is not None:
            return self._normalize_ads_metrics_payload(cached.iloc[0].to_dict())

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
        cached_payload = self._set_cached_dataframe(cache_key, pd.DataFrame([payload]))
        return self._normalize_ads_metrics_payload(cached_payload.iloc[0].to_dict())

    @staticmethod
    def _normalize_ads_metrics_payload(payload: dict[str, object]) -> dict[str, float]:
        impressions = int(float(payload.get("impressions") or 0))
        clicks = int(float(payload.get("clicks") or 0))
        leads = int(float(payload.get("leads") or 0))
        cost = float(payload.get("cost") or 0.0)
        return {
            "impressions": impressions,
            "clicks": clicks,
            "cost": cost,
            "leads": leads,
            "cost_leads": round(cost / leads, 2) if leads else 0.0,
        }

    async def _ads_base_details_dataframe(self, data: str, from_date: date, to_date: date, ad_type: str | None = None) -> pd.DataFrame:
        source = data.strip().lower()
        model_map = self._ads_model_map()
        if source not in model_map:
            supported_sources = ", ".join(sorted(model_map.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        columns = ["campaign_source", "campaign_id", "campaign_name", "ad_group", "ad_name", "spend", "impressions", "clicks", "leads"]
        model = model_map[source]
        cache_key = self._campaign_cache_key("details", model.__tablename__, ad_type or "all", from_date, to_date)
        cached = self._get_cached_dataframe(cache_key)
        if cached is not None:
            return cached

        filters = [model.date.between(from_date, to_date)]
        if ad_type:
            filters.append(Campaign.ad_type == ad_type)

        daily_rows = (
            select(
                model.date.label("date"),
                Campaign.ad_source.label("campaign_source"),
                model.campaign_id.label("campaign_id"),
                model.campaign_name.label("campaign_name"),
                model.ad_group.label("ad_group"),
                model.ad_name.label("ad_name"),
                func.coalesce(func.sum(model.cost), 0.0).label("spend"),
                func.coalesce(func.sum(model.impressions), 0).label("impressions"),
                func.coalesce(func.sum(model.clicks), 0).label("clicks"),
            )
            .join(model.campaign)
            .where(*filters)
            .group_by(
                model.date,
                Campaign.ad_source,
                model.campaign_id,
                model.campaign_name,
                model.ad_group,
                model.ad_name,
            )
            .subquery()
        )
        campaign_daily_totals = (
            select(
                daily_rows.c.date.label("date"),
                daily_rows.c.campaign_id.label("campaign_id"),
                func.sum(daily_rows.c.spend).label("campaign_spend"),
                func.count().label("row_count"),
            )
            .group_by(daily_rows.c.date, daily_rows.c.campaign_id)
            .subquery()
        )
        lead_allocation = case(
            (
                campaign_daily_totals.c.campaign_spend > 0,
                func.coalesce(DailyRegister.total_regis, 0) * (daily_rows.c.spend / campaign_daily_totals.c.campaign_spend),
            ),
            else_=func.coalesce(DailyRegister.total_regis, 0) / campaign_daily_totals.c.row_count,
        )
        query = (
            select(
                daily_rows.c.campaign_source.label("campaign_source"),
                daily_rows.c.campaign_id.label("campaign_id"),
                daily_rows.c.campaign_name.label("campaign_name"),
                daily_rows.c.ad_group.label("ad_group"),
                daily_rows.c.ad_name.label("ad_name"),
                func.coalesce(func.sum(daily_rows.c.spend), 0.0).label("spend"),
                func.coalesce(func.sum(daily_rows.c.impressions), 0).label("impressions"),
                func.coalesce(func.sum(daily_rows.c.clicks), 0).label("clicks"),
                func.coalesce(func.sum(lead_allocation), 0.0).label("leads"),
            )
            .select_from(daily_rows)
            .join(
                campaign_daily_totals,
                and_(
                    campaign_daily_totals.c.date == daily_rows.c.date,
                    campaign_daily_totals.c.campaign_id == daily_rows.c.campaign_id,
                ),
            )
            .outerjoin(
                DailyRegister,
                and_(
                    DailyRegister.date == daily_rows.c.date,
                    DailyRegister.campaign_id == daily_rows.c.campaign_id,
                ),
            )
            .group_by(
                daily_rows.c.campaign_source,
                daily_rows.c.campaign_id,
                daily_rows.c.campaign_name,
                daily_rows.c.ad_group,
                daily_rows.c.ad_name,
            )
            .order_by(func.sum(daily_rows.c.spend).desc())
        )
        rows = (await self._execute_query(query)).fetchall()
        if not rows:
            return self._set_cached_dataframe(cache_key, pd.DataFrame(columns=columns))

        df = pd.DataFrame(rows)
        for dim_col in ("campaign_name", "ad_group", "ad_name"):
            df[dim_col] = df[dim_col].fillna("N/A").replace("", "N/A")
        for metric_col in ("spend", "impressions", "clicks", "leads"):
            df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce").fillna(0)
        return self._set_cached_dataframe(cache_key, df[columns])

    async def ads_metrics(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, float]:
        source = data.strip().lower()
        model_map = self._ads_model_map()
        if source not in model_map:
            supported_sources = ", ".join(sorted(model_map.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        return await self._ads_metrics_from_sql(model=model_map[source], from_date=start_date, to_date=end_date)

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
