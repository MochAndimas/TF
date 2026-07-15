"""SQL repository for campaign analytics data access."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date

import pandas as pd
from sqlalchemy import and_, case, func, select

from app.db.models.external_api import Campaign, DailyRegister, DataDepo, DataMsDeposit
from app.utils.campaign.cache_adapter import CampaignCacheAdapter
from app.utils.campaign.serializer import CampaignSerializer
from app.utils.campaign.types import AdsModel

ExecuteQuery = Callable[[object], Awaitable[object]]


class CampaignRepository:
    """Encapsulates SQL query construction + dataframe normalization."""

    def __init__(
        self,
        *,
        execute_query: ExecuteQuery,
        cache: CampaignCacheAdapter,
        serializer: CampaignSerializer,
    ) -> None:
        self._execute_query = execute_query
        self._cache = cache
        self._serializer = serializer

    async def read_daily_register(self, *, from_date: date, to_date: date) -> pd.DataFrame:
        cache_key = self._cache.make_key("daily_register", from_date, to_date)
        cached = self._cache.get(cache_key)
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
        rows = (await self._execute_query(query)).fetchall()
        if not rows:
            return self._cache.set(cache_key, pd.DataFrame(columns=["date", "campaign_id", "leads"]))

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["campaign_id"] = df["campaign_id"].astype(str)
        df["leads"] = pd.to_numeric(df["leads"], errors="coerce").fillna(0.0)
        return self._cache.set(cache_key, df)

    async def read_daily_login(self, *, from_date: date, to_date: date) -> pd.DataFrame:
        cache_key = self._cache.make_key("daily_login", from_date, to_date)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        query = (
            select(
                DataMsDeposit.last_activity.label("date"),
                DataMsDeposit.campaign_id.label("campaign_id"),
                func.count(func.distinct(DataMsDeposit.email)).label("leads"),
            )
            .where(DataMsDeposit.last_activity.between(from_date, to_date))
            .group_by(DataMsDeposit.last_activity, DataMsDeposit.campaign_id)
        )
        rows = (await self._execute_query(query)).fetchall()
        if not rows:
            return self._cache.set(cache_key, pd.DataFrame(columns=["date", "campaign_id", "leads"]))

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["campaign_id"] = df["campaign_id"].astype(str)
        df["leads"] = pd.to_numeric(df["leads"], errors="coerce").fillna(0.0)
        return self._cache.set(cache_key, df)

    async def read_depo(self, *, from_date: date, to_date: date) -> pd.DataFrame:
        cache_key = self._cache.make_key("depo", from_date, to_date)
        cached = self._cache.get(cache_key)
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
            .filter(DataDepo.tanggal_regis.between(from_date, to_date))
        )
        rows = (await self._execute_query(query)).fetchall()
        if not rows:
            return self._cache.set(cache_key, self._serializer.empty_depo_frame(to_date))

        df = pd.DataFrame(rows)
        df["tanggal_regis"] = pd.to_datetime(df["tanggal_regis"]).dt.date
        return self._cache.set(cache_key, df)

    async def read_ads_base_details(
        self,
        *,
        model: type[AdsModel],
        from_date: date,
        to_date: date,
        ad_type: str | None,
    ) -> pd.DataFrame:
        columns = ["campaign_source", "campaign_id", "campaign_name", "ad_group", "ad_name", "spend", "impressions", "clicks", "leads"]
        cache_key = self._cache.make_key("details", model.__tablename__, ad_type or "all", from_date, to_date)
        cached = self._cache.get(cache_key)
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
            .group_by(model.date, Campaign.ad_source, model.campaign_id, model.campaign_name, model.ad_group, model.ad_name)
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
        daily_register = (
            select(
                DailyRegister.date.label("date"),
                DailyRegister.campaign_id.label("campaign_id"),
                func.sum(DailyRegister.total_regis).label("total_regis"),
            )
            .where(DailyRegister.date.between(from_date, to_date))
            .group_by(DailyRegister.date, DailyRegister.campaign_id)
            .subquery()
        )
        lead_allocation = case(
            (campaign_daily_totals.c.campaign_spend > 0, func.coalesce(daily_register.c.total_regis, 0) * (daily_rows.c.spend / campaign_daily_totals.c.campaign_spend)),
            else_=func.coalesce(daily_register.c.total_regis, 0) / campaign_daily_totals.c.row_count,
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
            .join(campaign_daily_totals, and_(campaign_daily_totals.c.date == daily_rows.c.date, campaign_daily_totals.c.campaign_id == daily_rows.c.campaign_id))
            .outerjoin(daily_register, and_(daily_register.c.date == daily_rows.c.date, daily_register.c.campaign_id == daily_rows.c.campaign_id))
            .group_by(daily_rows.c.campaign_source, daily_rows.c.campaign_id, daily_rows.c.campaign_name, daily_rows.c.ad_group, daily_rows.c.ad_name)
            .order_by(func.sum(daily_rows.c.spend).desc())
        )
        rows = (await self._execute_query(query)).fetchall()
        if not rows:
            return self._cache.set(cache_key, pd.DataFrame(columns=columns))
        df = pd.DataFrame(rows)
        for dim_col in ("campaign_name", "ad_group", "ad_name"):
            df[dim_col] = df[dim_col].fillna("N/A").replace("", "N/A")
        for metric_col in ("spend", "impressions", "clicks", "leads"):
            df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce").fillna(0)
        return self._cache.set(cache_key, df[columns])
