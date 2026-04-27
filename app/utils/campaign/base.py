"""Shared data loading and common analytics for campaign services."""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Campaign, DailyRegister, DataDepo, FacebookAds, GoogleAds, TikTokAds

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
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return self._empty_ads_frame()

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = await self._attach_register_leads(df=df, from_date=from_date, to_date=to_date)
        return df

    async def _read_daily_register_db(self, from_date: date, to_date: date) -> pd.DataFrame:
        query = (
            select(
                DailyRegister.date.label("date"),
                DailyRegister.campaign_id.label("campaign_id"),
                func.sum(DailyRegister.total_regis).label("leads"),
            )
            .where(DailyRegister.date.between(from_date, to_date))
            .group_by(DailyRegister.date, DailyRegister.campaign_id)
        )
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "campaign_id", "leads"])
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["campaign_id"] = df["campaign_id"].astype(str)
        df["leads"] = pd.to_numeric(df["leads"], errors="coerce").fillna(0.0)
        return df

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
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return self._empty_depo_frame()

        df = pd.DataFrame(rows)
        df["tanggal_regis"] = pd.to_datetime(df["tanggal_regis"]).dt.date
        return df

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

    async def _ads_base_details_dataframe(self, data: str, from_date: date, to_date: date, ad_type: str | None = None) -> pd.DataFrame:
        source = data.strip().lower()
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        df = frames[source]
        if from_date < self.from_date or to_date > self.to_date:
            df = await self._read_ads_db_with_range(model=self._ads_model_map()[source], from_date=from_date, to_date=to_date)

        columns = ["campaign_source", "campaign_id", "campaign_name", "ad_group", "ad_name", "spend", "impressions", "clicks", "leads"]
        if df.empty:
            return pd.DataFrame(columns=columns)

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        if ad_type:
            filtered = filtered.loc[filtered["campaign_type"] == ad_type]
        if filtered.empty:
            return pd.DataFrame(columns=columns)

        for column in ("impressions", "clicks", "cost", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)
        for dim_col in ("campaign_name", "ad_group", "ad_name"):
            filtered[dim_col] = filtered[dim_col].fillna("N/A").replace("", "N/A")

        grouped = (
            filtered.groupby(["campaign_source", "campaign_id", "campaign_name", "ad_group", "ad_name"], as_index=False)[["impressions", "clicks", "cost", "leads"]]
            .sum()
            .sort_values("cost", ascending=False)
        )
        grouped = grouped.rename(columns={"cost": "spend"})
        return grouped[columns]

    async def ads_metrics(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, float]:
        source = data.strip().lower()
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        df = frames[source]
        if start_date < self.from_date or end_date > self.to_date:
            df = await self._read_ads_db_with_range(model=self._ads_model_map()[source], from_date=start_date, to_date=end_date)

        if df.empty:
            return {"impressions": 0, "clicks": 0, "cost": 0.0, "leads": 0, "cost_leads": 0.0}

        filtered = df.loc[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
        if filtered.empty:
            return {"impressions": 0, "clicks": 0, "cost": 0.0, "leads": 0, "cost_leads": 0.0}

        for column in ("impressions", "clicks", "cost", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        totals = filtered[["impressions", "clicks", "cost", "leads"]].agg("sum")
        leads_total = float(totals["leads"])
        cost_total = float(totals["cost"])
        return {
            "impressions": int(totals["impressions"]),
            "clicks": int(totals["clicks"]),
            "cost": cost_total,
            "leads": int(leads_total),
            "cost_leads": round(cost_total / leads_total, 2) if leads_total else 0.0,
        }

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
