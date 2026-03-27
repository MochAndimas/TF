"""Overview campaign-cost service."""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Campaign, FacebookAds, GoogleAds, TikTokAds


class OverviewCampaignCostData:
    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_cost = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: date, to_date: date):
        instance = cls(session=session, from_date=from_date, to_date=to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        self.df_cost = await self._read_ads_cost_db_with_range(from_date=self.from_date, to_date=self.to_date)

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float:
        if previous_value == 0:
            return 100.0 if current_value else 0.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    async def _read_one_source_cost(self, model, source_key: str, from_date: date, to_date: date) -> pd.DataFrame:
        query = (
            select(model.date.label("date"), Campaign.ad_type.label("campaign_type"), func.sum(model.cost).label("cost"))
            .join(model.campaign)
            .where(func.date(model.date).between(from_date, to_date))
            .group_by(model.date, Campaign.ad_type)
        )
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "campaign_type", "cost", "source"])
        dataframe = pd.DataFrame(rows)
        dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date
        dataframe["campaign_type"] = dataframe["campaign_type"].fillna("unknown").astype(str).str.strip().str.lower()
        dataframe["cost"] = pd.to_numeric(dataframe["cost"], errors="coerce").fillna(0.0)
        dataframe["source"] = source_key
        return dataframe

    async def _read_ads_cost_db_with_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        frames = [
            await self._read_one_source_cost(GoogleAds, "google", from_date, to_date),
            await self._read_one_source_cost(FacebookAds, "facebook", from_date, to_date),
            await self._read_one_source_cost(TikTokAds, "tiktok", from_date, to_date),
        ]
        non_empty_frames = [frame for frame in frames if not frame.empty]
        if not non_empty_frames:
            return pd.DataFrame(columns=["date", "campaign_type", "cost", "source"])
        dataframe = pd.concat(non_empty_frames, ignore_index=True)
        if dataframe.empty:
            return pd.DataFrame(columns=["date", "campaign_type", "cost", "source"])
        return dataframe

    async def _frame_for_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_cost.empty:
            return self.df_cost.loc[(self.df_cost["date"] >= from_date) & (self.df_cost["date"] <= to_date)].copy()
        return await self._read_ads_cost_db_with_range(from_date=from_date, to_date=to_date)

    @staticmethod
    async def _pie_payload(title: str, labels: list[str], values: list[float]) -> dict[str, object]:
        if not labels or not values or not any(float(v) > 0 for v in values):
            figure = go.Figure()
            figure.update_layout(title=title, showlegend=False, margin=dict(l=24, r=24, t=56, b=24), annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
            rows = []
        else:
            figure = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.38, textinfo="label+percent", hovertemplate="<b>%{label}</b><br>Cost: Rp. %{value:,.0f}<extra></extra>")])
            figure.update_layout(title=title, showlegend=False, margin=dict(l=24, r=24, t=56, b=24))
            rows = [{"label": label, "cost": float(value)} for label, value in zip(labels, values)]
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        return {"rows": rows, "figure": json.loads(chart_json)}

    async def cost_metrics_with_growth(self, from_date: date, to_date: date) -> dict[str, object]:
        current_df = await self._frame_for_range(from_date, to_date)
        previous_from, previous_to = self._previous_period_range(from_date, to_date)
        previous_df = await self._read_ads_cost_db_with_range(previous_from, previous_to)
        current_source_cost = {source: float(current_df.loc[current_df["source"] == source, "cost"].sum()) for source in ("google", "facebook", "tiktok")}
        previous_source_cost = {source: float(previous_df.loc[previous_df["source"] == source, "cost"].sum()) for source in ("google", "facebook", "tiktok")}
        current_metrics = {"total_ad_cost": round(sum(current_source_cost.values()), 2), "google_ad_cost": round(current_source_cost["google"], 2), "facebook_ad_cost": round(current_source_cost["facebook"], 2), "tiktok_ad_cost": round(current_source_cost["tiktok"], 2)}
        previous_metrics = {"total_ad_cost": round(sum(previous_source_cost.values()), 2), "google_ad_cost": round(previous_source_cost["google"], 2), "facebook_ad_cost": round(previous_source_cost["facebook"], 2), "tiktok_ad_cost": round(previous_source_cost["tiktok"], 2)}
        growth = {metric: self._growth_percentage(float(current_metrics[metric]), float(previous_metrics[metric])) for metric in current_metrics.keys()}
        return {"current_period": {"from_date": from_date.isoformat(), "to_date": to_date.isoformat(), "metrics": current_metrics}, "previous_period": {"from_date": previous_from.isoformat(), "to_date": previous_to.isoformat(), "metrics": previous_metrics}, "growth_percentage": growth}

    async def cost_breakdown_charts(self, from_date: date, to_date: date) -> dict[str, object]:
        cost_df = await self._frame_for_range(from_date, to_date)
        if cost_df.empty:
            return {
                "cost_by_campaign_type": await self._pie_payload("Cost by Campaign Type", labels=[], values=[]),
                "ua_cost_by_platform": await self._pie_payload("User Acquisition Cost by Platform", labels=[], values=[]),
                "ba_cost_by_platform": await self._pie_payload("Brand Awareness Cost by Platform", labels=[], values=[]),
            }
        by_type = cost_df.groupby("campaign_type", as_index=False)["cost"].sum().sort_values("cost", ascending=False)
        cost_by_type = await self._pie_payload("Cost by Campaign Type", labels=[str(value).replace("_", " ").title() for value in by_type["campaign_type"].tolist()], values=[float(value) for value in by_type["cost"].tolist()])
        ua_df = cost_df.loc[cost_df["campaign_type"] == "user_acquisition"].copy()
        ua_by_platform = ua_df.groupby("source", as_index=False)["cost"].sum().sort_values("cost", ascending=False) if not ua_df.empty else pd.DataFrame(columns=["source", "cost"])
        ua_cost = await self._pie_payload("User Acquisition Cost by Platform", labels=[str(value).title() for value in ua_by_platform["source"].tolist()], values=[float(value) for value in ua_by_platform["cost"].tolist()])
        ba_df = cost_df.loc[cost_df["campaign_type"] == "brand_awareness"].copy()
        ba_by_platform = ba_df.groupby("source", as_index=False)["cost"].sum().sort_values("cost", ascending=False) if not ba_df.empty else pd.DataFrame(columns=["source", "cost"])
        ba_cost = await self._pie_payload("Brand Awareness Cost by Platform", labels=[str(value).title() for value in ba_by_platform["source"].tolist()], values=[float(value) for value in ba_by_platform["cost"].tolist()])
        return {"cost_by_campaign_type": cost_by_type, "ua_cost_by_platform": ua_cost, "ba_cost_by_platform": ba_cost}
