"""Overview brand-awareness service."""

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


class OverviewBrandAwarenessData:
    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_ads = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: date, to_date: date):
        instance = cls(session=session, from_date=from_date, to_date=to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        self.df_ads = await self._read_ads_ba_with_range(self.from_date, self.to_date)

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

    async def _read_one_source_ads_ba(self, model, source_key: str, from_date: date, to_date: date) -> pd.DataFrame:
        query = (
            select(model.date.label("date"), func.sum(model.cost).label("cost"), func.sum(model.impressions).label("impressions"), func.sum(model.clicks).label("clicks"))
            .join(model.campaign)
            .where(func.date(model.date).between(from_date, to_date), Campaign.ad_type == "brand_awareness")
            .group_by(model.date)
            .order_by(model.date.asc())
        )
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "source"])
        dataframe = pd.DataFrame(rows)
        dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date
        for column in ("cost", "impressions", "clicks"):
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").fillna(0)
        dataframe["source"] = source_key
        return dataframe

    async def _read_ads_ba_with_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        frames = [
            await self._read_one_source_ads_ba(GoogleAds, "google", from_date, to_date),
            await self._read_one_source_ads_ba(FacebookAds, "facebook", from_date, to_date),
            await self._read_one_source_ads_ba(TikTokAds, "tiktok", from_date, to_date),
        ]
        non_empty_frames = [frame for frame in frames if not frame.empty]
        if not non_empty_frames:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "source"])
        dataframe = pd.concat(non_empty_frames, ignore_index=True)
        if dataframe.empty:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "source"])
        return dataframe

    async def _ads_for_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_ads.empty:
            return self.df_ads.loc[(self.df_ads["date"] >= from_date) & (self.df_ads["date"] <= to_date)].copy()
        return await self._read_ads_ba_with_range(from_date, to_date)

    @staticmethod
    def _daily_totals_frame(dataframe: pd.DataFrame, from_date: date, to_date: date) -> pd.DataFrame:
        timeline = pd.DataFrame({"date": pd.date_range(start=from_date, end=to_date, freq="D").date})
        if dataframe.empty:
            merged = timeline.copy()
            merged["cost"] = 0.0
            merged["impressions"] = 0
            merged["clicks"] = 0
            return merged
        grouped = dataframe.groupby("date", as_index=False)[["cost", "impressions", "clicks"]].sum().sort_values("date")
        merged = timeline.merge(grouped, on="date", how="left")
        merged["cost"] = pd.to_numeric(merged.get("cost", 0), errors="coerce").fillna(0.0)
        merged["impressions"] = pd.to_numeric(merged.get("impressions", 0), errors="coerce").fillna(0).astype(int)
        merged["clicks"] = pd.to_numeric(merged.get("clicks", 0), errors="coerce").fillna(0).astype(int)
        return merged

    async def metrics_with_growth(self, from_date: date, to_date: date) -> dict[str, object]:
        current_ads = await self._ads_for_range(from_date, to_date)
        current_daily = self._daily_totals_frame(current_ads, from_date, to_date)
        previous_from, previous_to = self._previous_period_range(from_date, to_date)
        previous_ads = await self._read_ads_ba_with_range(previous_from, previous_to)
        previous_daily = self._daily_totals_frame(previous_ads, previous_from, previous_to)
        current_cost = float(current_daily["cost"].sum())
        current_impressions = int(current_daily["impressions"].sum())
        current_clicks = int(current_daily["clicks"].sum())
        previous_cost = float(previous_daily["cost"].sum())
        previous_impressions = int(previous_daily["impressions"].sum())
        previous_clicks = int(previous_daily["clicks"].sum())
        current_metrics = {"cost": round(current_cost, 2), "impressions": current_impressions, "clicks": current_clicks, "ctr": round((current_clicks / current_impressions) * 100, 2) if current_impressions else 0.0, "cpm": round((current_cost / current_impressions) * 1000, 2) if current_impressions else 0.0, "cpc": round(current_cost / current_clicks, 2) if current_clicks else 0.0}
        previous_metrics = {"cost": round(previous_cost, 2), "impressions": previous_impressions, "clicks": previous_clicks, "ctr": round((previous_clicks / previous_impressions) * 100, 2) if previous_impressions else 0.0, "cpm": round((previous_cost / previous_impressions) * 1000, 2) if previous_impressions else 0.0, "cpc": round(previous_cost / previous_clicks, 2) if previous_clicks else 0.0}
        growth = {metric: self._growth_percentage(float(current_metrics[metric]), float(previous_metrics[metric])) for metric in current_metrics.keys()}
        return {"current_period": {"from_date": from_date.isoformat(), "to_date": to_date.isoformat(), "metrics": current_metrics}, "previous_period": {"from_date": previous_from.isoformat(), "to_date": previous_to.isoformat(), "metrics": previous_metrics}, "growth_percentage": growth}

    async def spend_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        daily = self._daily_totals_frame(await self._ads_for_range(from_date, to_date), from_date, to_date)
        figure = go.Figure(data=[go.Bar(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=pd.to_numeric(daily["cost"], errors="coerce").fillna(0).tolist(), name="Spend", marker_color="#6176ff", hovertemplate="<b>%{x}</b><br>Spend: Rp. %{y:,.0f}<extra></extra>")])
        figure.update_layout(title="Brand Awareness Spend", xaxis=dict(type="category"), yaxis=dict(title="Spend"))
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [{"date": row["date"].isoformat(), "cost": float(row["cost"])} for _, row in daily.iterrows()]
        return {"rows": rows, "figure": json.loads(chart_json)}

    async def performance_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        daily = self._daily_totals_frame(await self._ads_for_range(from_date, to_date), from_date, to_date)
        daily["ctr"] = daily.apply(lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2) if float(row["impressions"]) else 0.0, axis=1)
        daily["cpm"] = daily.apply(lambda row: round((float(row["cost"]) / float(row["impressions"])) * 1000, 2) if float(row["impressions"]) else 0.0, axis=1)
        daily["cpc"] = daily.apply(lambda row: round(float(row["cost"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0, axis=1)
        date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
        figure = go.Figure()
        figure.add_trace(go.Bar(x=date_labels, y=pd.to_numeric(daily["impressions"], errors="coerce").fillna(0).tolist(), name="Impressions", marker_color="#6176ff", hovertemplate="<b>%{x}</b><br>Impressions: %{y:,}<extra></extra>"))
        figure.add_trace(go.Bar(x=date_labels, y=pd.to_numeric(daily["clicks"], errors="coerce").fillna(0).tolist(), name="Clicks", marker_color="#13c39c", hovertemplate="<b>%{x}</b><br>Clicks: %{y:,}<extra></extra>"))
        figure.add_trace(go.Scatter(x=date_labels, y=pd.to_numeric(daily["ctr"], errors="coerce").fillna(0).tolist(), mode="lines+markers", name="CTR", yaxis="y2", line=dict(color="#ff6248", width=2), hovertemplate="<b>%{x}</b><br>CTR: %{y:.2f}%<extra></extra>"))
        figure.add_trace(go.Scatter(x=date_labels, y=pd.to_numeric(daily["cpm"], errors="coerce").fillna(0).tolist(), mode="lines+markers", name="CPM", yaxis="y2", line=dict(color="#ffb547", width=2), hovertemplate="<b>%{x}</b><br>CPM: Rp. %{y:,.2f}<extra></extra>"))
        figure.add_trace(go.Scatter(x=date_labels, y=pd.to_numeric(daily["cpc"], errors="coerce").fillna(0).tolist(), mode="lines+markers", name="CPC", yaxis="y2", line=dict(color="#b379ff", width=2), hovertemplate="<b>%{x}</b><br>CPC: Rp. %{y:,.0f}<extra></extra>"))
        figure.update_layout(title="Brand Awareness Performance", barmode="stack", xaxis=dict(type="category"), yaxis=dict(title="Impressions / Clicks"), yaxis2=dict(title="CTR / CPM / CPC", overlaying="y", side="right"), legend=dict(orientation="h", y=1.12, x=0))
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [{"date": row["date"].isoformat(), "cost": float(row["cost"]), "impressions": int(row["impressions"]), "clicks": int(row["clicks"]), "ctr": float(row["ctr"]), "cpm": float(row["cpm"]), "cpc": float(row["cpc"])} for _, row in daily.iterrows()]
        return {"rows": rows, "figure": json.loads(chart_json)}
