"""Overview user-acquisition service."""

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
from app.utils.overview.shared import USD_TO_IDR_RATE


class OverviewLeadsAcquisitionData:
    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_ads = pd.DataFrame()
        self.df_revenue = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: date, to_date: date):
        instance = cls(session=session, from_date=from_date, to_date=to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        self.df_ads = await self._read_ads_ua_with_range(self.from_date, self.to_date)
        self.df_revenue = await self._read_revenue_ua_with_range(self.from_date, self.to_date)

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

    async def _read_one_source_ads_ua(self, model, source_key: str, from_date: date, to_date: date) -> pd.DataFrame:
        query = (
            select(
                model.date.label("date"),
                model.campaign_id.label("campaign_id"),
                func.sum(model.cost).label("cost"),
                func.sum(model.impressions).label("impressions"),
                func.sum(model.clicks).label("clicks"),
            )
            .join(model.campaign)
            .where(func.date(model.date).between(from_date, to_date), Campaign.ad_type == "user_acquisition")
            .group_by(model.date, model.campaign_id)
            .order_by(model.date.asc())
        )
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "leads", "source"])
        dataframe = pd.DataFrame(rows)
        dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date
        dataframe = await self._attach_register_leads(dataframe, from_date, to_date)
        grouped = dataframe.groupby("date", as_index=False)[["cost", "impressions", "clicks", "leads"]].sum()
        for column in ("cost", "impressions", "clicks", "leads"):
            grouped[column] = pd.to_numeric(grouped[column], errors="coerce").fillna(0)
        grouped["source"] = source_key
        return grouped

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
        dataframe = pd.DataFrame(rows)
        dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date
        dataframe["campaign_id"] = dataframe["campaign_id"].astype(str)
        dataframe["leads"] = pd.to_numeric(dataframe["leads"], errors="coerce").fillna(0)
        return dataframe

    async def _attach_register_leads(self, dataframe: pd.DataFrame, from_date: date, to_date: date) -> pd.DataFrame:
        regis_df = await self._read_daily_register_db(from_date=from_date, to_date=to_date)
        if regis_df.empty:
            dataframe["leads"] = 0.0
            return dataframe
        dataframe["campaign_id"] = dataframe["campaign_id"].astype(str)
        dataframe = dataframe.merge(regis_df, on=["date", "campaign_id"], how="left")
        dataframe["leads"] = pd.to_numeric(dataframe["leads"], errors="coerce").fillna(0)
        for column in ("cost", "impressions", "clicks"):
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").fillna(0)
        return dataframe

    async def _read_ads_ua_with_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        frames = [
            await self._read_one_source_ads_ua(GoogleAds, "google", from_date, to_date),
            await self._read_one_source_ads_ua(FacebookAds, "facebook", from_date, to_date),
            await self._read_one_source_ads_ua(TikTokAds, "tiktok", from_date, to_date),
        ]
        non_empty_frames = [frame for frame in frames if not frame.empty]
        if not non_empty_frames:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "leads", "source"])
        dataframe = pd.concat(non_empty_frames, ignore_index=True)
        if dataframe.empty:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "leads", "source"])
        return dataframe

    async def _read_revenue_ua_with_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        query = (
            select(DataDepo.tanggal_regis.label("date"), func.sum(DataDepo.first_depo).label("revenue"))
            .join(DataDepo.campaign)
            .where(DataDepo.tanggal_regis.between(from_date, to_date), Campaign.ad_type == "user_acquisition", DataDepo.first_depo.is_not(None), DataDepo.first_depo > 0)
            .group_by(DataDepo.tanggal_regis)
            .order_by(DataDepo.tanggal_regis.asc())
        )
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "revenue"])
        dataframe = pd.DataFrame(rows)
        dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date
        dataframe["revenue"] = pd.to_numeric(dataframe["revenue"], errors="coerce").fillna(0.0)
        return dataframe

    async def _ads_for_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_ads.empty:
            return self.df_ads.loc[(self.df_ads["date"] >= from_date) & (self.df_ads["date"] <= to_date)].copy()
        return await self._read_ads_ua_with_range(from_date, to_date)

    async def _revenue_for_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_revenue.empty:
            return self.df_revenue.loc[(self.df_revenue["date"] >= from_date) & (self.df_revenue["date"] <= to_date)].copy()
        return await self._read_revenue_ua_with_range(from_date, to_date)

    @staticmethod
    def _daily_totals_frame(dataframe: pd.DataFrame, from_date: date, to_date: date) -> pd.DataFrame:
        timeline = pd.DataFrame({"date": pd.date_range(start=from_date, end=to_date, freq="D").date})
        if dataframe.empty:
            merged = timeline.copy()
            merged["cost"] = 0.0
            merged["impressions"] = 0
            merged["clicks"] = 0
            merged["leads"] = 0
            return merged
        grouped = dataframe.groupby("date", as_index=False)[["cost", "impressions", "clicks", "leads"]].sum().sort_values("date")
        merged = timeline.merge(grouped, on="date", how="left")
        merged["cost"] = pd.to_numeric(merged.get("cost", 0), errors="coerce").fillna(0.0)
        merged["impressions"] = pd.to_numeric(merged.get("impressions", 0), errors="coerce").fillna(0).astype(int)
        merged["clicks"] = pd.to_numeric(merged.get("clicks", 0), errors="coerce").fillna(0).astype(int)
        merged["leads"] = pd.to_numeric(merged.get("leads", 0), errors="coerce").fillna(0).astype(int)
        return merged

    async def metrics_with_growth(self, from_date: date, to_date: date) -> dict[str, object]:
        current_ads = await self._ads_for_range(from_date, to_date)
        current_daily = self._daily_totals_frame(current_ads, from_date, to_date)
        current_revenue_df = await self._revenue_for_range(from_date, to_date)
        current_first_deposit_idr = float(pd.to_numeric(current_revenue_df.get("revenue", 0), errors="coerce").fillna(0).sum()) * float(USD_TO_IDR_RATE)
        previous_from, previous_to = self._previous_period_range(from_date, to_date)
        previous_ads = await self._read_ads_ua_with_range(previous_from, previous_to)
        previous_daily = self._daily_totals_frame(previous_ads, previous_from, previous_to)
        previous_revenue_df = await self._read_revenue_ua_with_range(previous_from, previous_to)
        previous_first_deposit_idr = float(pd.to_numeric(previous_revenue_df.get("revenue", 0), errors="coerce").fillna(0).sum()) * float(USD_TO_IDR_RATE)
        current_cost = float(current_daily["cost"].sum())
        current_impressions = int(current_daily["impressions"].sum())
        current_clicks = int(current_daily["clicks"].sum())
        current_leads = int(current_daily["leads"].sum())
        previous_cost = float(previous_daily["cost"].sum())
        previous_impressions = int(previous_daily["impressions"].sum())
        previous_clicks = int(previous_daily["clicks"].sum())
        previous_leads = int(previous_daily["leads"].sum())
        current_metrics = {"cost": round(current_cost, 2), "impressions": current_impressions, "clicks": current_clicks, "leads": current_leads, "cost_leads": round(current_cost / current_leads, 2) if current_leads else 0.0, "first_deposit": round(current_first_deposit_idr, 2), "cost_to_first_deposit": round((current_first_deposit_idr / current_cost) * 100, 2) if current_cost else 0.0}
        previous_metrics = {"cost": round(previous_cost, 2), "impressions": previous_impressions, "clicks": previous_clicks, "leads": previous_leads, "cost_leads": round(previous_cost / previous_leads, 2) if previous_leads else 0.0, "first_deposit": round(previous_first_deposit_idr, 2), "cost_to_first_deposit": round((previous_first_deposit_idr / previous_cost) * 100, 2) if previous_cost else 0.0}
        growth = {metric: self._growth_percentage(float(current_metrics[metric]), float(previous_metrics[metric])) for metric in current_metrics.keys()}
        return {"current_period": {"from_date": from_date.isoformat(), "to_date": to_date.isoformat(), "metrics": current_metrics}, "previous_period": {"from_date": previous_from.isoformat(), "to_date": previous_to.isoformat(), "metrics": previous_metrics}, "growth_percentage": growth}

    async def leads_by_source(self, from_date: date, to_date: date) -> dict[str, object]:
        ads_df = await self._ads_for_range(from_date, to_date)
        if ads_df.empty:
            figure = go.Figure()
            figure.update_layout(title="Leads by Source", showlegend=False, annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
            chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
            return {"table_rows": [], "pie_chart": {"rows": [], "figure": json.loads(chart_json)}}
        grouped = ads_df.groupby("source", as_index=False)[["cost", "impressions", "clicks", "leads"]].sum().sort_values("leads", ascending=False)
        total_leads = float(grouped["leads"].sum())
        grouped["cost_per_lead"] = grouped.apply(lambda row: round(float(row["cost"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0, axis=1)
        grouped["leads_share_pct"] = grouped.apply(lambda row: round((float(row["leads"]) / total_leads) * 100, 2) if total_leads else 0.0, axis=1)
        table_rows = [{"source": str(row["source"]).title(), "cost": float(row["cost"]), "impressions": int(row["impressions"]), "clicks": int(row["clicks"]), "leads": int(row["leads"]), "cost_per_lead": float(row["cost_per_lead"]), "leads_share_pct": float(row["leads_share_pct"])} for _, row in grouped.iterrows()]
        labels = [str(value).title() for value in grouped["source"].tolist()]
        values = [float(value) for value in grouped["leads"].tolist()]
        pie_figure = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.38, textinfo="label+percent", hovertemplate="<b>%{label}</b><br>Leads: %{value:,}<extra></extra>")])
        pie_figure.update_layout(title="Leads by Source", showlegend=False, margin=dict(l=24, r=24, t=56, b=24))
        pie_json = await asyncio.to_thread(json.dumps, pie_figure, cls=plotly.utils.PlotlyJSONEncoder)
        return {"table_rows": table_rows, "pie_chart": {"rows": [{"label": label, "leads": float(value)} for label, value in zip(labels, values)], "figure": json.loads(pie_json)}}

    async def cost_vs_leads_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        ads_df = await self._ads_for_range(from_date, to_date)
        daily = self._daily_totals_frame(ads_df, from_date, to_date)
        daily["cost_leads"] = daily.apply(lambda row: round(float(row["cost"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0, axis=1)
        figure = go.Figure()
        figure.add_trace(go.Bar(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=pd.to_numeric(daily["cost"], errors="coerce").fillna(0).tolist(), name="Cost", marker_color="#6176ff", hovertemplate="<b>%{x}</b><br>Cost: Rp. %{y:,.0f}<extra></extra>"))
        figure.add_trace(go.Scatter(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=pd.to_numeric(daily["cost_leads"], errors="coerce").fillna(0).tolist(), mode="lines+markers", name="Cost/Lead", yaxis="y2", line=dict(color="#ff6248", width=2), hovertemplate="<b>%{x}</b><br>Cost/Lead: Rp. %{y:,.0f}<extra></extra>"))
        figure.update_layout(title="Cost per Leads (Cost & Cost/Lead)", xaxis=dict(type="category"), yaxis=dict(title="Cost"), yaxis2=dict(title="Cost/Lead", overlaying="y", side="right"), legend=dict(orientation="h", y=1.1, x=0))
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [{"date": row["date"].isoformat(), "cost": float(row["cost"]), "leads": int(row["leads"]), "cost_leads": float(row["cost_leads"])} for _, row in daily.iterrows()]
        return {"rows": rows, "figure": json.loads(chart_json)}

    async def leads_per_day_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        ads_df = await self._ads_for_range(from_date, to_date)
        daily = self._daily_totals_frame(ads_df, from_date, to_date)
        figure = go.Figure(data=[go.Bar(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=pd.to_numeric(daily["leads"], errors="coerce").fillna(0).tolist(), name="Leads", marker_color="#6176ff", hovertemplate="<b>%{x}</b><br>Leads: %{y:,}<extra></extra>")])
        figure.update_layout(title="Leads per Day", xaxis=dict(type="category"), yaxis=dict(title="Leads"))
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [{"date": row["date"].isoformat(), "leads": int(row["leads"])} for _, row in daily.iterrows()]
        return {"rows": rows, "figure": json.loads(chart_json)}

    async def cost_to_revenue_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        ads_df = await self._ads_for_range(from_date, to_date)
        revenue_df = await self._revenue_for_range(from_date, to_date)
        daily_cost = self._daily_totals_frame(ads_df, from_date, to_date)[["date", "cost"]].copy()
        timeline = pd.DataFrame({"date": pd.date_range(start=from_date, end=to_date, freq="D").date})
        daily_revenue = timeline.merge(revenue_df, on="date", how="left")
        daily_revenue["revenue"] = pd.to_numeric(daily_revenue.get("revenue", 0), errors="coerce").fillna(0.0)
        daily = daily_cost.merge(daily_revenue[["date", "revenue"]], on="date", how="left")
        daily["cost"] = pd.to_numeric(daily.get("cost", 0), errors="coerce").fillna(0.0)
        daily["revenue"] = pd.to_numeric(daily.get("revenue", 0), errors="coerce").fillna(0.0)
        daily["first_deposit_idr"] = daily["revenue"] * float(USD_TO_IDR_RATE)
        daily["cost_to_revenue_pct"] = daily.apply(lambda row: round((float(row["first_deposit_idr"]) / float(row["cost"])) * 100, 2) if float(row["cost"]) else 0.0, axis=1)
        figure = go.Figure()
        date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
        figure.add_trace(go.Bar(x=date_labels, y=pd.to_numeric(daily["cost"], errors="coerce").fillna(0).tolist(), name="Cost", marker_color="#6176ff", yaxis="y", offsetgroup="cost", hovertemplate="<b>%{x}</b><br>Cost: Rp. %{y:,.0f}<extra></extra>"))
        figure.add_trace(go.Bar(x=date_labels, y=pd.to_numeric(daily["first_deposit_idr"], errors="coerce").fillna(0).tolist(), name="Deposit", marker_color="#13c39c", yaxis="y2", offsetgroup="deposit", hovertemplate="<b>%{x}</b><br>Deposit: Rp. %{y:,.0f}<extra></extra>"))
        figure.add_trace(go.Scatter(x=date_labels, y=pd.to_numeric(daily["cost_to_revenue_pct"], errors="coerce").fillna(0).tolist(), mode="lines+markers", name="Cost To Deposit", yaxis="y3", line=dict(color="#ff6248", width=2), hovertemplate="<b>%{x}</b><br>Cost To Deposit: %{y:.2f}%<extra></extra>"))
        figure.update_layout(title="Cost To Deposit Per Hari", barmode="group", xaxis=dict(type="category"), yaxis=dict(title="Cost"), yaxis2=dict(title="Deposit", overlaying="y", side="right", anchor="free", position=0.94, showgrid=False), yaxis3=dict(title="Cost To Deposit", overlaying="y", side="right", anchor="free", position=1, ticksuffix="%", showgrid=False), legend=dict(orientation="h", y=1.12, x=0))
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [{"date": row["date"].isoformat(), "cost": float(row["cost"]), "first_deposit_idr": float(row["first_deposit_idr"]), "cost_to_revenue_pct": float(row["cost_to_revenue_pct"])} for _, row in daily.iterrows()]
        return {"rows": rows, "figure": json.loads(chart_json)}
