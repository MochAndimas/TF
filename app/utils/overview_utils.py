"""Core analytics services for Overview dashboard sections.

The classes in this module are responsible for:
- loading raw rows from database models
- normalizing period windows (including previous-period comparisons)
- computing metric summaries and growth percentages
- producing FE-ready Plotly payloads (rows + serialized figures)
"""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

from decouple import config as env
import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Campaign, DataDepo, FacebookAds, Ga4DailyMetrics, GoogleAds, TikTokAds

USD_TO_IDR_RATE = env("USD_TO_IDR_RATE", default=16000.0, cast=float)


class OverviewData:
    """Service object for loading and aggregating GA4 active-user overview data."""

    def __init__(self, session: AsyncSession, from_date: date, to_date: date, source: str) -> None:
        """Initialize overview data container."""
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.source = source.strip().lower()
        self.df_ga4 = pd.DataFrame()

    @classmethod
    async def load_data(
        cls,
        session: AsyncSession,
        from_date: date,
        to_date: date,
        source: str = "app",
    ) -> "OverviewData":
        """Create service instance and preload GA4 rows for active window."""
        instance = cls(session=session, from_date=from_date, to_date=to_date, source=source)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        """Populate in-memory dataframe for initialized date/source range."""
        self.df_ga4 = await self._read_ga4_db_with_range(
            from_date=self.from_date,
            to_date=self.to_date,
            source=self.source,
        )

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        """Compute previous period with identical day span."""
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float:
        """Calculate growth percentage with zero-denominator safeguard."""
        if previous_value == 0:
            return 100.0 if current_value else 0.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    async def _read_ga4_db_with_range(
        self,
        from_date: date,
        to_date: date,
        source: str,
    ) -> pd.DataFrame:
        """Read GA4 daily rows in arbitrary date range and source."""
        query = (
            select(
                Ga4DailyMetrics.date.label("date"),
                Ga4DailyMetrics.source.label("source"),
                Ga4DailyMetrics.daily_active_users.label("daily_active_users"),
                Ga4DailyMetrics.monthly_active_users.label("monthly_active_users"),
                Ga4DailyMetrics.active_users.label("active_users"),
            )
            .where(
                Ga4DailyMetrics.date.between(from_date, to_date),
                Ga4DailyMetrics.source == source,
            )
            .order_by(Ga4DailyMetrics.date.asc())
        )
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(
                columns=[
                    "date",
                    "source",
                    "daily_active_users",
                    "monthly_active_users",
                    "active_users",
                ]
            )

        dataframe = pd.DataFrame(rows)
        dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date
        return dataframe

    async def _frame_for_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        """Get dataframe slice for date range, querying DB when needed."""
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_ga4.empty:
            dataframe = self.df_ga4.loc[
                (self.df_ga4["date"] >= from_date) & (self.df_ga4["date"] <= to_date)
            ].copy()
        else:
            dataframe = await self._read_ga4_db_with_range(
                from_date=from_date,
                to_date=to_date,
                source=self.source,
            )
        return dataframe

    @staticmethod
    def _build_daily_series(dataframe: pd.DataFrame, from_date: date, to_date: date) -> pd.DataFrame:
        """Normalize GA4 dataframe into full daily series with zero-fill."""
        timeline = pd.date_range(start=from_date, end=to_date, freq="D").date
        date_frame = pd.DataFrame({"date": timeline})

        if dataframe.empty:
            merged = date_frame.copy()
            merged["daily_active_users"] = 0
            merged["monthly_active_users"] = 0
            merged["active_users"] = 0
        else:
            working = dataframe.copy()
            for column in ("daily_active_users", "monthly_active_users", "active_users"):
                working[column] = pd.to_numeric(working[column], errors="coerce").fillna(0).astype(int)
            merged = date_frame.merge(
                working[["date", "daily_active_users", "monthly_active_users", "active_users"]],
                on="date",
                how="left",
            )
            merged["daily_active_users"] = merged["daily_active_users"].fillna(0).astype(int)
            merged["monthly_active_users"] = merged["monthly_active_users"].fillna(0).astype(int)
            merged["active_users"] = merged["active_users"].fillna(0).astype(int)

        merged["stickiness"] = merged.apply(
            lambda row: round((float(row["daily_active_users"]) / float(row["monthly_active_users"])) * 100, 2)
            if float(row["monthly_active_users"])
            else 0.0,
            axis=1,
        )
        return merged

    async def stickiness_with_growth(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build stickiness summary payload with growth against previous period."""
        current_raw = await self._frame_for_range(from_date, to_date)
        current_df = self._build_daily_series(current_raw, from_date, to_date)

        previous_from, previous_to = self._previous_period_range(from_date, to_date)
        previous_raw = await self._read_ga4_db_with_range(
            from_date=previous_from,
            to_date=previous_to,
            source=self.source,
        )
        previous_df = self._build_daily_series(previous_raw, previous_from, previous_to)

        current_last = float(current_df.iloc[-1]["stickiness"]) if not current_df.empty else 0.0
        previous_last = float(previous_df.iloc[-1]["stickiness"]) if not previous_df.empty else 0.0
        current_avg = float(current_df["stickiness"].mean()) if not current_df.empty else 0.0
        previous_avg = float(previous_df["stickiness"].mean()) if not previous_df.empty else 0.0
        current_active_user = float(current_df["active_users"].sum()) if not current_df.empty else 0.0
        previous_active_user = float(previous_df["active_users"].sum()) if not previous_df.empty else 0.0

        if not current_raw.empty:
            current_working = current_raw.copy()
            for column in ("daily_active_users", "monthly_active_users", "active_users"):
                current_working[column] = pd.to_numeric(current_working[column], errors="coerce").fillna(0)
            latest_current = current_working.sort_values("date").iloc[-1]
            current_active_user = float(current_working["active_users"].sum())
            monthly_current = float(latest_current.get("monthly_active_users", 0) or 0)
            if monthly_current:
                current_last = round(
                    (float(latest_current.get("daily_active_users", 0) or 0) / monthly_current) * 100,
                    2,
                )

        if not previous_raw.empty:
            previous_working = previous_raw.copy()
            for column in ("daily_active_users", "monthly_active_users", "active_users"):
                previous_working[column] = pd.to_numeric(previous_working[column], errors="coerce").fillna(0)
            latest_previous = previous_working.sort_values("date").iloc[-1]
            previous_active_user = float(previous_working["active_users"].sum())
            monthly_previous = float(latest_previous.get("monthly_active_users", 0) or 0)
            if monthly_previous:
                previous_last = round(
                    (float(latest_previous.get("daily_active_users", 0) or 0) / monthly_previous) * 100,
                    2,
                )

        current_last = round(current_last, 2)
        previous_last = round(previous_last, 2)
        current_avg = round(current_avg, 2)
        previous_avg = round(previous_avg, 2)
        current_active_user = int(current_active_user)
        previous_active_user = int(previous_active_user)

        return {
            "current_period": {
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "metrics": {
                    "last_day_stickiness": current_last,
                    "average_stickiness": current_avg,
                    "active_user": current_active_user,
                },
            },
            "previous_period": {
                "from_date": previous_from.isoformat(),
                "to_date": previous_to.isoformat(),
                "metrics": {
                    "last_day_stickiness": previous_last,
                    "average_stickiness": previous_avg,
                    "active_user": previous_active_user,
                },
            },
            "growth_percentage": {
                "last_day_stickiness": self._growth_percentage(current_last, previous_last),
                "average_stickiness": self._growth_percentage(current_avg, previous_avg),
                "active_user": self._growth_percentage(
                    float(current_active_user),
                    float(previous_active_user),
                ),
            },
        }

    async def active_users_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build dual-axis active-users chart payload for selected source."""
        daily = self._build_daily_series(await self._frame_for_range(from_date, to_date), from_date, to_date)
        source_label = self.source.upper()
        date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
        dau_values = pd.to_numeric(daily["daily_active_users"], errors="coerce").fillna(0).tolist()
        mau_values = pd.to_numeric(daily["monthly_active_users"], errors="coerce").fillna(0).tolist()

        figure = go.Figure()
        figure.add_trace(
            go.Scatter(
                x=date_labels,
                y=dau_values,
                mode="lines+markers",
                name="1 Day Active User",
                line=dict(color="#6176ff", width=2),
                marker=dict(size=7),
                hovertemplate="<b>%{x}</b><br>1 Day Active User: %{y:,}<extra></extra>",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=date_labels,
                y=mau_values,
                mode="lines+markers",
                name="28 Day Active User",
                yaxis="y2",
                line=dict(color="#ff6248", width=2),
                marker=dict(size=7),
                hovertemplate="<b>%{x}</b><br>28 Day Active User: %{y:,}<extra></extra>",
            )
        )
        figure.update_layout(
            title=f"FireBase Active User {source_label}",
            xaxis_title="Date",
            yaxis=dict(title="Active Users"),
            yaxis2=dict(title="Active Users", overlaying="y", side="right"),
            xaxis=dict(type="category"),
            legend=dict(orientation="v", y=0.92, x=1.02),
            margin=dict(l=48, r=48, t=60, b=20),
        )

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        rows = await asyncio.to_thread(
            lambda: [
                {
                    "date": row["date"].isoformat(),
                    "daily_active_users": int(row["daily_active_users"]),
                    "monthly_active_users": int(row["monthly_active_users"]),
                    "active_users": int(row["active_users"]),
                    "stickiness": float(row["stickiness"]),
                }
                for _, row in daily.iterrows()
            ]
        )
        return {
            "source": self.source,
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "rows": rows,
            "figure": json.loads(chart_json),
        }


class OverviewCampaignCostData:
    """Service object for campaign cost summary and pie-chart breakdowns."""

    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        """Initialize overview campaign cost container."""
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_cost = pd.DataFrame()

    @classmethod
    async def load_data(
        cls,
        session: AsyncSession,
        from_date: date,
        to_date: date,
    ) -> "OverviewCampaignCostData":
        """Create service instance and preload ads cost rows for active window."""
        instance = cls(session=session, from_date=from_date, to_date=to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        """Populate in-memory dataframe for initialized date range."""
        self.df_cost = await self._read_ads_cost_db_with_range(
            from_date=self.from_date,
            to_date=self.to_date,
        )

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        """Compute previous period with identical day span."""
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float:
        """Calculate growth percentage with zero-denominator safeguard."""
        if previous_value == 0:
            return 100.0 if current_value else 0.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    async def _read_one_source_cost(
        self,
        model,
        source_key: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Read cost rows from one ads source joined with campaign type metadata."""
        query = (
            select(
                model.date.label("date"),
                Campaign.ad_type.label("campaign_type"),
                func.sum(model.cost).label("cost"),
            )
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
        """Read all ads source cost rows in arbitrary date range."""
        google = await self._read_one_source_cost(GoogleAds, "google", from_date, to_date)
        facebook = await self._read_one_source_cost(FacebookAds, "facebook", from_date, to_date)
        tiktok = await self._read_one_source_cost(TikTokAds, "tiktok", from_date, to_date)
        non_empty_frames = [frame for frame in (google, facebook, tiktok) if not frame.empty]
        if not non_empty_frames:
            return pd.DataFrame(columns=["date", "campaign_type", "cost", "source"])
        dataframe = pd.concat(non_empty_frames, ignore_index=True)
        if dataframe.empty:
            return pd.DataFrame(columns=["date", "campaign_type", "cost", "source"])
        return dataframe

    async def _frame_for_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        """Get cost dataframe for range, querying DB when requested window differs."""
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_cost.empty:
            return self.df_cost.loc[(self.df_cost["date"] >= from_date) & (self.df_cost["date"] <= to_date)].copy()
        return await self._read_ads_cost_db_with_range(from_date=from_date, to_date=to_date)

    @staticmethod
    async def _pie_payload(title: str, labels: list[str], values: list[float]) -> dict[str, object]:
        """Build pie chart payload with empty fallback annotation."""
        if not labels or not values or not any(float(v) > 0 for v in values):
            figure = go.Figure()
            figure.update_layout(
                title=title,
                showlegend=False,
                margin=dict(l=24, r=24, t=56, b=24),
                annotations=[
                    {
                        "text": "No data available",
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0.5,
                        "y": 0.5,
                        "showarrow": False,
                    }
                ],
            )
            rows = []
        else:
            figure = go.Figure(
                data=[
                    go.Pie(
                        labels=labels,
                        values=values,
                        hole=0.38,
                        textinfo="label+percent",
                        hovertemplate="<b>%{label}</b><br>Cost: Rp. %{value:,.0f}<extra></extra>",
                    )
                ]
            )
            figure.update_layout(
                title=title,
                showlegend=False,
                margin=dict(l=24, r=24, t=56, b=24),
            )
            rows = [
                {"label": label, "cost": float(value)}
                for label, value in zip(labels, values)
            ]

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {"rows": rows, "figure": json.loads(chart_json)}

    async def cost_metrics_with_growth(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build cost metrics payload and growth versus previous period."""
        current_df = await self._frame_for_range(from_date, to_date)
        previous_from, previous_to = self._previous_period_range(from_date, to_date)
        previous_df = await self._read_ads_cost_db_with_range(previous_from, previous_to)

        current_source_cost = {
            source: float(current_df.loc[current_df["source"] == source, "cost"].sum())
            for source in ("google", "facebook", "tiktok")
        }
        previous_source_cost = {
            source: float(previous_df.loc[previous_df["source"] == source, "cost"].sum())
            for source in ("google", "facebook", "tiktok")
        }
        current_total = sum(current_source_cost.values())
        previous_total = sum(previous_source_cost.values())

        current_metrics = {
            "total_ad_cost": round(current_total, 2),
            "google_ad_cost": round(current_source_cost["google"], 2),
            "facebook_ad_cost": round(current_source_cost["facebook"], 2),
            "tiktok_ad_cost": round(current_source_cost["tiktok"], 2),
        }
        previous_metrics = {
            "total_ad_cost": round(previous_total, 2),
            "google_ad_cost": round(previous_source_cost["google"], 2),
            "facebook_ad_cost": round(previous_source_cost["facebook"], 2),
            "tiktok_ad_cost": round(previous_source_cost["tiktok"], 2),
        }
        growth = {
            metric: self._growth_percentage(
                float(current_metrics[metric]),
                float(previous_metrics[metric]),
            )
            for metric in current_metrics.keys()
        }

        return {
            "current_period": {
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "metrics": current_metrics,
            },
            "previous_period": {
                "from_date": previous_from.isoformat(),
                "to_date": previous_to.isoformat(),
                "metrics": previous_metrics,
            },
            "growth_percentage": growth,
        }

    async def cost_breakdown_charts(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build 3 pie charts: by campaign type, UA by platform, BA by platform."""
        cost_df = await self._frame_for_range(from_date, to_date)
        if cost_df.empty:
            empty_cost_by_type = await self._pie_payload(
                "Cost by Campaign Type",
                labels=[],
                values=[],
            )
            empty_ua = await self._pie_payload(
                "User Acquisition Cost by Platform",
                labels=[],
                values=[],
            )
            empty_ba = await self._pie_payload(
                "Brand Awareness Cost by Platform",
                labels=[],
                values=[],
            )
            return {
                "cost_by_campaign_type": empty_cost_by_type,
                "ua_cost_by_platform": empty_ua,
                "ba_cost_by_platform": empty_ba,
            }

        by_type = (
            cost_df.groupby("campaign_type", as_index=False)["cost"]
            .sum()
            .sort_values("cost", ascending=False)
        )
        by_type_labels = [
            str(value).replace("_", " ").title()
            for value in by_type["campaign_type"].tolist()
        ]
        by_type_values = [float(value) for value in by_type["cost"].tolist()]
        cost_by_type = await self._pie_payload(
            "Cost by Campaign Type",
            labels=by_type_labels,
            values=by_type_values,
        )

        ua_df = cost_df.loc[cost_df["campaign_type"] == "user_acquisition"].copy()
        ua_by_platform = (
            ua_df.groupby("source", as_index=False)["cost"]
            .sum()
            .sort_values("cost", ascending=False)
        ) if not ua_df.empty else pd.DataFrame(columns=["source", "cost"])
        ua_labels = [str(value).title() for value in ua_by_platform["source"].tolist()]
        ua_values = [float(value) for value in ua_by_platform["cost"].tolist()]
        ua_cost = await self._pie_payload(
            "User Acquisition Cost by Platform",
            labels=ua_labels,
            values=ua_values,
        )

        ba_df = cost_df.loc[cost_df["campaign_type"] == "brand_awareness"].copy()
        ba_by_platform = (
            ba_df.groupby("source", as_index=False)["cost"]
            .sum()
            .sort_values("cost", ascending=False)
        ) if not ba_df.empty else pd.DataFrame(columns=["source", "cost"])
        ba_labels = [str(value).title() for value in ba_by_platform["source"].tolist()]
        ba_values = [float(value) for value in ba_by_platform["cost"].tolist()]
        ba_cost = await self._pie_payload(
            "Brand Awareness Cost by Platform",
            labels=ba_labels,
            values=ba_values,
        )

        return {
            "cost_by_campaign_type": cost_by_type,
            "ua_cost_by_platform": ua_cost,
            "ba_cost_by_platform": ba_cost,
        }


class OverviewLeadsAcquisitionData:
    """Service object for UA leads-acquisition metrics, tables, and charts."""

    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        """Initialize UA overview service state and in-memory caches."""
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_ads = pd.DataFrame()
        self.df_revenue = pd.DataFrame()

    @classmethod
    async def load_data(
        cls,
        session: AsyncSession,
        from_date: date,
        to_date: date,
    ) -> "OverviewLeadsAcquisitionData":
        """Instantiate service and preload UA ads + first-deposit datasets."""
        instance = cls(session=session, from_date=from_date, to_date=to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        """Populate in-memory dataframes for configured initialization window."""
        self.df_ads = await self._read_ads_ua_with_range(self.from_date, self.to_date)
        self.df_revenue = await self._read_revenue_ua_with_range(self.from_date, self.to_date)

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        """Return previous date window with the same number of days."""
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float:
        """Compute percentage growth with zero-baseline safeguard."""
        if previous_value == 0:
            return 100.0 if current_value else 0.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    async def _read_one_source_ads_ua(
        self,
        model,
        source_key: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Load one platform's UA daily aggregates from ads tables.

        Returns:
            pd.DataFrame: Columns ``date,cost,impressions,clicks,leads,source``.
        """
        query = (
            select(
                model.date.label("date"),
                func.sum(model.cost).label("cost"),
                func.sum(model.impressions).label("impressions"),
                func.sum(model.clicks).label("clicks"),
                func.sum(model.leads).label("leads"),
            )
            .join(model.campaign)
            .where(
                func.date(model.date).between(from_date, to_date),
                Campaign.ad_type == "user_acquisition",
            )
            .group_by(model.date)
            .order_by(model.date.asc())
        )
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "leads", "source"])

        dataframe = pd.DataFrame(rows)
        dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date
        for column in ("cost", "impressions", "clicks", "leads"):
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").fillna(0)
        dataframe["source"] = source_key
        return dataframe

    async def _read_ads_ua_with_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        """Load and merge UA ads data across Google/Facebook/TikTok."""
        google = await self._read_one_source_ads_ua(GoogleAds, "google", from_date, to_date)
        facebook = await self._read_one_source_ads_ua(FacebookAds, "facebook", from_date, to_date)
        tiktok = await self._read_one_source_ads_ua(TikTokAds, "tiktok", from_date, to_date)
        non_empty_frames = [frame for frame in (google, facebook, tiktok) if not frame.empty]
        if not non_empty_frames:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "leads", "source"])
        dataframe = pd.concat(non_empty_frames, ignore_index=True)
        if dataframe.empty:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "leads", "source"])
        return dataframe

    async def _read_revenue_ua_with_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        """Load first-deposit totals (stored as ``revenue``) for UA campaigns."""
        query = (
            select(
                DataDepo.tanggal_regis.label("date"),
                func.sum(DataDepo.first_depo).label("revenue"),
            )
            .join(DataDepo.campaign)
            .where(
                DataDepo.tanggal_regis.between(from_date, to_date),
                Campaign.ad_type == "user_acquisition",
                DataDepo.first_depo.is_not(None),
                DataDepo.first_depo > 0,
            )
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
        """Return UA ads data from cache or DB for requested range."""
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_ads.empty:
            return self.df_ads.loc[(self.df_ads["date"] >= from_date) & (self.df_ads["date"] <= to_date)].copy()
        return await self._read_ads_ua_with_range(from_date, to_date)

    async def _revenue_for_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        """Return UA first-deposit data from cache or DB for requested range."""
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_revenue.empty:
            return self.df_revenue.loc[
                (self.df_revenue["date"] >= from_date) & (self.df_revenue["date"] <= to_date)
            ].copy()
        return await self._read_revenue_ua_with_range(from_date, to_date)

    @staticmethod
    def _daily_totals_frame(dataframe: pd.DataFrame, from_date: date, to_date: date) -> pd.DataFrame:
        """Build full-day timeline with zero-filled UA numeric columns."""
        timeline = pd.DataFrame({"date": pd.date_range(start=from_date, end=to_date, freq="D").date})
        if dataframe.empty:
            merged = timeline.copy()
            merged["cost"] = 0.0
            merged["impressions"] = 0
            merged["clicks"] = 0
            merged["leads"] = 0
            return merged
        grouped = (
            dataframe.groupby("date", as_index=False)[["cost", "impressions", "clicks", "leads"]]
            .sum()
            .sort_values("date")
        )
        merged = timeline.merge(grouped, on="date", how="left")
        merged["cost"] = pd.to_numeric(merged.get("cost", 0), errors="coerce").fillna(0.0)
        merged["impressions"] = pd.to_numeric(merged.get("impressions", 0), errors="coerce").fillna(0).astype(int)
        merged["clicks"] = pd.to_numeric(merged.get("clicks", 0), errors="coerce").fillna(0).astype(int)
        merged["leads"] = pd.to_numeric(merged.get("leads", 0), errors="coerce").fillna(0).astype(int)
        return merged

    async def metrics_with_growth(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build UA KPI summary for current/previous period and growth deltas.

        Metrics include:
        - cost
        - impressions
        - clicks
        - leads
        - cost_leads
        - first_deposit (converted to IDR)
        - cost_to_first_deposit (ratio %)
        """
        current_ads = await self._ads_for_range(from_date, to_date)
        current_daily = self._daily_totals_frame(current_ads, from_date, to_date)
        current_revenue_df = await self._revenue_for_range(from_date, to_date)
        current_first_deposit = float(pd.to_numeric(current_revenue_df.get("revenue", 0), errors="coerce").fillna(0).sum())
        current_first_deposit_idr = current_first_deposit * float(USD_TO_IDR_RATE)

        previous_from, previous_to = self._previous_period_range(from_date, to_date)
        previous_ads = await self._read_ads_ua_with_range(previous_from, previous_to)
        previous_daily = self._daily_totals_frame(previous_ads, previous_from, previous_to)
        previous_revenue_df = await self._read_revenue_ua_with_range(previous_from, previous_to)
        previous_first_deposit = float(pd.to_numeric(previous_revenue_df.get("revenue", 0), errors="coerce").fillna(0).sum())
        previous_first_deposit_idr = previous_first_deposit * float(USD_TO_IDR_RATE)

        current_cost = float(current_daily["cost"].sum())
        current_impressions = int(current_daily["impressions"].sum())
        current_clicks = int(current_daily["clicks"].sum())
        current_leads = int(current_daily["leads"].sum())
        current_cost_leads = round(current_cost / current_leads, 2) if current_leads else 0.0
        current_cost_to_first_deposit = (
            round((current_first_deposit_idr / current_cost) * 100, 2) if current_cost else 0.0
        )

        previous_cost = float(previous_daily["cost"].sum())
        previous_impressions = int(previous_daily["impressions"].sum())
        previous_clicks = int(previous_daily["clicks"].sum())
        previous_leads = int(previous_daily["leads"].sum())
        previous_cost_leads = round(previous_cost / previous_leads, 2) if previous_leads else 0.0
        previous_cost_to_first_deposit = (
            round((previous_first_deposit_idr / previous_cost) * 100, 2) if previous_cost else 0.0
        )

        current_metrics = {
            "cost": round(current_cost, 2),
            "impressions": current_impressions,
            "clicks": current_clicks,
            "leads": current_leads,
            "cost_leads": current_cost_leads,
            "first_deposit": round(current_first_deposit_idr, 2),
            "cost_to_first_deposit": current_cost_to_first_deposit,
        }
        previous_metrics = {
            "cost": round(previous_cost, 2),
            "impressions": previous_impressions,
            "clicks": previous_clicks,
            "leads": previous_leads,
            "cost_leads": previous_cost_leads,
            "first_deposit": round(previous_first_deposit_idr, 2),
            "cost_to_first_deposit": previous_cost_to_first_deposit,
        }
        growth = {
            metric: self._growth_percentage(float(current_metrics[metric]), float(previous_metrics[metric]))
            for metric in current_metrics.keys()
        }
        return {
            "current_period": {
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "metrics": current_metrics,
            },
            "previous_period": {
                "from_date": previous_from.isoformat(),
                "to_date": previous_to.isoformat(),
                "metrics": previous_metrics,
            },
            "growth_percentage": growth,
        }

    async def leads_by_source(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build leads-by-source table rows and donut chart payload."""
        ads_df = await self._ads_for_range(from_date, to_date)
        if ads_df.empty:
            figure = go.Figure()
            figure.update_layout(
                title="Leads by Source",
                showlegend=False,
                annotations=[
                    {
                        "text": "No data available",
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0.5,
                        "y": 0.5,
                        "showarrow": False,
                    }
                ],
            )
            chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
            return {"table_rows": [], "pie_chart": {"rows": [], "figure": json.loads(chart_json)}}

        grouped = (
            ads_df.groupby("source", as_index=False)[["cost", "impressions", "clicks", "leads"]]
            .sum()
            .sort_values("leads", ascending=False)
        )
        total_leads = float(grouped["leads"].sum())
        grouped["cost_per_lead"] = grouped.apply(
            lambda row: round(float(row["cost"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0,
            axis=1,
        )
        grouped["leads_share_pct"] = grouped.apply(
            lambda row: round((float(row["leads"]) / total_leads) * 100, 2) if total_leads else 0.0,
            axis=1,
        )
        table_rows = [
            {
                "source": str(row["source"]).title(),
                "cost": float(row["cost"]),
                "impressions": int(row["impressions"]),
                "clicks": int(row["clicks"]),
                "leads": int(row["leads"]),
                "cost_per_lead": float(row["cost_per_lead"]),
                "leads_share_pct": float(row["leads_share_pct"]),
            }
            for _, row in grouped.iterrows()
        ]

        labels = [str(value).title() for value in grouped["source"].tolist()]
        values = [float(value) for value in grouped["leads"].tolist()]
        pie_figure = go.Figure(
            data=[
                go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.38,
                    textinfo="label+percent",
                    hovertemplate="<b>%{label}</b><br>Leads: %{value:,}<extra></extra>",
                )
            ]
        )
        pie_figure.update_layout(
            title="Leads by Source",
            showlegend=False,
            margin=dict(l=24, r=24, t=56, b=24),
        )
        pie_json = await asyncio.to_thread(json.dumps, pie_figure, cls=plotly.utils.PlotlyJSONEncoder)
        pie_rows = [{"label": label, "leads": float(value)} for label, value in zip(labels, values)]
        return {"table_rows": table_rows, "pie_chart": {"rows": pie_rows, "figure": json.loads(pie_json)}}

    async def cost_vs_leads_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build combined chart payload: daily cost bars + leads line."""
        ads_df = await self._ads_for_range(from_date, to_date)
        daily = self._daily_totals_frame(ads_df, from_date, to_date)
        date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
        cost_values = pd.to_numeric(daily["cost"], errors="coerce").fillna(0).tolist()
        leads_values = pd.to_numeric(daily["leads"], errors="coerce").fillna(0).tolist()

        figure = go.Figure()
        figure.add_trace(
            go.Bar(
                x=date_labels,
                y=cost_values,
                name="Cost",
                marker_color="#6176ff",
                hovertemplate="<b>%{x}</b><br>Cost: Rp. %{y:,.0f}<extra></extra>",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=date_labels,
                y=leads_values,
                mode="lines+markers",
                name="Leads",
                yaxis="y2",
                line=dict(color="#ff6248", width=2),
                hovertemplate="<b>%{x}</b><br>Leads: %{y:,}<extra></extra>",
            )
        )
        figure.update_layout(
            title="Cost per Leads (Cost & Leads)",
            xaxis=dict(type="category"),
            yaxis=dict(title="Cost"),
            yaxis2=dict(title="Leads", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1, x=0),
        )
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [
            {"date": row["date"].isoformat(), "cost": float(row["cost"]), "leads": int(row["leads"])}
            for _, row in daily.iterrows()
        ]
        return {"rows": rows, "figure": json.loads(chart_json)}

    async def leads_per_day_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build daily leads bar-chart payload for UA section."""
        ads_df = await self._ads_for_range(from_date, to_date)
        daily = self._daily_totals_frame(ads_df, from_date, to_date)
        date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
        leads_values = pd.to_numeric(daily["leads"], errors="coerce").fillna(0).tolist()

        figure = go.Figure(
            data=[
                go.Bar(
                    x=date_labels,
                    y=leads_values,
                    name="Leads",
                    marker_color="#6176ff",
                    hovertemplate="<b>%{x}</b><br>Leads: %{y:,}<extra></extra>",
                )
            ]
        )
        figure.update_layout(
            title="Leads per Day",
            xaxis=dict(type="category"),
            yaxis=dict(title="Leads"),
        )
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [{"date": row["date"].isoformat(), "leads": int(row["leads"])} for _, row in daily.iterrows()]
        return {"rows": rows, "figure": json.loads(chart_json)}

    async def cost_to_revenue_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build stacked cost/first-deposit chart plus ratio line.

        Note:
            ``DataDepo.first_depo`` is treated as USD source data and converted to
            IDR using ``USD_TO_IDR_RATE`` before ratio and chart rendering.
        """
        ads_df = await self._ads_for_range(from_date, to_date)
        revenue_df = await self._revenue_for_range(from_date, to_date)
        daily_cost = self._daily_totals_frame(ads_df, from_date, to_date)[["date", "cost"]].copy()
        timeline = pd.DataFrame({"date": pd.date_range(start=from_date, end=to_date, freq="D").date})
        daily_revenue = timeline.merge(revenue_df, on="date", how="left")
        daily_revenue["revenue"] = pd.to_numeric(daily_revenue.get("revenue", 0), errors="coerce").fillna(0.0)
        daily = daily_cost.merge(daily_revenue[["date", "revenue"]], on="date", how="left")
        daily["cost"] = pd.to_numeric(daily.get("cost", 0), errors="coerce").fillna(0.0)
        daily["revenue"] = pd.to_numeric(daily.get("revenue", 0), errors="coerce").fillna(0.0)
        # `first_depo` source is USD; convert to IDR before cost-vs-deposit comparison.
        daily["first_deposit_idr"] = daily["revenue"] * float(USD_TO_IDR_RATE)
        daily["cost_to_revenue_pct"] = daily.apply(
            lambda row: round((float(row["first_deposit_idr"]) / float(row["cost"])) * 100, 2)
            if float(row["cost"])
            else 0.0,
            axis=1,
        )

        date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
        cost_values = pd.to_numeric(daily["cost"], errors="coerce").fillna(0).tolist()
        revenue_values = pd.to_numeric(daily["first_deposit_idr"], errors="coerce").fillna(0).tolist()
        ratio_values = pd.to_numeric(daily["cost_to_revenue_pct"], errors="coerce").fillna(0).tolist()

        figure = go.Figure()
        figure.add_trace(
            go.Bar(
                x=date_labels,
                y=cost_values,
                name="Cost",
                marker_color="#6176ff",
                hovertemplate="<b>%{x}</b><br>Cost: Rp. %{y:,.0f}<extra></extra>",
            )
        )
        figure.add_trace(
            go.Bar(
                x=date_labels,
                y=revenue_values,
                name="First Deposit",
                marker_color="#13c39c",
                hovertemplate="<b>%{x}</b><br>First Deposit: Rp. %{y:,.0f}<extra></extra>",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=date_labels,
                y=ratio_values,
                mode="lines+markers",
                name="Cost To First Deposit",
                yaxis="y2",
                line=dict(color="#ff6248", width=2),
                hovertemplate="<b>%{x}</b><br>Cost To First Deposit: %{y:.2f}%<extra></extra>",
            )
        )
        figure.update_layout(
            title="Cost To First Deposit Per Hari",
            barmode="stack",
            xaxis=dict(type="category"),
            yaxis=dict(title="Cost / First Deposit"),
            yaxis2=dict(title="Cost To First Deposit", overlaying="y", side="right", ticksuffix="%"),
            legend=dict(orientation="h", y=1.12, x=0),
        )
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [
            {
                "date": row["date"].isoformat(),
                "cost": float(row["cost"]),
                "first_deposit_idr": float(row["first_deposit_idr"]),
                "cost_to_revenue_pct": float(row["cost_to_revenue_pct"]),
            }
            for _, row in daily.iterrows()
        ]
        return {"rows": rows, "figure": json.loads(chart_json)}


class OverviewBrandAwarenessData:
    """Service object for BA metrics with growth and BA spend/performance charts."""

    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        """Initialize BA overview service state and in-memory cache."""
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_ads = pd.DataFrame()

    @classmethod
    async def load_data(
        cls,
        session: AsyncSession,
        from_date: date,
        to_date: date,
    ) -> "OverviewBrandAwarenessData":
        """Instantiate service and preload BA ads aggregates for initial range."""
        instance = cls(session=session, from_date=from_date, to_date=to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        """Populate in-memory BA ads dataframe for initialized date window."""
        self.df_ads = await self._read_ads_ba_with_range(self.from_date, self.to_date)

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        """Return previous date window with matching duration."""
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float:
        """Compute percentage growth with zero-baseline safeguard."""
        if previous_value == 0:
            return 100.0 if current_value else 0.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    async def _read_one_source_ads_ba(
        self,
        model,
        source_key: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Load one platform's BA daily aggregates from ads tables."""
        query = (
            select(
                model.date.label("date"),
                func.sum(model.cost).label("cost"),
                func.sum(model.impressions).label("impressions"),
                func.sum(model.clicks).label("clicks"),
            )
            .join(model.campaign)
            .where(
                func.date(model.date).between(from_date, to_date),
                Campaign.ad_type == "brand_awareness",
            )
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
        """Load and merge BA ads data across Google/Facebook/TikTok."""
        google = await self._read_one_source_ads_ba(GoogleAds, "google", from_date, to_date)
        facebook = await self._read_one_source_ads_ba(FacebookAds, "facebook", from_date, to_date)
        tiktok = await self._read_one_source_ads_ba(TikTokAds, "tiktok", from_date, to_date)
        non_empty_frames = [frame for frame in (google, facebook, tiktok) if not frame.empty]
        if not non_empty_frames:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "source"])
        dataframe = pd.concat(non_empty_frames, ignore_index=True)
        if dataframe.empty:
            return pd.DataFrame(columns=["date", "cost", "impressions", "clicks", "source"])
        return dataframe

    async def _ads_for_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        """Return BA ads data from cache or DB for requested range."""
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_ads.empty:
            return self.df_ads.loc[(self.df_ads["date"] >= from_date) & (self.df_ads["date"] <= to_date)].copy()
        return await self._read_ads_ba_with_range(from_date, to_date)

    @staticmethod
    def _daily_totals_frame(dataframe: pd.DataFrame, from_date: date, to_date: date) -> pd.DataFrame:
        """Build full-day timeline with zero-filled BA numeric columns."""
        timeline = pd.DataFrame({"date": pd.date_range(start=from_date, end=to_date, freq="D").date})
        if dataframe.empty:
            merged = timeline.copy()
            merged["cost"] = 0.0
            merged["impressions"] = 0
            merged["clicks"] = 0
            return merged
        grouped = (
            dataframe.groupby("date", as_index=False)[["cost", "impressions", "clicks"]]
            .sum()
            .sort_values("date")
        )
        merged = timeline.merge(grouped, on="date", how="left")
        merged["cost"] = pd.to_numeric(merged.get("cost", 0), errors="coerce").fillna(0.0)
        merged["impressions"] = pd.to_numeric(merged.get("impressions", 0), errors="coerce").fillna(0).astype(int)
        merged["clicks"] = pd.to_numeric(merged.get("clicks", 0), errors="coerce").fillna(0).astype(int)
        return merged

    async def metrics_with_growth(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build BA KPI summary for current/previous period and growth deltas."""
        current_ads = await self._ads_for_range(from_date, to_date)
        current_daily = self._daily_totals_frame(current_ads, from_date, to_date)

        previous_from, previous_to = self._previous_period_range(from_date, to_date)
        previous_ads = await self._read_ads_ba_with_range(previous_from, previous_to)
        previous_daily = self._daily_totals_frame(previous_ads, previous_from, previous_to)

        current_cost = float(current_daily["cost"].sum())
        current_impressions = int(current_daily["impressions"].sum())
        current_clicks = int(current_daily["clicks"].sum())
        current_ctr = round((current_clicks / current_impressions) * 100, 2) if current_impressions else 0.0
        current_cpm = round((current_cost / current_impressions) * 1000, 2) if current_impressions else 0.0
        current_cpc = round(current_cost / current_clicks, 2) if current_clicks else 0.0

        previous_cost = float(previous_daily["cost"].sum())
        previous_impressions = int(previous_daily["impressions"].sum())
        previous_clicks = int(previous_daily["clicks"].sum())
        previous_ctr = round((previous_clicks / previous_impressions) * 100, 2) if previous_impressions else 0.0
        previous_cpm = round((previous_cost / previous_impressions) * 1000, 2) if previous_impressions else 0.0
        previous_cpc = round(previous_cost / previous_clicks, 2) if previous_clicks else 0.0

        current_metrics = {
            "cost": round(current_cost, 2),
            "impressions": current_impressions,
            "clicks": current_clicks,
            "ctr": current_ctr,
            "cpm": current_cpm,
            "cpc": current_cpc,
        }
        previous_metrics = {
            "cost": round(previous_cost, 2),
            "impressions": previous_impressions,
            "clicks": previous_clicks,
            "ctr": previous_ctr,
            "cpm": previous_cpm,
            "cpc": previous_cpc,
        }
        growth = {
            metric: self._growth_percentage(float(current_metrics[metric]), float(previous_metrics[metric]))
            for metric in current_metrics.keys()
        }
        return {
            "current_period": {
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "metrics": current_metrics,
            },
            "previous_period": {
                "from_date": previous_from.isoformat(),
                "to_date": previous_to.isoformat(),
                "metrics": previous_metrics,
            },
            "growth_percentage": growth,
        }

    async def spend_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build BA daily spend bar-chart payload."""
        ads_df = await self._ads_for_range(from_date, to_date)
        daily = self._daily_totals_frame(ads_df, from_date, to_date)
        date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
        cost_values = pd.to_numeric(daily["cost"], errors="coerce").fillna(0).tolist()

        figure = go.Figure(
            data=[
                go.Bar(
                    x=date_labels,
                    y=cost_values,
                    name="Spend",
                    marker_color="#6176ff",
                    hovertemplate="<b>%{x}</b><br>Spend: Rp. %{y:,.0f}<extra></extra>",
                )
            ]
        )
        figure.update_layout(
            title="Brand Awareness Spend",
            xaxis=dict(type="category"),
            yaxis=dict(title="Spend"),
        )
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [{"date": row["date"].isoformat(), "cost": float(row["cost"])} for _, row in daily.iterrows()]
        return {"rows": rows, "figure": json.loads(chart_json)}

    async def performance_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        """Build BA mixed chart payload.

        Chart composition:
        - stacked bars: impressions and clicks
        - lines on secondary axis: CTR, CPM, CPC
        """
        ads_df = await self._ads_for_range(from_date, to_date)
        daily = self._daily_totals_frame(ads_df, from_date, to_date)
        daily["ctr"] = daily.apply(
            lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2)
            if float(row["impressions"])
            else 0.0,
            axis=1,
        )
        daily["cpm"] = daily.apply(
            lambda row: round((float(row["cost"]) / float(row["impressions"])) * 1000, 2)
            if float(row["impressions"])
            else 0.0,
            axis=1,
        )
        daily["cpc"] = daily.apply(
            lambda row: round(float(row["cost"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0,
            axis=1,
        )

        date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
        impression_values = pd.to_numeric(daily["impressions"], errors="coerce").fillna(0).tolist()
        click_values = pd.to_numeric(daily["clicks"], errors="coerce").fillna(0).tolist()
        ctr_values = pd.to_numeric(daily["ctr"], errors="coerce").fillna(0).tolist()
        cpm_values = pd.to_numeric(daily["cpm"], errors="coerce").fillna(0).tolist()
        cpc_values = pd.to_numeric(daily["cpc"], errors="coerce").fillna(0).tolist()

        figure = go.Figure()
        figure.add_trace(
            go.Bar(
                x=date_labels,
                y=impression_values,
                name="Impressions",
                marker_color="#6176ff",
                hovertemplate="<b>%{x}</b><br>Impressions: %{y:,}<extra></extra>",
            )
        )
        figure.add_trace(
            go.Bar(
                x=date_labels,
                y=click_values,
                name="Clicks",
                marker_color="#13c39c",
                hovertemplate="<b>%{x}</b><br>Clicks: %{y:,}<extra></extra>",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=date_labels,
                y=ctr_values,
                mode="lines+markers",
                name="CTR",
                yaxis="y2",
                line=dict(color="#ff6248", width=2),
                hovertemplate="<b>%{x}</b><br>CTR: %{y:.2f}%<extra></extra>",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=date_labels,
                y=cpm_values,
                mode="lines+markers",
                name="CPM",
                yaxis="y2",
                line=dict(color="#ffb547", width=2),
                hovertemplate="<b>%{x}</b><br>CPM: Rp. %{y:,.2f}<extra></extra>",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=date_labels,
                y=cpc_values,
                mode="lines+markers",
                name="CPC",
                yaxis="y2",
                line=dict(color="#b379ff", width=2),
                hovertemplate="<b>%{x}</b><br>CPC: Rp. %{y:,.2f}<extra></extra>",
            )
        )
        figure.update_layout(
            title="Brand Awareness Performance",
            barmode="stack",
            xaxis=dict(type="category"),
            yaxis=dict(title="Impressions / Clicks"),
            yaxis2=dict(title="CTR / CPM / CPC", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.12, x=0),
        )

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = [
            {
                "date": row["date"].isoformat(),
                "cost": float(row["cost"]),
                "impressions": int(row["impressions"]),
                "clicks": int(row["clicks"]),
                "ctr": float(row["ctr"]),
                "cpm": float(row["cpm"]),
                "cpc": float(row["cpc"]),
            }
            for _, row in daily.iterrows()
        ]
        return {"rows": rows, "figure": json.loads(chart_json)}
