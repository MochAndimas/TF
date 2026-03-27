"""GA4 active-user overview service."""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from sqlalchemy import func, literal, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Ga4DailyMetrics


class OverviewData:
    def __init__(self, session: AsyncSession, from_date: date, to_date: date, source: str) -> None:
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.source = source.strip().lower()
        self.df_ga4 = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: date, to_date: date, source: str = "app"):
        instance = cls(session=session, from_date=from_date, to_date=to_date, source=source)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        self.df_ga4 = await self._read_ga4_db_with_range(from_date=self.from_date, to_date=self.to_date, source=self.source)

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

    async def _read_ga4_db_with_range(self, from_date: date, to_date: date, source: str) -> pd.DataFrame:
        normalized_source = source.strip().lower()
        if normalized_source == "app_web":
            query = (
                select(
                    Ga4DailyMetrics.date.label("date"),
                    literal("app_web").label("source"),
                    func.sum(Ga4DailyMetrics.daily_active_users).label("daily_active_users"),
                    func.sum(Ga4DailyMetrics.monthly_active_users).label("monthly_active_users"),
                    func.sum(Ga4DailyMetrics.active_users).label("active_users"),
                )
                .where(Ga4DailyMetrics.date.between(from_date, to_date), Ga4DailyMetrics.source.in_(["app", "web"]))
                .group_by(Ga4DailyMetrics.date)
                .order_by(Ga4DailyMetrics.date.asc())
            )
        else:
            query = (
                select(
                    Ga4DailyMetrics.date.label("date"),
                    Ga4DailyMetrics.source.label("source"),
                    Ga4DailyMetrics.daily_active_users.label("daily_active_users"),
                    Ga4DailyMetrics.monthly_active_users.label("monthly_active_users"),
                    Ga4DailyMetrics.active_users.label("active_users"),
                )
                .where(Ga4DailyMetrics.date.between(from_date, to_date), Ga4DailyMetrics.source == normalized_source)
                .order_by(Ga4DailyMetrics.date.asc())
            )
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "source", "daily_active_users", "monthly_active_users", "active_users"])
        dataframe = pd.DataFrame(rows)
        dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date
        return dataframe

    async def _frame_for_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        if from_date >= self.from_date and to_date <= self.to_date and not self.df_ga4.empty:
            return self.df_ga4.loc[(self.df_ga4["date"] >= from_date) & (self.df_ga4["date"] <= to_date)].copy()
        return await self._read_ga4_db_with_range(from_date=from_date, to_date=to_date, source=self.source)

    @staticmethod
    def _build_daily_series(dataframe: pd.DataFrame, from_date: date, to_date: date) -> pd.DataFrame:
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
            merged = date_frame.merge(working[["date", "daily_active_users", "monthly_active_users", "active_users"]], on="date", how="left")
            merged["daily_active_users"] = merged["daily_active_users"].fillna(0).astype(int)
            merged["monthly_active_users"] = merged["monthly_active_users"].fillna(0).astype(int)
            merged["active_users"] = merged["active_users"].fillna(0).astype(int)
        merged["stickiness"] = merged.apply(lambda row: round((float(row["daily_active_users"]) / float(row["monthly_active_users"])) * 100, 2) if float(row["monthly_active_users"]) else 0.0, axis=1)
        return merged

    async def stickiness_with_growth(self, from_date: date, to_date: date) -> dict[str, object]:
        current_raw = await self._frame_for_range(from_date, to_date)
        current_df = self._build_daily_series(current_raw, from_date, to_date)
        previous_from, previous_to = self._previous_period_range(from_date, to_date)
        previous_raw = await self._read_ga4_db_with_range(from_date=previous_from, to_date=previous_to, source=self.source)
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
                current_last = round((float(latest_current.get("daily_active_users", 0) or 0) / monthly_current) * 100, 2)

        if not previous_raw.empty:
            previous_working = previous_raw.copy()
            for column in ("daily_active_users", "monthly_active_users", "active_users"):
                previous_working[column] = pd.to_numeric(previous_working[column], errors="coerce").fillna(0)
            latest_previous = previous_working.sort_values("date").iloc[-1]
            previous_active_user = float(previous_working["active_users"].sum())
            monthly_previous = float(latest_previous.get("monthly_active_users", 0) or 0)
            if monthly_previous:
                previous_last = round((float(latest_previous.get("daily_active_users", 0) or 0) / monthly_previous) * 100, 2)

        current_last = round(current_last, 2)
        previous_last = round(previous_last, 2)
        current_avg = round(current_avg, 2)
        previous_avg = round(previous_avg, 2)
        current_active_user = int(current_active_user)
        previous_active_user = int(previous_active_user)

        return {
            "current_period": {"from_date": from_date.isoformat(), "to_date": to_date.isoformat(), "metrics": {"last_day_stickiness": current_last, "average_stickiness": current_avg, "active_user": current_active_user}},
            "previous_period": {"from_date": previous_from.isoformat(), "to_date": previous_to.isoformat(), "metrics": {"last_day_stickiness": previous_last, "average_stickiness": previous_avg, "active_user": previous_active_user}},
            "growth_percentage": {
                "last_day_stickiness": self._growth_percentage(current_last, previous_last),
                "average_stickiness": self._growth_percentage(current_avg, previous_avg),
                "active_user": self._growth_percentage(float(current_active_user), float(previous_active_user)),
            },
        }

    async def active_users_chart(self, from_date: date, to_date: date) -> dict[str, object]:
        daily = self._build_daily_series(await self._frame_for_range(from_date, to_date), from_date, to_date)
        source_label = "APP + WEB" if self.source == "app_web" else self.source.upper()
        figure = go.Figure()
        figure.add_trace(go.Scatter(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=pd.to_numeric(daily["daily_active_users"], errors="coerce").fillna(0).tolist(), mode="lines+markers", name="1 Day Active User", line=dict(color="#6176ff", width=2), marker=dict(size=7), hovertemplate="<b>%{x}</b><br>1 Day Active User: %{y:,}<extra></extra>"))
        figure.add_trace(go.Scatter(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=pd.to_numeric(daily["monthly_active_users"], errors="coerce").fillna(0).tolist(), mode="lines+markers", name="28 Day Active User", yaxis="y2", line=dict(color="#ff6248", width=2), marker=dict(size=7), hovertemplate="<b>%{x}</b><br>28 Day Active User: %{y:,}<extra></extra>"))
        figure.update_layout(title=f"FireBase Active User {source_label}", xaxis_title="Date", yaxis=dict(title="Active Users"), yaxis2=dict(title="Active Users", overlaying="y", side="right"), xaxis=dict(type="category"), legend=dict(orientation="v", y=0.92, x=1.02), margin=dict(l=48, r=48, t=60, b=20))
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = await asyncio.to_thread(lambda: [{"date": row["date"].isoformat(), "daily_active_users": int(row["daily_active_users"]), "monthly_active_users": int(row["monthly_active_users"]), "active_users": int(row["active_users"]), "stickiness": float(row["stickiness"])} for _, row in daily.iterrows()])
        return {"source": self.source, "from_date": from_date.isoformat(), "to_date": to_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}
