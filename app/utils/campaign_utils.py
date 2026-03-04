from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Campaign, DataDepo, FacebookAds, GoogleAds, TikTokAds

AdsModel = GoogleAds | FacebookAds | TikTokAds


class CampaignData:
    """Load and normalize campaign/deposit datasets for a date range."""

    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_google = pd.DataFrame()
        self.df_facebook = pd.DataFrame()
        self.df_tiktok = pd.DataFrame()
        self.df_depo = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: date, to_date: date) -> "CampaignData":
        """Create instance and load all supported sources."""
        instance = cls(session, from_date, to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        """Fetch all source tables and populate DataFrame attributes."""
        self.df_google = await self._read_ads_db(GoogleAds)
        self.df_facebook = await self._read_ads_db(FacebookAds)
        self.df_tiktok = await self._read_ads_db(TikTokAds)
        self.df_depo = await self._read_depo_db()

    async def _read_ads_db(self, model: type[AdsModel]) -> pd.DataFrame:
        """Read aggregated ad metrics for one ad platform."""
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
                func.sum(model.leads).label("leads"),
            )
            .join(model.campaign)
            .filter(func.date(model.date).between(self.from_date, self.to_date))
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
        return df

    async def _read_depo_db(self) -> pd.DataFrame:
        """Read deposit records joined with campaign metadata."""
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
        """Return placeholder row when ad table has no data in range."""
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
        """Return placeholder row when deposit table has no data in range."""
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
        """Map ads source keys to their loaded DataFrames."""
        return {
            "google": self.df_google,
            "facebook": self.df_facebook,
            "tiktok": self.df_tiktok,
        }

    async def ads_metrics(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, float]:
        """Calculate aggregated ad metrics for one source and date window."""
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
        if df.empty:
            return {
                "impressions": 0,
                "clicks": 0,
                "cost": 0.0,
                "leads": 0,
                "cost_leads": 0.0,
            }

        filtered = df.loc[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
        if filtered.empty:
            return {
                "impressions": 0,
                "clicks": 0,
                "cost": 0.0,
                "leads": 0,
                "cost_leads": 0.0,
            }

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

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        """Build previous range with identical duration to current range."""
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float | None:
        """Calculate growth percentage against previous value."""
        if previous_value == 0:
            return None
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    async def ads_metrics_with_growth(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Return current metrics, previous-period metrics, and growth percentages."""
        current_from = from_date or self.from_date
        current_to = to_date or self.to_date
        if current_from > current_to:
            raise ValueError("from_date cannot be after to_date.")

        previous_from, previous_to = self._previous_period_range(current_from, current_to)

        current_metrics = await self.ads_metrics(data=data, from_date=current_from, to_date=current_to)
        previous_metrics = await self.ads_metrics(data=data, from_date=previous_from, to_date=previous_to)

        growth = {
            metric: self._growth_percentage(
                current_value=float(current_metrics[metric]),
                previous_value=float(previous_metrics[metric]),
            )
            for metric in ("impressions", "clicks", "cost", "leads", "cost_leads")
        }

        return {
            "current_period": {
                "from_date": current_from.isoformat(),
                "to_date": current_to.isoformat(),
                "metrics": current_metrics,
            },
            "previous_period": {
                "from_date": previous_from.isoformat(),
                "to_date": previous_to.isoformat(),
                "metrics": previous_metrics,
            },
            "growth_percentage": growth,
        }
