"""Serialization and default-frame helpers for campaign analytics."""

from __future__ import annotations

from datetime import date

import pandas as pd


class CampaignSerializer:
    """Keep dataframe defaulting and serialization concerns in one place."""

    @staticmethod
    def empty_ads_frame(to_date: date) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": [to_date],
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

    @staticmethod
    def empty_depo_frame(to_date: date) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "tanggal_regis": [to_date],
                "campaign_id": ["No data"],
                "campaign_name": ["No data"],
                "campaign_source": ["No data"],
                "campaign_type": ["No data"],
                "user_status": ["No data"],
                "email": ["No data"],
                "first_depo": [0.0],
            }
        )

    @staticmethod
    def serialize_daily_rows(daily: pd.DataFrame) -> list[dict[str, object]]:
        if daily.empty:
            return []
        serializable = daily.copy()
        serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
        return serializable.to_dict(orient="records")

    @staticmethod
    def normalize_ads_metrics_payload(payload: dict[str, object]) -> dict[str, float]:
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
