"""Business-math allocation helpers for campaign analytics."""

from __future__ import annotations

import pandas as pd


class CampaignLeadAllocator:
    """Allocate aggregate lead metrics into row-level campaign datasets."""

    @staticmethod
    def attach_activity_leads(df: pd.DataFrame, activity_df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df["leads"] = 0.0
            return df
        if activity_df.empty:
            df["leads"] = 0.0
            return df

        merged = df.merge(activity_df, on=["date", "campaign_id"], how="left")
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

    @staticmethod
    def growth_percentage(current_value: float, previous_value: float) -> float | None:
        if previous_value == 0:
            if current_value == 0:
                return 0.0
            return 100.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)
