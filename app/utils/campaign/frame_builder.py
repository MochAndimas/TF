"""Dataframe shaping helpers for campaign analytics payloads."""

from __future__ import annotations

from datetime import date

import pandas as pd


class CampaignFrameBuilder:
    """Build normalized campaign analytics dataframes using vectorized ops."""

    @staticmethod
    def _ratio(numerator: pd.Series, denominator: pd.Series, *, multiplier: float = 1.0) -> pd.Series:
        safe_denominator = pd.to_numeric(denominator, errors="coerce").fillna(0).replace(0, pd.NA)
        safe_numerator = pd.to_numeric(numerator, errors="coerce").fillna(0)
        return ((safe_numerator / safe_denominator) * multiplier).fillna(0.0).round(2)

    @staticmethod
    def _empty_daily_ads_frame() -> pd.DataFrame:
        return pd.DataFrame(columns=["date", "cost", "clicks", "leads", "cost_leads", "clicks_leads"])

    def ads_daily_frame(
        self,
        *,
        df: pd.DataFrame,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Aggregate one ads dataframe into daily cost/click/leads metrics."""
        if df.empty:
            return self._empty_daily_ads_frame()

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        if filtered.empty:
            return self._empty_daily_ads_frame()

        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        if filtered.empty:
            return self._empty_daily_ads_frame()

        for column in ("cost", "clicks", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        daily = filtered.groupby("date", as_index=False)[["cost", "clicks", "leads"]].sum().sort_values("date")
        daily["cost_leads"] = self._ratio(daily["cost"], daily["leads"])
        daily["clicks_leads"] = self._ratio(daily["clicks"], daily["leads"])
        return daily[["date", "cost", "clicks", "leads", "cost_leads", "clicks_leads"]]

    def ads_performance_frame(self, *, base: pd.DataFrame, dimension: str) -> pd.DataFrame:
        """Aggregate detailed ads rows into one performance table by dimension."""
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

        shaped = base.copy()
        shaped[dimension] = shaped[dimension].fillna("N/A").replace("", "N/A")
        grouped = (
            shaped.groupby(["campaign_source", dimension], as_index=False)[["impressions", "clicks", "spend", "leads"]]
            .sum()
            .sort_values("spend", ascending=False)
            .rename(columns={dimension: "dimension_name"})
        )

        grouped["click_to_leads_pct"] = self._ratio(grouped["leads"], grouped["clicks"], multiplier=100.0)
        grouped["ctr_pct"] = self._ratio(grouped["clicks"], grouped["impressions"], multiplier=100.0)
        grouped["cpc"] = self._ratio(grouped["spend"], grouped["clicks"])
        grouped["cpm"] = self._ratio(grouped["spend"], grouped["impressions"], multiplier=1000.0)
        grouped["cost_leads"] = self._ratio(grouped["spend"], grouped["leads"])
        return grouped[columns]

    def ads_daily_dimension_frame(
        self,
        *,
        df: pd.DataFrame,
        dimension: str,
        from_date: date,
        to_date: date,
        campaign_type: str | None = None,
        include_leads: bool = False,
    ) -> pd.DataFrame:
        """Build daily metrics grouped by one ads dimension."""
        metric_columns = ["cost", "impressions", "clicks"]
        if include_leads:
            metric_columns.append("leads")
        columns = ["date", "dimension_name", *metric_columns]
        if df.empty:
            return pd.DataFrame(columns=columns)

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        if campaign_type is not None:
            typed_filtered = filtered.loc[filtered["campaign_type"] == campaign_type]
            if not typed_filtered.empty:
                filtered = typed_filtered
        if filtered.empty:
            return pd.DataFrame(columns=columns)

        filtered[dimension] = filtered[dimension].fillna("N/A").replace("", "N/A")
        for column in metric_columns:
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        daily = (
            filtered.groupby(["date", dimension], as_index=False)[metric_columns]
            .sum()
            .sort_values(["date", dimension])
            .rename(columns={dimension: "dimension_name"})
        )
        return daily[columns]

    def add_ctr_cpm_cpc_columns(
        self,
        *,
        df: pd.DataFrame,
        cost_column: str = "cost",
        impressions_column: str = "impressions",
        clicks_column: str = "clicks",
    ) -> pd.DataFrame:
        """Add vectorized CTR, CPM, and CPC columns to a dataframe copy."""
        shaped = df.copy()
        shaped["ctr"] = self._ratio(shaped[clicks_column], shaped[impressions_column], multiplier=100.0)
        shaped["cpm"] = self._ratio(shaped[cost_column], shaped[impressions_column], multiplier=1000.0)
        shaped["cpc"] = self._ratio(shaped[cost_column], shaped[clicks_column])
        return shaped
