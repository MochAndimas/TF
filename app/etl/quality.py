"""Data quality checks for ETL pipeline inputs."""

from __future__ import annotations

import pandas as pd


def _duplicate_ratio(df: pd.DataFrame, keys: list[str]) -> float:
    if df.empty:
        return 0.0
    duplicate_count = int(df.duplicated(subset=keys, keep=False).sum())
    return duplicate_count / len(df)


def validate_depo_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed deposit data before load."""
    if df.empty:
        return

    missing_key = df[["id", "tgl_regis", "campaignid"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(f"DQ failed: data_depo has {int(missing_key)} rows with missing business keys.")

    invalid_dates = ((df["tgl_regis"].isna()) | (df["tgl_regis"] < pd.Timestamp("2022-01-01").date())).sum()
    if invalid_dates:
        raise ValueError(f"DQ failed: data_depo has {int(invalid_dates)} rows with invalid registration dates.")

    negative_depo = (pd.to_numeric(df["First Depo $"], errors="coerce").fillna(0) < 0).sum()
    if negative_depo:
        raise ValueError(f"DQ failed: data_depo has {int(negative_depo)} rows with negative first deposit.")

    dup_ratio = _duplicate_ratio(df, ["id", "tgl_regis", "campaignid"])
    if dup_ratio > 0:
        raise ValueError(f"DQ failed: data_depo duplicate key ratio {dup_ratio:.2%}.")


def validate_ads_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed ads data before load."""
    if df.empty:
        return

    missing_key = df[["date", "campaign_id", "ad_group", "ad_name"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(f"DQ failed: ads data has {int(missing_key)} rows with missing business keys.")

    invalid_dates = ((df["date"].isna()) | (df["date"] < pd.Timestamp("2022-01-01").date())).sum()
    if invalid_dates:
        raise ValueError(f"DQ failed: ads data has {int(invalid_dates)} rows with invalid metric dates.")

    negative_metric = (
        (pd.to_numeric(df["cost"], errors="coerce").fillna(0) < 0)
        | (pd.to_numeric(df["impressions"], errors="coerce").fillna(0) < 0)
        | (pd.to_numeric(df["clicks"], errors="coerce").fillna(0) < 0)
        | (pd.to_numeric(df["leads"], errors="coerce").fillna(0) < 0)
    ).sum()
    if negative_metric:
        raise ValueError(f"DQ failed: ads data has {int(negative_metric)} rows with negative metrics.")

    dup_ratio = _duplicate_ratio(df, ["date", "campaign_id", "ad_group", "ad_name"])
    if dup_ratio > 0:
        raise ValueError(f"DQ failed: ads duplicate key ratio {dup_ratio:.2%}.")

