"""Data quality checks for ETL pipeline inputs."""

from __future__ import annotations

import pandas as pd


def _duplicate_ratio(df: pd.DataFrame, keys: list[str]) -> float:
    """Compute duplicate ratio for a dataframe business-key definition.

    Args:
        df (pd.DataFrame): Dataframe to evaluate.
        keys (list[str]): Column set used as uniqueness key.

    Returns:
        float: Duplicate-row ratio in range ``0.0`` to ``1.0``.
    """
    if df.empty:
        return 0.0
    duplicate_count = int(df.duplicated(subset=keys, keep=False).sum())
    return duplicate_count / len(df)


def validate_ads_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed ads dataframe before load.

    Args:
        df (pd.DataFrame): Parsed and normalized ads dataframe.

    Returns:
        None: Validation-only function.

    Raises:
        ValueError: Raised when dataframe violates key/date/metric quality rules.
    """
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

    # Duplicate business keys are handled in transform stage (dedupe), so
    # ads validation intentionally does not fail on duplicates here.


def validate_ga4_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed GA4 dataframe before load.

    Args:
        df (pd.DataFrame): Parsed and normalized GA4 dataframe.

    Returns:
        None: Validation-only function.

    Raises:
        ValueError: Raised when dataframe violates key/date/source/metric rules.
    """
    if df.empty:
        return

    missing_key = df[["date", "source"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(f"DQ failed: ga4 data has {int(missing_key)} rows with missing business keys.")

    invalid_dates = ((df["date"].isna()) | (df["date"] < pd.Timestamp("2022-01-01").date())).sum()
    if invalid_dates:
        raise ValueError(f"DQ failed: ga4 data has {int(invalid_dates)} rows with invalid metric dates.")

    invalid_source = (~df["source"].isin(["app", "web"])).sum()
    if invalid_source:
        raise ValueError(f"DQ failed: ga4 data has {int(invalid_source)} rows with invalid source.")

    negative_metric = (
        (pd.to_numeric(df["daily_active_users"], errors="coerce").fillna(0) < 0)
        | (pd.to_numeric(df["monthly_active_users"], errors="coerce").fillna(0) < 0)
        | (pd.to_numeric(df["active_users"], errors="coerce").fillna(0) < 0)
    ).sum()
    if negative_metric:
        raise ValueError(f"DQ failed: ga4 data has {int(negative_metric)} rows with negative metrics.")

    dup_ratio = _duplicate_ratio(df, ["date", "source"])
    if dup_ratio > 0:
        raise ValueError(f"DQ failed: ga4 duplicate key ratio {dup_ratio:.2%}.")


def validate_daily_register_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed daily registration data before load."""
    if df.empty:
        return

    missing_key = df[["date", "campaign_id"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(
            f"DQ failed: daily register data has {int(missing_key)} rows with missing business keys."
        )

    invalid_dates = ((df["date"].isna()) | (df["date"] < pd.Timestamp("2022-01-01").date())).sum()
    if invalid_dates:
        raise ValueError(
            f"DQ failed: daily register data has {int(invalid_dates)} rows with invalid dates."
        )

    negative_metric = (pd.to_numeric(df["total_regis"], errors="coerce").fillna(0) < 0).sum()
    if negative_metric:
        raise ValueError(
            f"DQ failed: daily register data has {int(negative_metric)} rows with negative total_regis."
        )

    dup_ratio = _duplicate_ratio(df, ["date", "campaign_id"])
    if dup_ratio > 0:
        raise ValueError(f"DQ failed: daily register duplicate key ratio {dup_ratio:.2%}.")


def validate_first_deposit_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed first-deposit data before the load step.

    The first-deposit pipeline only keeps rows that should contribute to
    deposit analytics, so validation here is intentionally strict:
    - business-key columns must be present,
    - registration date must be parseable and within a sane historical range,
    - ``first_depo`` must be strictly positive.

    Args:
        df (pd.DataFrame): Transformed first-deposit dataframe that has already
            passed extraction and field mapping.

    Returns:
        None: Validation-only function with no mutation side effects.

    Raises:
        ValueError: Raised when key fields, dates, or metric values do not
        satisfy the ETL quality rules.
    """
    if df.empty:
        return

    missing_key = df[["user_id", "tanggal_regis", "campaign_id"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(
            f"DQ failed: first deposit data has {int(missing_key)} rows with missing business keys."
        )

    invalid_dates = (
        (df["tanggal_regis"].isna()) | (df["tanggal_regis"] < pd.Timestamp("2022-01-01").date())
    ).sum()
    if invalid_dates:
        raise ValueError(
            f"DQ failed: first deposit data has {int(invalid_dates)} rows with invalid registration dates."
        )

    invalid_metric = (pd.to_numeric(df["first_depo"], errors="coerce").fillna(0) <= 0).sum()
    if invalid_metric:
        raise ValueError(
            f"DQ failed: first deposit data has {int(invalid_metric)} rows with non-positive first deposit."
        )
