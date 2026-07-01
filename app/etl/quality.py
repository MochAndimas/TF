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


def validate_instagram_insights_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed Instagram insights data before load."""
    if df.empty:
        return

    missing_key = df[["date"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(
            f"DQ failed: Instagram insights data has {int(missing_key)} rows with missing date."
        )

    invalid_dates = ((df["date"].isna()) | (df["date"] < pd.Timestamp("2022-01-01").date())).sum()
    if invalid_dates:
        raise ValueError(
            f"DQ failed: Instagram insights data has {int(invalid_dates)} rows with invalid dates."
        )

    metric_columns = [
        "total_followers",
        "new_followers",
        "unfollowers",
        "total_engagement",
        "likes",
        "comments",
        "shares",
        "saves",
    ]
    negative_metric = (
        df[metric_columns]
        .apply(lambda column: pd.to_numeric(column, errors="coerce").fillna(0) < 0)
        .any(axis=1)
        .sum()
    )
    if negative_metric:
        raise ValueError(
            f"DQ failed: Instagram insights data has {int(negative_metric)} rows with negative metrics."
        )

    dup_ratio = _duplicate_ratio(df, ["date"])
    if dup_ratio > 0:
        raise ValueError(f"DQ failed: Instagram insights duplicate key ratio {dup_ratio:.2%}.")


def validate_youtube_daily_insight_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed YouTube channel metrics before load."""
    if df.empty:
        return

    missing_key = df[["date"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(
            f"DQ failed: YouTube daily insight data has {int(missing_key)} rows with missing date."
        )

    invalid_dates = ((df["date"].isna()) | (df["date"] < pd.Timestamp("2022-01-01").date())).sum()
    if invalid_dates:
        raise ValueError(
            f"DQ failed: YouTube daily insight data has {int(invalid_dates)} rows with invalid dates."
        )

    nonnegative_columns = [
        "views",
        "watch_hours",
        "subscribers_gained",
        "subscribers_lost",
        "likes",
        "comments",
        "shares",
        "average_view_duration",
    ]
    negative_metric = (
        df[nonnegative_columns]
        .apply(lambda column: pd.to_numeric(column, errors="coerce").fillna(0) < 0)
        .any(axis=1)
        .sum()
    )
    if negative_metric:
        raise ValueError(
            f"DQ failed: YouTube daily insight data has {int(negative_metric)} rows with negative metrics."
        )

    invalid_net = (
        df["net_subscribers"]
        != (df["subscribers_gained"] - df["subscribers_lost"])
    ).sum()
    if invalid_net:
        raise ValueError(
            f"DQ failed: YouTube daily insight data has {int(invalid_net)} inconsistent net subscriber rows."
        )

    dup_ratio = _duplicate_ratio(df, ["date"])
    if dup_ratio > 0:
        raise ValueError(
            f"DQ failed: YouTube daily insight duplicate key ratio {dup_ratio:.2%}."
        )


def validate_youtube_media_insight_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed YouTube per-content snapshots before load."""
    if df.empty:
        return

    missing_key = df[["date", "video_id", "published_at"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(
            f"DQ failed: YouTube media insight has {int(missing_key)} rows with missing keys."
        )

    invalid_dates = (df["date"] < pd.Timestamp("2005-01-01").date()).sum()
    if invalid_dates:
        raise ValueError(
            f"DQ failed: YouTube media insight has {int(invalid_dates)} invalid dates."
        )

    metric_columns = [
        "views",
        "watch_hours",
        "average_view_percentage",
        "likes",
        "comments",
        "shares",
        "subscribers_gained",
    ]
    negative_metric = (
        df[metric_columns]
        .apply(lambda column: pd.to_numeric(column, errors="coerce") < 0)
        .any(axis=1)
        .sum()
    )
    if negative_metric:
        raise ValueError(
            f"DQ failed: YouTube media insight has {int(negative_metric)} rows with negative metrics."
        )

    dup_ratio = _duplicate_ratio(df, ["video_id"])
    if dup_ratio > 0:
        raise ValueError(
            f"DQ failed: YouTube media insight duplicate key ratio {dup_ratio:.2%}."
        )


def validate_instagram_media_insights_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed Instagram post/reels media insights before load."""
    if df.empty:
        return

    missing_key = df[["date", "media_id"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(
            f"DQ failed: Instagram media insights data has {int(missing_key)} rows with missing keys."
        )

    invalid_dates = ((df["date"].isna()) | (df["date"] < pd.Timestamp("2022-01-01").date())).sum()
    if invalid_dates:
        raise ValueError(
            f"DQ failed: Instagram media insights data has {int(invalid_dates)} rows with invalid dates."
        )

    invalid_product_type = (~df["media_product_type"].isin(["FEED", "REELS"])).sum()
    if invalid_product_type:
        raise ValueError(
            f"DQ failed: Instagram media insights data has {int(invalid_product_type)} unsupported media types."
        )

    metric_columns = [
        "likes",
        "comments",
        "shares",
        "saves",
        "reach",
        "views",
        "profile_visits",
        "follows",
        "total_engagement",
    ]
    negative_metric = (
        df[metric_columns]
        .apply(lambda column: pd.to_numeric(column, errors="coerce").fillna(0) < 0)
        .any(axis=1)
        .sum()
    )
    if negative_metric:
        raise ValueError(
            f"DQ failed: Instagram media insights data has {int(negative_metric)} rows with negative metrics."
        )

    dup_ratio = _duplicate_ratio(df, ["media_id"])
    if dup_ratio > 0:
        raise ValueError(f"DQ failed: Instagram media insights duplicate media_id ratio {dup_ratio:.2%}.")


def validate_facebook_page_insights_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed Facebook Page daily insights before load."""
    if df.empty:
        return

    missing_key = df[["page_id", "date"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(
            f"DQ failed: Facebook Page insights data has {int(missing_key)} rows with missing keys."
        )

    invalid_dates = ((df["date"].isna()) | (df["date"] < pd.Timestamp("2022-01-01").date())).sum()
    if invalid_dates:
        raise ValueError(
            f"DQ failed: Facebook Page insights data has {int(invalid_dates)} rows with invalid dates."
        )

    metric_columns = [
        "page_fans",
        "page_fan_adds",
        "page_fan_removes",
        "page_impressions",
        "page_impressions_unique",
        "page_impressions_paid",
        "page_impressions_organic_v2",
        "page_post_engagements",
        "reaction_like",
        "reaction_love",
        "reaction_wow",
        "reaction_haha",
        "reaction_sorry",
        "reaction_anger",
        "page_video_views",
        "page_views_total",
    ]
    negative_metric = (
        df[metric_columns]
        .apply(lambda column: pd.to_numeric(column, errors="coerce").fillna(0) < 0)
        .any(axis=1)
        .sum()
    )
    if negative_metric:
        raise ValueError(
            f"DQ failed: Facebook Page insights data has {int(negative_metric)} rows with negative metrics."
        )

    dup_ratio = _duplicate_ratio(df, ["page_id", "date"])
    if dup_ratio > 0:
        raise ValueError(f"DQ failed: Facebook Page insights duplicate key ratio {dup_ratio:.2%}.")


def validate_facebook_page_media_insights_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed Facebook Page post/media insights before load."""
    if df.empty:
        return

    missing_key = df[["page_id", "date", "post_id"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(
            f"DQ failed: Facebook Page media insights data has {int(missing_key)} rows with missing keys."
        )

    invalid_dates = ((df["date"].isna()) | (df["date"] < pd.Timestamp("2022-01-01").date())).sum()
    if invalid_dates:
        raise ValueError(
            f"DQ failed: Facebook Page media insights data has {int(invalid_dates)} rows with invalid dates."
        )

    metric_columns = [
        "likes",
        "comments",
        "shares",
        "reaction_like",
        "reaction_love",
        "reaction_wow",
        "reaction_haha",
        "reaction_sorry",
        "reaction_anger",
        "post_media_view",
        "post_clicks",
        "post_video_views",
        "total_engagement",
    ]
    negative_metric = (
        df[metric_columns]
        .apply(lambda column: pd.to_numeric(column, errors="coerce").fillna(0) < 0)
        .any(axis=1)
        .sum()
    )
    if negative_metric:
        raise ValueError(
            f"DQ failed: Facebook Page media insights data has {int(negative_metric)} rows with negative metrics."
        )

    dup_ratio = _duplicate_ratio(df, ["post_id"])
    if dup_ratio > 0:
        raise ValueError(f"DQ failed: Facebook Page media insights duplicate post_id ratio {dup_ratio:.2%}.")


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


def validate_ms_deposit_dataframe(df: pd.DataFrame) -> None:
    """Validate transformed MS1 deposit/activity data before load."""
    if df.empty:
        return

    missing_key = df[["email", "last_activity", "campaign_id", "tag"]].isna().any(axis=1).sum()
    if missing_key:
        raise ValueError(
            f"DQ failed: MS deposit data has {int(missing_key)} rows with missing business keys."
        )

    invalid_dates = (
        (df["last_activity"].isna()) | (df["last_activity"] < pd.Timestamp("2022-01-01").date())
    ).sum()
    if invalid_dates:
        raise ValueError(
            f"DQ failed: MS deposit data has {int(invalid_dates)} rows with invalid last activity dates."
        )

    invalid_metric = (pd.to_numeric(df["first_depo"], errors="coerce").fillna(0) < 0).sum()
    if invalid_metric:
        raise ValueError(
            f"DQ failed: MS deposit data has {int(invalid_metric)} rows with negative first deposit."
        )
