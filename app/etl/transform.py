"""Transformation utilities for external API pipelines."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from fastapi import HTTPException


def normalize_date(value) -> date:
    """Convert date-like input into ``datetime.date``.

    Args:
        value: Date-like value from request payload (`date`, `datetime`, or
            parseable string).

    Returns:
        date: Normalized date value.

    Raises:
        ValueError: Raised when value is ``None``.
        pandas.errors.ParserError: Raised when string date parsing fails.
    """
    if value is None:
        raise ValueError("Date value is required.")
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        return value
    return pd.to_datetime(value, errors="raise").date()


def resolve_date_window(types: str, start_date, end_date) -> tuple[date, date]:
    """Resolve effective ETL date window based on update mode.

    Args:
        types (str): Update mode (`auto` or `manual`).
        start_date: User-provided manual start date.
        end_date: User-provided manual end date.

    Returns:
        tuple[date, date]: Inclusive target date window used by ETL pipeline.

    Raises:
        fastapi.HTTPException: Raised when mode is invalid or date window is invalid.
    """
    if types not in {"auto", "manual"}:
        raise HTTPException(400, "Invalid update type. Use 'auto' or 'manual'.")

    if types == "auto":
        yesterday = datetime.now().date() - timedelta(1)
        return yesterday, yesterday

    target_start = normalize_date(start_date)
    target_end = normalize_date(end_date)
    if target_start > target_end:
        raise HTTPException(400, "start_date cannot be greater than end_date.")
    return target_start, target_end


def normalize_columns(columns: list[str]) -> list[str]:
    """Normalize source header names into machine-friendly keys.

    Args:
        columns (list[str]): Raw header labels from external source payload.

    Returns:
        list[str]: Normalized lower-case headers with spaces/newlines replaced
        by underscore separators.
    """
    normalized = []
    for column in columns:
        normalized.append(str(column).strip().lower().replace("\n", " ").replace(" ", "_"))
    return normalized


def parse_ads_dataframe(raw_rows: list) -> pd.DataFrame:
    """Parse ads payload into normalized ads dataframe.

    Args:
        raw_rows (list): Raw values rows where first row is header and the rest
            are campaign daily metric rows.

    Returns:
        pd.DataFrame: Normalized ads dataframe with required business-key and
        metric columns in numeric/date dtypes.

    Raises:
        ValueError: Raised when required headers are missing.
    """
    if not raw_rows:
        return pd.DataFrame()

    if isinstance(raw_rows[0], dict):
        df = pd.DataFrame(raw_rows)
        df.columns = normalize_columns(list(df.columns))
    else:
        if len(raw_rows) < 2:
            return pd.DataFrame()
        headers = normalize_columns(raw_rows[0])
        df = pd.DataFrame(raw_rows[1:], columns=headers)
    required_columns = [
        "date",
        "campaign_id",
        "campaign_name",
        "ad_group",
        "ad_name",
        "cost",
        "impressions",
        "clicks",
        "leads",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in ads sheet: {missing_columns}")

    null_markers = {"null", "none", "nan", ""}
    object_columns = df.select_dtypes(include=["object"]).columns
    for column in object_columns:
        normalized = df[column].astype(str).str.strip().str.lower()
        df.loc[normalized.isin(null_markers), column] = None

    df["campaign_id"] = df["campaign_id"].where(df["campaign_id"].notna(), "-")
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0)
    df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0)
    df["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0)
    df["leads"] = pd.to_numeric(df["leads"], errors="coerce").fillna(0)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["campaign_id"] = df["campaign_id"].astype(str)
    df["campaign_name"] = df["campaign_name"].astype(str)
    df["ad_group"] = df["ad_group"].astype(str)
    df["ad_name"] = df["ad_name"].astype(str)
    df["cost"] = df["cost"].astype(float)
    df["impressions"] = df["impressions"].astype(int)
    df["clicks"] = df["clicks"].astype(int)
    df["leads"] = df["leads"].astype(int)
    df = df[df["date"].notna()]
    return df


def dedupe_ads_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop duplicate ads business keys without metric aggregation.

    Keeps the last row for each business key:
    ``date, campaign_id, ad_group, ad_name``.
    """
    if df.empty:
        return df, 0

    deduped = df.drop_duplicates(
        subset=["date", "campaign_id", "ad_group", "ad_name"],
        keep="last",
    ).copy()
    dropped_count = len(df) - len(deduped)
    return deduped, dropped_count


def parse_ga4_dataframe(raw_rows: list[dict]) -> pd.DataFrame:
    """Parse GA4 report rows into normalized daily dataframe.

    Args:
        raw_rows (list[dict]): Raw rows returned by GA4 runReport extractor.

    Returns:
        pd.DataFrame: Aggregated dataframe at ``date + source`` grain with
        metric columns ``daily_active_users``, ``monthly_active_users``,
        and ``active_users``.

    Raises:
        ValueError: Raised when expected GA4 fields are missing.
    """
    if not raw_rows:
        return pd.DataFrame()

    df = pd.DataFrame(raw_rows)
    required_columns = [
        "date",
        "platform",
        "daily_active_users",
        "monthly_active_users",
        "active_users",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in GA4 payload: {missing_columns}")

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").dt.date
    df["platform"] = df["platform"].astype(str).str.strip().str.upper()
    df["source"] = np.where(df["platform"] == "WEB", "web", "app")
    df["daily_active_users"] = pd.to_numeric(df["daily_active_users"], errors="coerce").fillna(0).astype(int)
    df["monthly_active_users"] = pd.to_numeric(df["monthly_active_users"], errors="coerce").fillna(0).astype(int)
    df["active_users"] = pd.to_numeric(df["active_users"], errors="coerce").fillna(0).astype(int)
    df = df[df["date"].notna()]
    df = (
        df.groupby(["date", "source"], as_index=False)[
            ["daily_active_users", "monthly_active_users", "active_users"]
        ]
        .sum()
        .sort_values(["date", "source"])
    )
    return df


def parse_daily_register_dataframe(raw_rows: list) -> pd.DataFrame:
    """Parse raw registration rows into daily campaign registration totals."""
    if not raw_rows:
        return pd.DataFrame()
    if len(raw_rows) < 2:
        return pd.DataFrame()

    headers = normalize_columns(raw_rows[0])
    df = pd.DataFrame(raw_rows[1:], columns=headers)
    date_column = "tanggal_regis" if "tanggal_regis" in df.columns else "tgl_regis"
    campaign_column = "campaign_id" if "campaign_id" in df.columns else "campaignid"
    required_columns = [date_column, campaign_column, "id", "tag"]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in daily register sheet: {missing_columns}")

    df = df[df["tag"].fillna("").astype(str).str.contains("CP1", case=False, na=False)].copy()
    if df.empty:
        return pd.DataFrame(columns=["date", "campaign_id", "total_regis"])

    parsed = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_column], errors="coerce").dt.date,
            "campaign_id": df[campaign_column].fillna("").astype(str).str.strip(),
            "id": df["id"].fillna("").astype(str).str.strip(),
        }
    )
    parsed["campaign_id"] = parsed["campaign_id"].replace({"": "-", "0": "-"})
    parsed = parsed[(parsed["date"].notna()) & (parsed["id"] != "")].copy()
    if parsed.empty:
        return pd.DataFrame(columns=["date", "campaign_id", "total_regis"])

    parsed = (
        parsed.groupby(["date", "campaign_id"], as_index=False)["id"]
        .nunique()
        .rename(columns={"id": "total_regis"})
        .sort_values(["date", "campaign_id"])
    )
    parsed["total_regis"] = parsed["total_regis"].astype(int)
    return parsed


def parse_instagram_insights_dataframe(raw_rows: list[dict]) -> pd.DataFrame:
    """Parse Instagram insight rows into normalized daily metrics."""
    if not raw_rows:
        return pd.DataFrame()

    df = pd.DataFrame(raw_rows)
    required_columns = [
        "date",
        "total_followers",
        "new_followers",
        "unfollowers",
        "total_engagement",
        "likes",
        "comments",
        "shares",
        "saves",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in Instagram insights payload: {missing_columns}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for column in required_columns:
        if column == "date":
            continue
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df = df[df["date"].notna()]
    return df.sort_values("date")


def parse_instagram_media_insights_dataframe(raw_rows: list[dict]) -> pd.DataFrame:
    """Parse Instagram media insight rows into normalized media-grain metrics."""
    if not raw_rows:
        return pd.DataFrame()

    df = pd.DataFrame(raw_rows)
    required_columns = [
        "date",
        "media_id",
        "media_type",
        "media_product_type",
        "timestamp",
        "caption",
        "permalink",
        "media_url",
        "thumbnail_url",
        "likes",
        "comments",
        "shares",
        "saves",
        "reach",
        "impressions",
        "plays",
        "total_engagement",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in Instagram media insights payload: {missing_columns}")

    parsed_timestamp = pd.to_datetime(df["timestamp"], errors="coerce")
    if getattr(parsed_timestamp.dt, "tz", None) is not None:
        parsed_timestamp = parsed_timestamp.dt.tz_localize(None)

    parsed = pd.DataFrame(
        {
            "date": pd.to_datetime(df["date"], errors="coerce").dt.date,
            "media_id": df["media_id"].fillna("").astype(str).str.strip(),
            "media_type": df["media_type"].fillna("").astype(str).str.strip().str.upper(),
            "media_product_type": df["media_product_type"].fillna("").astype(str).str.strip().str.upper(),
            "timestamp": parsed_timestamp,
            "caption": df["caption"].where(df["caption"].notna(), None),
            "permalink": df["permalink"].where(df["permalink"].notna(), None),
            "media_url": df["media_url"].where(df["media_url"].notna(), None),
            "thumbnail_url": df["thumbnail_url"].where(df["thumbnail_url"].notna(), None),
        }
    )
    metric_columns = [
        "likes",
        "comments",
        "shares",
        "saves",
        "reach",
        "impressions",
        "plays",
        "total_engagement",
    ]
    for column in metric_columns:
        parsed[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    parsed = parsed[
        parsed["date"].notna()
        & (parsed["media_id"] != "")
        & parsed["media_product_type"].isin(["FEED", "REELS"])
    ].copy()
    if parsed.empty:
        return parsed
    parsed = parsed.drop_duplicates(subset=["media_id"], keep="last")
    return parsed.sort_values(["date", "media_product_type", "media_id"])


def parse_facebook_page_insights_dataframe(raw_rows: list[dict]) -> pd.DataFrame:
    """Parse Facebook Page daily insight rows into normalized metrics."""
    if not raw_rows:
        return pd.DataFrame()

    df = pd.DataFrame(raw_rows)
    required_columns = [
        "page_id",
        "date",
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
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in Facebook Page insights payload: {missing_columns}")

    df["page_id"] = df["page_id"].fillna("").astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for column in required_columns:
        if column in {"page_id", "date"}:
            continue
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df = df[(df["date"].notna()) & (df["page_id"] != "")].copy()
    return df.sort_values(["date", "page_id"])


def parse_facebook_page_media_insights_dataframe(raw_rows: list[dict]) -> pd.DataFrame:
    """Parse Facebook Page post/media insight rows into normalized media-grain metrics."""
    if not raw_rows:
        return pd.DataFrame()

    df = pd.DataFrame(raw_rows)
    required_columns = [
        "page_id",
        "date",
        "post_id",
        "post_type",
        "status_type",
        "created_time",
        "message",
        "permalink_url",
        "full_picture",
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
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in Facebook Page media insights payload: {missing_columns}")

    parsed_created_time = pd.to_datetime(df["created_time"], errors="coerce")
    if getattr(parsed_created_time.dt, "tz", None) is not None:
        parsed_created_time = parsed_created_time.dt.tz_localize(None)

    parsed = pd.DataFrame(
        {
            "page_id": df["page_id"].fillna("").astype(str).str.strip(),
            "date": pd.to_datetime(df["date"], errors="coerce").dt.date,
            "post_id": df["post_id"].fillna("").astype(str).str.strip(),
            "post_type": df["post_type"].fillna("").astype(str).str.strip().str.upper(),
            "status_type": df["status_type"].where(df["status_type"].notna(), None),
            "created_time": parsed_created_time,
            "message": df["message"].where(df["message"].notna(), None),
            "permalink_url": df["permalink_url"].where(df["permalink_url"].notna(), None),
            "full_picture": df["full_picture"].where(df["full_picture"].notna(), None),
        }
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
    for column in metric_columns:
        parsed[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    parsed = parsed[
        parsed["date"].notna()
        & (parsed["page_id"] != "")
        & (parsed["post_id"] != "")
    ].copy()
    if parsed.empty:
        return parsed
    parsed = parsed.drop_duplicates(subset=["post_id"], keep="last")
    return parsed.sort_values(["date", "page_id", "post_id"])


def parse_first_deposit_dataframe(raw_rows: list[dict]) -> pd.DataFrame:
    """Parse raw first-deposit API payload into a load-ready dataframe.

    The upstream response is a list of JSON objects with mixed field naming and
    many records that are not useful for downstream reporting. This transformer:
    - maps source keys into project-standard column names,
    - parses registration timestamps into ``date`` objects,
    - normalizes blank campaign IDs into the placeholder ``"-"``,
    - coerces ``First Depo $`` into numeric form,
    - keeps only rows with valid registration dates and positive first deposits,
    - drops rows that do not have a usable user identifier.

    Args:
        raw_rows (list[dict]): Raw JSON rows fetched from the first-deposit API.

    Returns:
        pd.DataFrame: Normalized dataframe containing only rows that are valid
        for DQ checks and upsert into ``data_depo``.

    Raises:
        ValueError: Raised when the payload does not contain the expected source
        columns required by the ETL mapping.
    """
    if not raw_rows:
        return pd.DataFrame()

    df = pd.DataFrame(raw_rows)
    required_columns = ["id", "email", "tgl_regis", "campaignid", "Status\nNew / Existing", "First Depo $"]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in first deposit payload: {missing_columns}")

    def _optional_series(column_name: str) -> pd.Series:
        return df[column_name] if column_name in df.columns else pd.Series([None] * len(df), index=df.index)

    def _clean_string(series: pd.Series, *, lowercase: bool = False) -> pd.Series:
        cleaned = series.fillna("").astype(str).str.strip()
        cleaned = cleaned.replace({"": None, "-": None, "nan": None, "None": None})
        if lowercase:
            cleaned = cleaned.str.lower()
        return cleaned

    def _clean_date(series: pd.Series) -> pd.Series:
        parsed_dates = pd.to_datetime(series.replace({0: None, "0": None, "": None}), errors="coerce")
        if getattr(parsed_dates.dt, "tz", None) is not None:
            parsed_dates = parsed_dates.dt.tz_localize(None)
        # Spreadsheet zero-dates such as 1899-12-30 are placeholders, not real business dates.
        parsed_dates = parsed_dates.where(parsed_dates >= pd.Timestamp("1900-01-01"))
        return parsed_dates.dt.date

    def _clean_decimal(series: pd.Series) -> pd.Series:
        normalized = series.fillna("").astype(str).str.strip()
        normalized = normalized.str.replace(",", ".", regex=False)
        return pd.to_numeric(normalized, errors="coerce")

    parsed = pd.DataFrame(
        {
            "user_id": pd.to_numeric(df["id"], errors="coerce"),
            "fullname": _clean_string(_optional_series("fullname")),
            "email": _clean_string(df["email"], lowercase=True),
            "phone": _clean_string(_optional_series("phone")),
            "tanggal_regis": pd.to_datetime(df["tgl_regis"], errors="coerce").dt.date,
            "campaign_id": df["campaignid"].fillna("").astype(str).str.strip(),
            "tag": _clean_string(_optional_series("tag")),
            "protection": pd.to_numeric(_optional_series("protection"), errors="coerce"),
            "user_status": _clean_string(df["Status\nNew / Existing"]),
            "assign_date": _clean_date(_optional_series("Assign Date")),
            "analyst": pd.to_numeric(_optional_series("Analyst"), errors="coerce"),
            "first_depo_date": _clean_date(_optional_series("First Depo Date")),
            "first_depo": _clean_decimal(df["First Depo $"]),
            "time_to_closing": _clean_string(_optional_series("Time To Closing")),
            "nmi": pd.to_numeric(_optional_series("NMI"), errors="coerce"),
            "lot": pd.to_numeric(_optional_series("Lot"), errors="coerce"),
            "cabang": _clean_string(_optional_series("Cabang")),
            "pool": _optional_series("Pool"),
        }
    )
    parsed["campaign_id"] = parsed["campaign_id"].replace({"": "-", "0": "-"})
    parsed["first_depo"] = parsed["first_depo"].fillna(0.0).astype(float)
    parsed["protection"] = pd.to_numeric(parsed["protection"], errors="coerce").fillna(0).astype(int)
    parsed["analyst"] = pd.to_numeric(parsed["analyst"], errors="coerce")
    parsed["nmi"] = pd.to_numeric(parsed["nmi"], errors="coerce")
    parsed["lot"] = pd.to_numeric(parsed["lot"], errors="coerce")
    parsed["pool"] = parsed["pool"].where(parsed["pool"].notna(), None)
    parsed = parsed.loc[
        parsed["tanggal_regis"].notna()
        & (parsed["first_depo"] > 0)
        & parsed["tag"].fillna("").str.contains("CP1", case=False, na=False)
    ].copy()
    parsed = parsed.loc[parsed["user_id"].notna()].copy()
    if parsed.empty:
        return parsed

    parsed["user_id"] = parsed["user_id"].astype(int)
    parsed["analyst"] = parsed["analyst"].where(parsed["analyst"].notna(), None)
    parsed["nmi"] = parsed["nmi"].where(parsed["nmi"].notna(), None)
    parsed["lot"] = parsed["lot"].where(parsed["lot"].notna(), None)
    return parsed


def parse_ms_deposit_dataframe(raw_rows: list[dict]) -> pd.DataFrame:
    """Parse raw MS1 deposit/activity payload into a load-ready dataframe."""
    if not raw_rows:
        return pd.DataFrame()

    df = pd.DataFrame(raw_rows)
    required_columns = [
        "email",
        "tag",
        "campaignid",
        "Status\nNew / Existing",
        "First Depo $",
        "Time To Closing",
        "Last Depo",
        "Last Depo Amount",
        "Last Activity",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in MS deposit payload: {missing_columns}")

    def _clean_string(series: pd.Series, *, lowercase: bool = False) -> pd.Series:
        cleaned = series.fillna("").astype(str).str.strip()
        cleaned = cleaned.replace({"": None, "-": None, "nan": None, "None": None})
        if lowercase:
            cleaned = cleaned.str.lower()
        return cleaned

    def _clean_date(series: pd.Series) -> pd.Series:
        parsed_dates = pd.to_datetime(series.replace({0: None, "0": None, "": None}), errors="coerce")
        if getattr(parsed_dates.dt, "tz", None) is not None:
            parsed_dates = parsed_dates.dt.tz_localize(None)
        parsed_dates = parsed_dates.where(parsed_dates >= pd.Timestamp("1900-01-01"))
        return parsed_dates.dt.date

    def _clean_decimal(series: pd.Series) -> pd.Series:
        normalized = series.fillna("").astype(str).str.strip()
        normalized = normalized.str.replace(",", ".", regex=False)
        return pd.to_numeric(normalized, errors="coerce")

    parsed = pd.DataFrame(
        {
            "email": _clean_string(df["email"], lowercase=True),
            "tag": _clean_string(df["tag"]),
            "campaign_id": df["campaignid"].fillna("").astype(str).str.strip(),
            "user_status": _clean_string(df["Status\nNew / Existing"]),
            "first_depo": _clean_decimal(df["First Depo $"]),
            "time_to_closing": _clean_string(df["Time To Closing"]),
            "last_depo": _clean_date(df["Last Depo"]),
            "last_depo_amount": _clean_decimal(df["Last Depo Amount"]),
            "last_activity": _clean_date(df["Last Activity"]),
        }
    )
    parsed["campaign_id"] = parsed["campaign_id"].replace({"": "-", "0": "-"})
    parsed["first_depo"] = parsed["first_depo"].fillna(0.0).astype(float)
    parsed["last_depo_amount"] = pd.to_numeric(parsed["last_depo_amount"], errors="coerce")
    parsed = parsed.loc[
        parsed["last_activity"].notna()
        & (parsed["first_depo"] >= 0)
        & parsed["tag"].fillna("").str.contains("MS1", case=False, na=False)
    ].copy()
    parsed = parsed.loc[parsed["email"].notna()].copy()
    return parsed
