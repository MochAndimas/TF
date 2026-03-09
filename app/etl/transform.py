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


def parse_depo_dataframe(raw_data: list) -> pd.DataFrame:
    """Parse deposit source payload into normalized dataframe.

    Args:
        raw_data (list): Raw JSON records from deposit source endpoint.

    Returns:
        pd.DataFrame: Cleaned dataframe with standardized dtypes and nullable
        handling, ready for DQ checks and load phase.

    Raises:
        ValueError: Raised when required columns are missing.
    """
    df = pd.DataFrame(raw_data)
    if df.empty:
        return df

    required_columns = [
        "id",
        "tgl_regis",
        "fullname",
        "email",
        "phone",
        "Status\nNew / Existing",
        "campaignid",
        "tag",
        "protection",
        "Assign Date",
        "Analyst",
        "First Depo Date",
        "First Depo $",
        "Time To Closing",
        "NMI",
        "Lot",
        "Cabang",
        "Pool",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in depo source: {missing_columns}")

    null_markers = {"null", "none", "nan", ""}
    object_columns = df.select_dtypes(include=["object"]).columns
    for column in object_columns:
        normalized = df[column].astype(str).str.strip().str.lower()
        df.loc[normalized.isin(null_markers), column] = None

    df = df[df["tag"].notna()]
    df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0)
    df["campaignid"] = pd.to_numeric(df["campaignid"], errors="coerce").fillna(0)
    df["protection"] = pd.to_numeric(df["protection"], errors="coerce").fillna(0)
    df["Analyst"] = pd.to_numeric(df["Analyst"], errors="coerce").fillna(0)
    df["NMI"] = pd.to_numeric(df["NMI"], errors="coerce").fillna(0)
    df["Lot"] = pd.to_numeric(df["Lot"], errors="coerce").fillna(0)
    df["First Depo $"] = pd.to_numeric(df["First Depo $"], errors="coerce").fillna(0.0)

    df["id"] = df["id"].astype(int)
    df["tgl_regis"] = pd.to_datetime(
        df["tgl_regis"],
        format="%Y-%m-%dT%H:%M:%S.%fZ",
        utc=True,
        errors="coerce",
    ).dt.tz_localize(None).dt.date
    df["fullname"] = df["fullname"].astype(str)
    df["email"] = df["email"].astype(str)
    df["phone"] = df["phone"].astype(str)
    df["Status\nNew / Existing"] = df["Status\nNew / Existing"].astype(str)
    df["campaignid"] = df["campaignid"].astype(int).astype(str)
    df["tag"] = df["tag"].astype(str)
    df["protection"] = df["protection"].astype(int)
    df["Assign Date"] = pd.to_datetime(
        df["Assign Date"],
        format="%Y-%m-%dT%H:%M:%S.%fZ",
        utc=True,
        errors="coerce",
    ).dt.tz_localize(None).dt.date
    df["Analyst"] = df["Analyst"].astype(int)
    df["First Depo Date"] = pd.to_datetime(
        df["First Depo Date"],
        format="%Y-%m-%dT%H:%M:%S.%fZ",
        utc=True,
        errors="coerce",
    ).dt.tz_localize(None).dt.date
    df["First Depo $"] = df["First Depo $"].astype(float)
    df["Time To Closing"] = df["Time To Closing"].astype(str)
    df["NMI"] = df["NMI"].astype(int)
    df["Lot"] = df["Lot"].astype(int)
    df["Cabang"] = df["Cabang"].astype(str)
    df["Pool"] = df["Pool"].apply(lambda value: str(value).strip().lower() in {"true", "1", "yes", "y"})
    df = df[df["tgl_regis"].notna()]
    df.replace({np.nan: None}, inplace=True)
    return df


def parse_ads_dataframe(raw_rows: list) -> pd.DataFrame:
    """Parse Google Sheets rows into normalized ads dataframe.

    Args:
        raw_rows (list): Raw values rows where first row is header and the rest
            are campaign daily metric rows.

    Returns:
        pd.DataFrame: Normalized ads dataframe with required business-key and
        metric columns in numeric/date dtypes.

    Raises:
        ValueError: Raised when required headers are missing.
    """
    if not raw_rows or len(raw_rows) < 2:
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


def aggregate_ads_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate ads rows into daily business-grain records for loading.

    Args:
        df (pd.DataFrame): Parsed ads dataframe that may contain duplicate
            business keys.

    Returns:
        pd.DataFrame: Aggregated dataframe grouped by
        ``date/campaign_id/ad_group/ad_name`` with summed metrics.
    """
    if df.empty:
        return df

    aggregated = (
        df.groupby(["date", "campaign_id", "ad_group", "ad_name"], as_index=False)
        .agg(
            campaign_name=("campaign_name", "first"),
            cost=("cost", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            leads=("leads", "sum"),
        )
    )
    aggregated["cost"] = aggregated["cost"].astype(float)
    aggregated["impressions"] = aggregated["impressions"].astype(int)
    aggregated["clicks"] = aggregated["clicks"].astype(int)
    aggregated["leads"] = aggregated["leads"].astype(int)
    return aggregated


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
