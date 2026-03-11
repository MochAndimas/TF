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
            "first_depo": pd.to_numeric(df["First Depo $"], errors="coerce"),
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
    parsed = parsed.loc[parsed["tanggal_regis"].notna() & (parsed["first_depo"] > 0)].copy()
    parsed = parsed.loc[parsed["user_id"].notna()].copy()
    if parsed.empty:
        return parsed

    parsed["user_id"] = parsed["user_id"].astype(int)
    parsed["analyst"] = parsed["analyst"].where(parsed["analyst"].notna(), None)
    parsed["nmi"] = parsed["nmi"].where(parsed["nmi"].notna(), None)
    parsed["lot"] = parsed["lot"].where(parsed["lot"].notna(), None)
    return parsed
