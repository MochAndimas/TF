"""Header contract and strict normalization of OIS user records."""
import math
from datetime import datetime

import pandas as pd

from app.etl.quality import MIN_HISTORICAL_DATE

COLUMNS = {
    "tf_id": "tf_id", "email": "email", "phone": "phone", "username": "username",
    "tgl_regis": "tgl_regis", "utm_source": "utm_source", "utm_medium": "utm_medium",
    "utm_campaign": "utm_campaign", "Status New / Existing": "user_status",
    "Assign Date": "assign_date", "Analyst": "analyst", "First Subs": "first_subs",
    "First Subs (IDR)": "first_subs_idr", "Time To Closing": "time_to_closing",
    "Cabang": "cabang", "Pool": "pool", "Status": "status",
    "First Assign Date": "first_assign_date", "Last Subs": "last_subs",
    "Last Subs Nominal (IDR)": "last_subs_nominal_idr", "Last Activity": "last_activity",
}
DATE_FIELDS = ("tgl_regis", "assign_date", "first_subs", "first_assign_date", "last_subs", "last_activity")
AMOUNT_FIELDS = ("first_subs_idr", "last_subs_nominal_idr")


def project_rows(values: list) -> list[dict]:
    """Require the header contract, preserve strings and pad trailing empty cells."""
    if not values:
        raise ValueError("DQ failed: OIS sheet is missing its header row.")

    def normalize(value): return "".join(str(value).lower().split())
    mapping = {normalize(header): field for header, field in COLUMNS.items()}
    headers = [mapping.get(normalize(value)) for value in values[0]]
    selected = [header for header in headers if header is not None]
    if len(selected) != len(set(selected)) or set(COLUMNS.values()) - set(selected):
        raise ValueError("DQ failed: OIS sheet has missing or duplicate required headers.")
    rows = []
    for row in values[1:]:
        if not any(value is not None and str(value).strip() for value in row):
            continue
        if len(row) > len(headers):
            raise ValueError("DQ failed: OIS row exceeds header width.")
        rows.append({field: row[index] if index < len(row) else None
                     for index, field in enumerate(headers) if field is not None})
    return rows


def text_value(value):
    return str(value).strip() if value is not None and str(value).strip() else None


def timestamp(value, *, required=False):
    value = text_value(value)
    if value in (None, "0", "-"):
        if required:
            raise ValueError("DQ failed: OIS has a missing registration timestamp.")
        return None
    try:
        # FORMATTED_STRING requests avoid interpreting spreadsheet serials as nanoseconds.
        parsed = datetime.fromisoformat(value)
    except ValueError:
        parsed = None
        for pattern in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
            try:
                parsed = datetime.strptime(value, pattern)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError("DQ failed: OIS contains an invalid timestamp.") from None
    if parsed.tzinfo is not None:
        raise ValueError("DQ failed: OIS timestamps must use the sheet's local time.")
    too_old = parsed.date() < MIN_HISTORICAL_DATE if required else parsed.year < 1900
    if too_old:
        if required:
            raise ValueError("DQ failed: OIS registration timestamp is too old.")
        return None
    return parsed


def parse_rows(raw_rows: list[dict]) -> pd.DataFrame:
    records = []
    for raw in raw_rows:
        if set(COLUMNS.values()) - set(raw):
            raise ValueError("DQ failed: OIS payload is missing required fields.")
        row = {field: text_value(raw[field]) for field in COLUMNS.values()}
        if row["utm_source"] not in {"ois_id", "ois.id"}:
            continue
        for field in DATE_FIELDS:
            row[field] = timestamp(raw[field], required=field == "tgl_regis")
        row["registration_date"] = row["tgl_regis"].date()
        for field in AMOUNT_FIELDS:
            value = row[field]
            try:
                row[field] = float(value) if value is not None else None
            except (ValueError, TypeError):
                raise ValueError(f"DQ failed: OIS has an invalid amount in {field}.") from None
            if row[field] is not None and (not math.isfinite(row[field]) or row[field] < 0):
                raise ValueError(f"DQ failed: OIS has an invalid amount in {field}.")
        value = row["pool"]
        if value is not None and value.lower() not in {"true", "false"}:
            raise ValueError("DQ failed: OIS Pool must be TRUE, FALSE or blank.")
        row["pool"] = value.lower() == "true" if value is not None else None
        records.append(row)
    df = pd.DataFrame(records, columns=[*COLUMNS.values(), "registration_date"], dtype=object)
    validate_rows(df)
    return df


def validate_rows(df: pd.DataFrame) -> None:
    if df.empty:
        return
    if df.tf_id.isna().any() or df.tf_id.eq("").any():
        raise ValueError("DQ failed: OIS has missing tf_id values.")
    if df.duplicated("tf_id").any():
        raise ValueError("DQ failed: OIS has duplicate tf_id values.")
