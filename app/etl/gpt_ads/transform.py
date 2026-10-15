"""Strict validation of daily Ads API rows without recomputing finalized metrics."""
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from app.etl.gpt_ads.config import REPORT_LEVELS, METRICS


def text(value, field, *, required=False):
    if value is None or not str(value).strip():
        if required:
            raise ValueError(f"DQ failed: GPT Ads missing {field}")
        return None
    return str(value).strip()


def number(value, field, *, integer=False):
    if value is None:
        if integer:
            raise ValueError(f"DQ failed: GPT Ads missing {field}")
        return None
    try:
        if isinstance(value, bool):
            raise ValueError()
        result = Decimal(str(value))
        if not result.is_finite() or result < 0 or result >= Decimal('1e18'):
            raise ValueError()
        if integer and result != result.to_integral_value():
            raise ValueError()
        if field == "ctr" and result > 1:
            raise ValueError()
        return int(result) if integer else result
    except (InvalidOperation, ValueError):
        raise ValueError(f"DQ failed: GPT Ads invalid {field}") from None


def normalize_row(raw, level, account, start, end, run_id):
    if level not in REPORT_LEVELS:
        raise ValueError("DQ failed: GPT Ads invalid report level")
    try:
        day = date.fromisoformat(raw.get("readable_time", ""))
    except (ValueError, TypeError):
        raise ValueError("DQ failed: GPT Ads invalid readable_time") from None
    if not start <= day <= end:
        raise ValueError("DQ failed: GPT Ads row is outside requested date range")
    if raw.get("timezone") != account["timezone"]:
        raise ValueError("DQ failed: GPT Ads row timezone differs from account")
    if any(metric not in raw for metric in METRICS):
        raise ValueError("DQ failed: GPT Ads report is missing requested metrics")
    row = dict(report_level=level, account_id=account["id"], date=day, timezone=account["timezone"],
               currency=account["currency_code"].upper(), run_id=run_id,
               updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
               attribution_window_days=30, view_through_attribution_window_days=1,
               attribution_time_basis="ad_event_time")
    if len(row["currency"]) != 3 or not row["currency"].isalpha():
        raise ValueError("DQ failed: GPT Ads invalid account currency")
    hierarchy = list(REPORT_LEVELS)
    for entity in hierarchy:
        row[f"{entity}_id"] = text(raw.get(f"{entity}_id"), f"{entity}_id", required=hierarchy.index(entity) <= hierarchy.index(level))
        row[f"{entity}_name"] = text(raw.get(f"{entity}_name"), f"{entity}_name")
    row["entity_id"] = row[f"{level}_id"]
    row.update({metric: number(raw[metric], metric, integer=metric in {"impressions", "clicks"}) for metric in METRICS})
    return row


def restore_row(row):
    """Restore database types after the bounded temporary spool."""
    row["date"] = date.fromisoformat(row["date"])
    row["updated_at"] = datetime.fromisoformat(row["updated_at"])
    for metric in METRICS:
        if metric not in {"impressions", "clicks"} and row[metric] is not None:
            row[metric] = Decimal(row[metric])
    return row
