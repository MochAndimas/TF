"""Reporting dimensions and date-window policy shared by UI jobs and scheduler."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from app.core.config import settings
from app.etl.transform import resolve_date_window

REPORT_LEVELS = ("campaign", "ad_group", "ad")
METRICS = ("impressions", "clicks", "spend", "ctr", "cpc", "cpm", "conversions")


def resolve_gpt_ads_window(types, start_date=None, end_date=None):
    if not 1 <= settings.GPT_ADS_LOOKBACK_DAYS <= 365:
        raise ValueError("GPT_ADS_LOOKBACK_DAYS must be between 1 and 365")
    if types == "auto":
        yesterday = datetime.now(ZoneInfo(settings.GPT_ADS_REPORTING_TIMEZONE)).date() - timedelta(days=1)
        return yesterday - timedelta(days=settings.GPT_ADS_LOOKBACK_DAYS - 1), yesterday
    return resolve_date_window(types, start_date, end_date)


def report_fields(level):
    hierarchy = list(REPORT_LEVELS)[:list(REPORT_LEVELS).index(level) + 1]
    return ["metadata.readable_time", "metadata.timezone"] + [
        f"{entity}.{field}" for entity in hierarchy for field in ("id", "name")
    ] + [f"{level}.{metric}" for metric in METRICS if metric != "conversions"] + ["conversions"]
