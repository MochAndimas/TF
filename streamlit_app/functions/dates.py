"""Date helpers for Streamlit pages."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from dateutil.relativedelta import relativedelta


def get_date_range(days, period="days", months=3):
    """Return either a rolling-day range or previous full-month range."""
    if period == "days":
        end_date = datetime.today() - timedelta(days=1)
        start_date = datetime.today() - timedelta(days=days)
    elif period == "months":
        end_date = datetime.today().replace(day=1)
        start_date = end_date - relativedelta(months=months)
        end_date = end_date - relativedelta(days=1)
    else:
        raise ValueError("period must be either 'days' or 'months'.")

    return start_date.date(), end_date.date()


def campaign_preset_ranges(today: date) -> dict[str, tuple[date, date] | None]:
    """Build reusable preset date windows for campaign-facing dashboard pages."""
    yesterday = today - timedelta(days=1)
    this_month_start = today.replace(day=1)
    last_month_start = (this_month_start - timedelta(days=1)).replace(day=1)
    last_month_end = this_month_start - timedelta(days=1)
    return {
        "Last 7 Day": (today - timedelta(days=7), yesterday),
        "Last 30 Day": (today - timedelta(days=30), yesterday),
        "This Month": (this_month_start, today),
        "Last Month": (last_month_start, last_month_end),
        "Custom Range": None,
    }
