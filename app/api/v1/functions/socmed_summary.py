"""Attach period comparisons while preserving each platform's media summary."""
from datetime import date
from numbers import Real

from app.utils.period_comparison import growth_percentage


def media_summary_with_growth(current: dict, previous: dict, *, start_date: date,
                              end_date: date, previous_start: date, previous_end: date) -> dict:
    current_metrics = {key: value for key, value in current.get("totals", current).items()
                       if isinstance(value, Real)}
    previous_metrics = {key: value for key, value in previous.get("totals", previous).items()
                        if isinstance(value, Real)}
    return {
        **current,
        "current_period": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
                           "metrics": current_metrics},
        "previous_period": {"start_date": previous_start.isoformat(), "end_date": previous_end.isoformat(),
                            "metrics": previous_metrics},
        "growth_percentage": {key: growth_percentage(float(value), float(previous_metrics.get(key, 0)))
                              for key, value in current_metrics.items()},
    }
