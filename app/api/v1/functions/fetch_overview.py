"""Payload assembler for overview active-user endpoint.

This module orchestrates active-user data retrieval from ``OverviewData`` and
returns a response shape that is directly consumed by FE cards and chart blocks.
"""

from datetime import date
import asyncio

from app.utils.overview import OverviewData


async def fetch_overview_active_users_payload(
    overview_data: OverviewData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Assemble active-user section payload for one source and period.

    Args:
        overview_data: Preloaded service object with GA4 source data.
        start_date: Inclusive reporting start date.
        end_date: Inclusive reporting end date.

    Returns:
        dict[str, object]: Payload containing period metadata, stickiness summary,
        and dual-axis active-users chart payload.
    """
    stickiness_summary, chart_payload = await asyncio.gather(
        overview_data.stickiness_with_growth(from_date=start_date, to_date=end_date),
        overview_data.active_users_chart(from_date=start_date, to_date=end_date),
    )
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "source": overview_data.source,
        "stickiness_with_growth": stickiness_summary,
        "active_users_chart": chart_payload,
    }
