"""Payload assembler for overview brand-awareness endpoint.

This module gathers KPI summaries and chart payloads for the Overview
"Overall Performance Campaign Brand Awareness" section.
"""

from datetime import date
import asyncio

from app.utils.overview import OverviewBrandAwarenessData


async def fetch_overview_brand_awareness_payload(
    brand_data: OverviewBrandAwarenessData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Assemble brand-awareness section payload for a selected period.

    Args:
        brand_data: Preloaded service object for BA ads data.
        start_date: Inclusive reporting start date.
        end_date: Inclusive reporting end date.

    Returns:
        dict[str, object]: Payload containing metric cards with growth,
        spend bar chart, and mixed performance chart.
    """
    metrics, spend_chart, performance_chart = await asyncio.gather(
        brand_data.metrics_with_growth(from_date=start_date, to_date=end_date),
        brand_data.spend_chart(from_date=start_date, to_date=end_date),
        brand_data.performance_chart(from_date=start_date, to_date=end_date),
    )
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "metrics_with_growth": metrics,
        "spend_chart": spend_chart,
        "performance_chart": performance_chart,
    }
