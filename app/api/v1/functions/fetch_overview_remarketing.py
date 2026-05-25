"""Payload assembler for overview remarketing-performance endpoint."""

from datetime import date
import asyncio

from app.utils.overview import OverviewRemarketingPerformanceData


async def fetch_overview_remarketing_payload(
    remarketing_data: OverviewRemarketingPerformanceData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    metrics, spend_chart, performance_chart, performance_by_source = await asyncio.gather(
        remarketing_data.metrics_with_growth(from_date=start_date, to_date=end_date),
        remarketing_data.spend_chart(from_date=start_date, to_date=end_date),
        remarketing_data.performance_chart(from_date=start_date, to_date=end_date),
        remarketing_data.performance_by_source(from_date=start_date, to_date=end_date),
    )
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "metrics_with_growth": metrics,
        "spend_chart": spend_chart,
        "performance_chart": performance_chart,
        "performance_by_source": performance_by_source,
    }
