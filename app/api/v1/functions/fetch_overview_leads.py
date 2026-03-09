"""Payload assembler for overview user-acquisition endpoint.

This module gathers KPI summaries and chart/table blocks for the Overview
"Overall Performance Campaign User Acquisition" section.
"""

from datetime import date
import asyncio

from app.utils.overview_utils import OverviewLeadsAcquisitionData


async def fetch_overview_leads_acquisition_payload(
    leads_data: OverviewLeadsAcquisitionData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Assemble user-acquisition section payload for a selected period.

    Args:
        leads_data: Preloaded service object for UA ads and first-deposit data.
        start_date: Inclusive reporting start date.
        end_date: Inclusive reporting end date.

    Returns:
        dict[str, object]: Payload containing metric cards, leads-by-source table
        and pie chart, cost-vs-leads trend, leads-per-day chart, and
        cost-to-first-deposit chart.
    """
    (
        metrics,
        leads_by_source,
        cost_vs_leads,
        leads_per_day,
        cost_to_revenue,
    ) = await asyncio.gather(
        leads_data.metrics_with_growth(from_date=start_date, to_date=end_date),
        leads_data.leads_by_source(from_date=start_date, to_date=end_date),
        leads_data.cost_vs_leads_chart(from_date=start_date, to_date=end_date),
        leads_data.leads_per_day_chart(from_date=start_date, to_date=end_date),
        leads_data.cost_to_revenue_chart(from_date=start_date, to_date=end_date),
    )
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "metrics_with_growth": metrics,
        "leads_by_source": leads_by_source,
        "cost_vs_leads_chart": cost_vs_leads,
        "leads_per_day_chart": leads_per_day,
        "cost_to_revenue_chart": cost_to_revenue,
    }
