"""Payload assembler for overview campaign-cost endpoint.

This module combines campaign-cost metrics and chart datasets into one response
object for the Overview FE section "Ad Cost Spend".
"""

from datetime import date
import asyncio

from app.utils.overview import OverviewCampaignCostData


async def fetch_overview_campaign_cost_payload(
    campaign_cost_data: OverviewCampaignCostData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Assemble campaign-cost section payload for a selected period.

    Args:
        campaign_cost_data: Preloaded service object with campaign-cost records.
        start_date: Inclusive reporting start date.
        end_date: Inclusive reporting end date.

    Returns:
        dict[str, object]: Payload containing period metadata, metric summary with
        growth percentages, and pie breakdown charts.
    """
    metrics, charts = await asyncio.gather(
        campaign_cost_data.cost_metrics_with_growth(from_date=start_date, to_date=end_date),
        campaign_cost_data.cost_breakdown_charts(from_date=start_date, to_date=end_date),
    )
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "cost_metrics_with_growth": metrics,
        "cost_breakdown_charts": charts,
    }
