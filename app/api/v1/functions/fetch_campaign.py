from datetime import date
import asyncio

from app.utils.campaign_utils import CampaignData


async def fetch_leads_by_source_payload(
    campaign_data: CampaignData,
    chart: str,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build leads-by-source response payload based on requested chart type."""
    chart_type = chart.strip().lower()

    if chart_type == "table":
        return {
            "table": await campaign_data.leads_by_source_table(
                from_date=start_date,
                to_date=end_date,
            )
        }

    if chart_type == "pie":
        return {
            "pie": await campaign_data.leads_by_source_pie_chart(
                from_date=start_date,
                to_date=end_date,
            )
        }

    return {
        "table": await campaign_data.leads_by_source_table(
            from_date=start_date,
            to_date=end_date,
        ),
        "pie": await campaign_data.leads_by_source_pie_chart(
            from_date=start_date,
            to_date=end_date,
        ),
    }


async def fetch_ads_metrics_with_growth_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build ads metrics-with-growth payload for all supported ad sources."""
    google_task = campaign_data.ads_metrics_with_growth(
        data="google",
        from_date=start_date,
        to_date=end_date,
    )
    facebook_task = campaign_data.ads_metrics_with_growth(
        data="facebook",
        from_date=start_date,
        to_date=end_date,
    )
    tiktok_task = campaign_data.ads_metrics_with_growth(
        data="tiktok",
        from_date=start_date,
        to_date=end_date,
    )
    google, facebook, tiktok = await asyncio.gather(google_task, facebook_task, tiktok_task)

    return {
        "google": google,
        "facebook": facebook,
        "tiktok": tiktok,
    }


async def fetch_campaign_overview_payload(
    campaign_data: CampaignData,
    chart: str,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build combined overview payload for dashboard initial load."""
    leads_by_source, ads_metrics_growth = await asyncio.gather(
        fetch_leads_by_source_payload(
            campaign_data=campaign_data,
            chart=chart,
            start_date=start_date,
            end_date=end_date,
        ),
        fetch_ads_metrics_with_growth_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
    )

    return {
        "leads_by_source": leads_by_source,
        "ads_metrics_with_growth": ads_metrics_growth,
    }
