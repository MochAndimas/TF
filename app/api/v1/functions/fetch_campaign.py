from datetime import date
import asyncio

from app.utils.campaign_utils import CampaignData


async def fetch_leads_by_source_payload(
    campaign_data: CampaignData,
    chart: str,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build leads-by-source payload based on chart mode.

    Args:
        campaign_data (CampaignData): Preloaded campaign data service instance.
        chart (str): Requested chart mode (`table`, `pie`, or `both`).
        start_date (date): Start date for aggregation window.
        end_date (date): End date for aggregation window.

    Returns:
        dict[str, object]: Payload containing one or both keys:
            - ``table``: Leads-by-source table payload.
            - ``pie``: Leads-by-source pie chart payload.
    """
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
    """Build metrics-with-growth payload for each ads source.

    Args:
        campaign_data (CampaignData): Preloaded campaign data service instance.
        start_date (date): Start date for metrics calculation.
        end_date (date): End date for metrics calculation.

    Returns:
        dict[str, object]: Mapping by source key (`google`, `facebook`, `tiktok`)
            where each value contains current-period metrics, previous-period
            metrics, and growth percentages.
    """
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


async def fetch_leads_performance_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build chart payload bundle for each ads source.

    Args:
        campaign_data (CampaignData): Preloaded campaign data service instance.
        start_date (date): Start date for chart data extraction.
        end_date (date): End date for chart data extraction.

    Returns:
        dict[str, object]: Mapping keyed by source (`google`, `facebook`, `tiktok`)
            that contains:
            - ``cost_to_leads`` chart payload.
            - ``leads_by_periods`` chart payload.
            - ``clicks_to_leads`` chart payload.
    """
    sources = ("google", "facebook", "tiktok")
    tasks = []
    for source in sources:
        tasks.extend(
            [
                campaign_data.cost_to_leads_chart(source, start_date, end_date),
                campaign_data.leads_by_periods_chart(source, start_date, end_date),
                campaign_data.clicks_to_leads_chart(source, start_date, end_date),
            ]
        )

    results = await asyncio.gather(*tasks)
    payload: dict[str, object] = {}
    for index, source in enumerate(sources):
        base_index = index * 3
        payload[source] = {
            "cost_to_leads": results[base_index],
            "leads_by_periods": results[base_index + 1],
            "clicks_to_leads": results[base_index + 2],
        }
    return payload


async def fetch_ads_campaign_details_tables_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build campaign-details table payload for all ads sources.

    Args:
        campaign_data (CampaignData): Preloaded campaign data service instance.
        start_date (date): Start date for detail rows.
        end_date (date): End date for detail rows.

    Returns:
        dict[str, object]: Mapping keyed by source (`google`, `facebook`, `tiktok`)
            with each value containing table figure payload and raw rows.
    """
    sources = ("google", "facebook", "tiktok")
    results = await asyncio.gather(
        *[campaign_data.ads_campaign_details_table(source, start_date, end_date) for source in sources]
    )
    return {source: result for source, result in zip(sources, results)}


async def fetch_campaign_overview_payload(
    campaign_data: CampaignData,
    chart: str,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build full campaign overview payload used by dashboard screens.

    Args:
        campaign_data (CampaignData): Preloaded campaign data service instance.
        chart (str): Leads-by-source chart mode (`table`, `pie`, `both`).
        start_date (date): Start date for all requested aggregates/charts.
        end_date (date): End date for all requested aggregates/charts.

    Returns:
        dict[str, object]: Composite response payload with:
            - ``leads_by_source`` summary charts.
            - ``ads_metrics_with_growth`` KPI metrics and growth percentages.
            - ``leads_performance_charts`` multi-chart source performance.
            - ``ads_campaign_details`` campaign-level table details.
    """
    leads_by_source, ads_metrics_growth, performance_charts, campaign_details_tables = await asyncio.gather(
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
        fetch_leads_performance_charts_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
        fetch_ads_campaign_details_tables_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
    )

    return {
        "leads_by_source": leads_by_source,
        "ads_metrics_with_growth": ads_metrics_growth,
        "leads_performance_charts": performance_charts,
        "ads_campaign_details": campaign_details_tables,
    }
