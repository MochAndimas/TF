from datetime import date
import asyncio

from app.utils.campaign_utils import CampaignData


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


async def fetch_brand_awareness_metrics_with_growth_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build Brand Awareness metrics-with-growth payload for each ads source."""
    google_task = campaign_data.brand_awareness_metrics_with_growth(
        data="google",
        from_date=start_date,
        to_date=end_date,
    )
    facebook_task = campaign_data.brand_awareness_metrics_with_growth(
        data="facebook",
        from_date=start_date,
        to_date=end_date,
    )
    tiktok_task = campaign_data.brand_awareness_metrics_with_growth(
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
    """Build ads details payload for all ads sources.

    Args:
        campaign_data (CampaignData): Preloaded campaign data service instance.
        start_date (date): Start date for detail rows.
        end_date (date): End date for detail rows.

    Returns:
        dict[str, object]: Mapping keyed by source (`google`, `facebook`, `tiktok`)
            with each value containing raw ``rows`` for frontend grouping/filtering.
    """
    sources = ("google", "facebook", "tiktok")
    results = await asyncio.gather(
        *[campaign_data.ads_campaign_details_table(source, start_date, end_date) for source in sources]
    )
    return {source: result for source, result in zip(sources, results)}


async def fetch_user_acquisition_insight_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build backend-processed insight chart payloads for User Acquisition page."""
    sources = ("google", "facebook", "tiktok")
    dimensions = ("campaign_id", "ad_group", "ad_name")
    top_n = 10
    pacing_top_n = 8
    ratio_top_n = 8
    ratio_metrics = ("cost_per_lead", "click_per_lead", "click_through_lead")

    scatter_tasks = []
    top_tasks = []
    cumulative_tasks = []
    daily_mix_tasks = []
    ratio_tasks = []
    for source in sources:
        for dimension in dimensions:
            for metric in ratio_metrics:
                ratio_tasks.append(
                    campaign_data.user_acquisition_ratio_trend_chart(
                        data=source,
                        dimension=dimension,
                        metric=metric,
                        top_n=ratio_top_n,
                        from_date=start_date,
                        to_date=end_date,
                    )
                )
            cumulative_tasks.append(
                campaign_data.user_acquisition_cumulative_chart(
                    data=source,
                    dimension=dimension,
                    top_n=pacing_top_n,
                    from_date=start_date,
                    to_date=end_date,
                )
            )
            daily_mix_tasks.append(
                campaign_data.user_acquisition_daily_mix_chart(
                    data=source,
                    dimension=dimension,
                    top_n=pacing_top_n,
                    from_date=start_date,
                    to_date=end_date,
                )
            )
            scatter_tasks.append(
                campaign_data.user_acquisition_spend_vs_leads_chart(
                    data=source,
                    dimension=dimension,
                    from_date=start_date,
                    to_date=end_date,
                )
            )
            top_tasks.append(
                campaign_data.user_acquisition_top_leads_chart(
                    data=source,
                    dimension=dimension,
                    top_n=top_n,
                    from_date=start_date,
                    to_date=end_date,
                )
            )

    (
        scatter_results,
        top_results,
        cumulative_results,
        daily_mix_results,
        ratio_results,
    ) = await asyncio.gather(
        asyncio.gather(*scatter_tasks),
        asyncio.gather(*top_tasks),
        asyncio.gather(*cumulative_tasks),
        asyncio.gather(*daily_mix_tasks),
        asyncio.gather(*ratio_tasks),
    )

    spend_vs_leads: dict[str, dict[str, object]] = {source: {} for source in sources}
    top_leads: dict[str, dict[str, object]] = {source: {} for source in sources}
    cumulative: dict[str, dict[str, object]] = {source: {} for source in sources}
    daily_mix: dict[str, dict[str, object]] = {source: {} for source in sources}
    ratio_trends: dict[str, dict[str, dict[str, object]]] = {source: {} for source in sources}
    scatter_index = 0
    top_index = 0
    cumulative_index = 0
    daily_mix_index = 0
    ratio_index = 0
    for source in sources:
        for dimension in dimensions:
            ratio_trends[source][dimension] = {}
            for metric in ratio_metrics:
                ratio_trends[source][dimension][metric] = ratio_results[ratio_index]
                ratio_index += 1
            cumulative[source][dimension] = cumulative_results[cumulative_index]
            daily_mix[source][dimension] = daily_mix_results[daily_mix_index]
            spend_vs_leads[source][dimension] = scatter_results[scatter_index]
            top_leads[source][dimension] = top_results[top_index]
            cumulative_index += 1
            daily_mix_index += 1
            scatter_index += 1
            top_index += 1

    return {
        "spend_vs_leads": spend_vs_leads,
        "top_leads": top_leads,
        "ratio_trends": ratio_trends,
        "cumulative": cumulative,
        "daily_mix": daily_mix,
        "top_n": top_n,
        "pacing_top_n": pacing_top_n,
        "ratio_top_n": ratio_top_n,
    }


async def fetch_brand_awareness_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build Brand Awareness chart payload bundle for each ads source."""
    sources = ("google", "facebook", "tiktok")
    tasks = []
    for source in sources:
        tasks.extend(
            [
                campaign_data.brand_awareness_spend_chart(source, start_date, end_date),
                campaign_data.brand_awareness_performance_chart(source, start_date, end_date),
            ]
        )
    results = await asyncio.gather(*tasks)
    payload: dict[str, object] = {}
    for index, source in enumerate(sources):
        base_index = index * 2
        payload[source] = {
            "spend": results[base_index],
            "performance": results[base_index + 1],
        }
    return payload


async def fetch_brand_awareness_details_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build Brand Awareness details payload for all ads sources."""
    sources = ("google", "facebook", "tiktok")
    results = await asyncio.gather(
        *[campaign_data.brand_awareness_details_table(source, start_date, end_date) for source in sources]
    )
    return {source: result for source, result in zip(sources, results)}


async def fetch_brand_awareness_insight_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build Brand Awareness CTR/CPM/CPC trend payload by source and dimension."""
    sources = ("google", "facebook", "tiktok")
    dimensions = ("campaign_id", "ad_group", "ad_name")
    ratio_metrics = ("ctr", "cpm", "cpc")
    ratio_top_n = 8

    ratio_tasks = []
    for source in sources:
        for dimension in dimensions:
            for metric in ratio_metrics:
                ratio_tasks.append(
                    campaign_data.brand_awareness_ratio_trend_chart(
                        data=source,
                        dimension=dimension,
                        metric=metric,
                        top_n=ratio_top_n,
                        from_date=start_date,
                        to_date=end_date,
                    )
                )

    ratio_results = await asyncio.gather(*ratio_tasks)

    ratio_trends: dict[str, dict[str, dict[str, object]]] = {source: {} for source in sources}
    ratio_index = 0
    for source in sources:
        for dimension in dimensions:
            ratio_trends[source][dimension] = {}
            for metric in ratio_metrics:
                ratio_trends[source][dimension][metric] = ratio_results[ratio_index]
                ratio_index += 1

    return {
        "ratio_trends": ratio_trends,
        "ratio_top_n": ratio_top_n,
    }


async def fetch_user_acquisition_overview_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build User Acquisition payload used by User Acquisition dashboard page.

    Args:
        campaign_data (CampaignData): Preloaded campaign data service instance.
        start_date (date): Start date for all requested aggregates/charts.
        end_date (date): End date for all requested aggregates/charts.

    Returns:
        dict[str, object]: User Acquisition payload with:
            - ``ads_metrics_with_growth`` KPI metrics and growth percentages.
            - ``leads_performance_charts`` multi-chart source performance.
            - ``ads_campaign_details`` raw rows for performance table.
    """
    (
        ads_metrics_growth,
        performance_charts,
        campaign_details_tables,
        insight_charts,
    ) = await asyncio.gather(
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
        fetch_user_acquisition_insight_charts_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
    )

    return {
        "ads_metrics_with_growth": ads_metrics_growth,
        "leads_performance_charts": performance_charts,
        "ads_campaign_details": campaign_details_tables,
        "ua_insight_charts": insight_charts,
    }


async def fetch_brand_awareness_overview_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build Brand Awareness payload used by Brand Awareness dashboard page."""
    brand_metrics, brand_charts, brand_details, brand_insight_charts = await asyncio.gather(
        fetch_brand_awareness_metrics_with_growth_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
        fetch_brand_awareness_charts_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
        fetch_brand_awareness_details_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
        fetch_brand_awareness_insight_charts_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
    )
    return {
        "metrics_with_growth": brand_metrics,
        "charts": brand_charts,
        "details": brand_details,
        "insight_charts": brand_insight_charts,
    }
