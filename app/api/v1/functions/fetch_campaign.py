"""Fetch Campaign module.

This module is part of `app.api.v1.functions` and contains runtime logic used by the
Traders Family application.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date
from typing import Any

from app.utils.campaign import CampaignData

ALL_SOURCES: tuple[str, ...] = ("google", "facebook", "tiktok")
REMARKETING_SOURCES: tuple[str, ...] = ("google", "facebook")
DEFAULT_DIMENSIONS: tuple[str, ...] = ("campaign_id", "ad_group", "ad_name")
UA_RATIO_METRICS: tuple[str, ...] = ("cost_per_lead", "click_per_lead", "click_through_lead")
BA_RATIO_METRICS: tuple[str, ...] = ("ctr", "cpm", "cpc")


@dataclass(frozen=True)
class PerSourceTaskSpec:
    """Declare one keyed task to be executed for every selected source."""

    key: str
    task_factory: callable


async def _run_source_map(
    *,
    sources: tuple[str, ...],
    task_factory,
) -> dict[str, object]:
    """Run one async task per source and map results back by source name."""
    results = await asyncio.gather(*[task_factory(source) for source in sources])
    return {source: result for source, result in zip(sources, results)}


async def _run_per_source_task_specs(
    *,
    sources: tuple[str, ...],
    task_specs: tuple[PerSourceTaskSpec, ...],
) -> dict[str, dict[str, object]]:
    """Run multiple keyed tasks per source and return `{source: {key: value}}`."""
    tasks = []
    for source in sources:
        for spec in task_specs:
            tasks.append(spec.task_factory(source))
    results = await asyncio.gather(*tasks)

    payload: dict[str, dict[str, object]] = {}
    index = 0
    for source in sources:
        payload[source] = {}
        for spec in task_specs:
            payload[source][spec.key] = results[index]
            index += 1
    return payload


async def _run_nested_metric_tasks(
    *,
    sources: tuple[str, ...],
    dimensions: tuple[str, ...],
    metrics: tuple[str, ...],
    task_factory,
) -> dict[str, dict[str, dict[str, object]]]:
    """Run tasks for each source/dimension/metric and return nested payload."""
    tasks = [
        task_factory(source, dimension, metric)
        for source in sources
        for dimension in dimensions
        for metric in metrics
    ]
    results = await asyncio.gather(*tasks)

    payload: dict[str, dict[str, dict[str, object]]] = {source: {} for source in sources}
    index = 0
    for source in sources:
        for dimension in dimensions:
            payload[source][dimension] = {}
            for metric in metrics:
                payload[source][dimension][metric] = results[index]
                index += 1
    return payload


async def _run_nested_dimension_tasks(
    *,
    sources: tuple[str, ...],
    dimensions: tuple[str, ...],
    task_specs: tuple[PerSourceTaskSpec, ...],
) -> dict[str, dict[str, dict[str, object]]]:
    """Run tasks for each source/dimension and return `{source: {key: {dim: value}}}`."""
    tasks = [
        spec.task_factory(source, dimension)
        for source in sources
        for dimension in dimensions
        for spec in task_specs
    ]
    results = await asyncio.gather(*tasks)

    payload: dict[str, dict[str, dict[str, object]]] = {
        source: {spec.key: {} for spec in task_specs}
        for source in sources
    }
    index = 0
    for source in sources:
        for dimension in dimensions:
            for spec in task_specs:
                payload[source][spec.key][dimension] = results[index]
                index += 1
    return payload


async def fetch_ads_metrics_with_growth_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    return await _run_source_map(
        sources=ALL_SOURCES,
        task_factory=lambda source: campaign_data.ads_metrics_with_growth(
            data=source,
            from_date=start_date,
            to_date=end_date,
            ad_type="user_acquisition",
        ),
    )


async def fetch_brand_awareness_metrics_with_growth_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    return await _run_source_map(
        sources=ALL_SOURCES,
        task_factory=lambda source: campaign_data.brand_awareness_metrics_with_growth(
            data=source,
            from_date=start_date,
            to_date=end_date,
        ),
    )


async def fetch_leads_performance_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    task_specs = (
        PerSourceTaskSpec(
            key="cost_to_leads",
            task_factory=lambda source: campaign_data.cost_to_leads_chart(source, start_date, end_date),
        ),
        PerSourceTaskSpec(
            key="leads_by_periods",
            task_factory=lambda source: campaign_data.leads_by_periods_chart(source, start_date, end_date),
        ),
        PerSourceTaskSpec(
            key="clicks_to_leads",
            task_factory=lambda source: campaign_data.clicks_to_leads_chart(source, start_date, end_date),
        ),
    )
    return await _run_per_source_task_specs(sources=ALL_SOURCES, task_specs=task_specs)


async def fetch_ads_campaign_details_tables_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    return await _run_source_map(
        sources=ALL_SOURCES,
        task_factory=lambda source: campaign_data.ads_campaign_details_table(
            source,
            start_date,
            end_date,
            ad_type="user_acquisition",
        ),
    )


async def fetch_user_acquisition_insight_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    top_n = 10
    pacing_top_n = 8
    ratio_top_n = 8

    ratio_trends_task = _run_nested_metric_tasks(
        sources=ALL_SOURCES,
        dimensions=DEFAULT_DIMENSIONS,
        metrics=UA_RATIO_METRICS,
        task_factory=lambda source, dimension, metric: campaign_data.user_acquisition_ratio_trend_chart(
            data=source,
            dimension=dimension,
            metric=metric,
            top_n=ratio_top_n,
            from_date=start_date,
            to_date=end_date,
        ),
    )

    dimension_task_specs = (
        PerSourceTaskSpec(
            key="spend_vs_leads",
            task_factory=lambda source, dimension: campaign_data.user_acquisition_spend_vs_leads_chart(
                data=source,
                dimension=dimension,
                from_date=start_date,
                to_date=end_date,
            ),
        ),
        PerSourceTaskSpec(
            key="top_leads",
            task_factory=lambda source, dimension: campaign_data.user_acquisition_top_leads_chart(
                data=source,
                dimension=dimension,
                top_n=top_n,
                from_date=start_date,
                to_date=end_date,
            ),
        ),
        PerSourceTaskSpec(
            key="cumulative",
            task_factory=lambda source, dimension: campaign_data.user_acquisition_cumulative_chart(
                data=source,
                dimension=dimension,
                top_n=pacing_top_n,
                from_date=start_date,
                to_date=end_date,
            ),
        ),
        PerSourceTaskSpec(
            key="daily_mix",
            task_factory=lambda source, dimension: campaign_data.user_acquisition_daily_mix_chart(
                data=source,
                dimension=dimension,
                top_n=pacing_top_n,
                from_date=start_date,
                to_date=end_date,
            ),
        ),
    )
    dimension_payload_task = _run_nested_dimension_tasks(
        sources=ALL_SOURCES,
        dimensions=DEFAULT_DIMENSIONS,
        task_specs=dimension_task_specs,
    )

    ratio_trends, dimension_payload = await asyncio.gather(ratio_trends_task, dimension_payload_task)
    return {
        "spend_vs_leads": {source: dimension_payload[source]["spend_vs_leads"] for source in ALL_SOURCES},
        "top_leads": {source: dimension_payload[source]["top_leads"] for source in ALL_SOURCES},
        "ratio_trends": ratio_trends,
        "cumulative": {source: dimension_payload[source]["cumulative"] for source in ALL_SOURCES},
        "daily_mix": {source: dimension_payload[source]["daily_mix"] for source in ALL_SOURCES},
        "top_n": top_n,
        "pacing_top_n": pacing_top_n,
        "ratio_top_n": ratio_top_n,
    }


async def fetch_brand_awareness_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    task_specs = (
        PerSourceTaskSpec(
            key="spend",
            task_factory=lambda source: campaign_data.brand_awareness_spend_chart(source, start_date, end_date),
        ),
        PerSourceTaskSpec(
            key="performance",
            task_factory=lambda source: campaign_data.brand_awareness_performance_chart(source, start_date, end_date),
        ),
    )
    return await _run_per_source_task_specs(sources=ALL_SOURCES, task_specs=task_specs)


async def fetch_brand_awareness_details_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    return await _run_source_map(
        sources=ALL_SOURCES,
        task_factory=lambda source: campaign_data.brand_awareness_details_table(source, start_date, end_date),
    )


async def fetch_brand_awareness_insight_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    ratio_top_n = 8
    ratio_trends = await _run_nested_metric_tasks(
        sources=ALL_SOURCES,
        dimensions=DEFAULT_DIMENSIONS,
        metrics=BA_RATIO_METRICS,
        task_factory=lambda source, dimension, metric: campaign_data.brand_awareness_ratio_trend_chart(
            data=source,
            dimension=dimension,
            metric=metric,
            top_n=ratio_top_n,
            from_date=start_date,
            to_date=end_date,
        ),
    )
    return {
        "ratio_trends": ratio_trends,
        "ratio_top_n": ratio_top_n,
    }


async def fetch_user_acquisition_overview_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
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


async def fetch_remarketing_metrics_with_growth_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    return await _run_source_map(
        sources=REMARKETING_SOURCES,
        task_factory=lambda source: campaign_data.remarketing_metrics_with_growth(
            data=source,
            from_date=start_date,
            to_date=end_date,
        ),
    )


async def fetch_remarketing_performance_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    task_specs = (
        PerSourceTaskSpec(
            key="spend",
            task_factory=lambda source: campaign_data.remarketing_spend_chart(
                source,
                start_date,
                end_date,
            ),
        ),
        PerSourceTaskSpec(
            key="performance",
            task_factory=lambda source: campaign_data.remarketing_performance_chart(
                source,
                start_date,
                end_date,
            ),
        ),
    )
    return await _run_per_source_task_specs(sources=REMARKETING_SOURCES, task_specs=task_specs)


async def fetch_remarketing_details_tables_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    return await _run_source_map(
        sources=REMARKETING_SOURCES,
        task_factory=lambda source: campaign_data.remarketing_details_table(
            source,
            start_date,
            end_date,
        ),
    )


async def fetch_remarketing_insight_charts_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    ratio_top_n = 8
    ratio_trends = await _run_nested_metric_tasks(
        sources=REMARKETING_SOURCES,
        dimensions=DEFAULT_DIMENSIONS,
        metrics=BA_RATIO_METRICS,
        task_factory=lambda source, dimension, metric: campaign_data.remarketing_ratio_trend_chart(
            data=source,
            dimension=dimension,
            metric=metric,
            top_n=ratio_top_n,
            from_date=start_date,
            to_date=end_date,
        ),
    )

    return {
        "ratio_trends": ratio_trends,
        "ratio_top_n": ratio_top_n,
    }


async def fetch_remarketing_overview_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    (
        metrics_growth,
        performance_charts,
        details_tables,
        insight_charts,
    ) = await asyncio.gather(
        fetch_remarketing_metrics_with_growth_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
        fetch_remarketing_performance_charts_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
        fetch_remarketing_details_tables_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
        fetch_remarketing_insight_charts_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        ),
    )
    return {
        "metrics_with_growth": metrics_growth,
        "charts": performance_charts,
        "details": details_tables,
        "insight_charts": insight_charts,
    }


async def fetch_brand_awareness_overview_payload(
    campaign_data: CampaignData,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
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
