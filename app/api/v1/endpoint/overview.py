"""HTTP endpoints for the Overview dashboard."""

import logging
from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import (
    build_analytics_response,
    require_roles_dep,
    validate_date_range,
)
from app.api.v1.functions.fetch_overview import fetch_overview_active_users_payload
from app.api.v1.functions.fetch_overview_brand import fetch_overview_brand_awareness_payload
from app.api.v1.functions.fetch_overview_campaign import fetch_overview_campaign_cost_payload
from app.api.v1.functions.fetch_overview_leads import fetch_overview_leads_acquisition_payload
from app.api.v1.functions.fetch_overview_remarketing import fetch_overview_remarketing_payload
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.responses import AnalyticsResponse
from app.utils.overview import (
    OverviewBrandAwarenessData,
    OverviewCampaignCostData,
    OverviewData,
    OverviewLeadsAcquisitionData,
    OverviewRemarketingPerformanceData,
)
from app.utils.rbac import ANALYTICS_ROLES

router = APIRouter()
logger = logging.getLogger(__name__)


async def _build_overview_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    source: str,
) -> OverviewData:
    validate_date_range(start_date, end_date)
    return await OverviewData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
        source=source,
    )


async def _build_overview_campaign_cost_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> OverviewCampaignCostData:
    validate_date_range(start_date, end_date)
    return await OverviewCampaignCostData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


async def _build_overview_leads_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> OverviewLeadsAcquisitionData:
    validate_date_range(start_date, end_date)
    return await OverviewLeadsAcquisitionData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


async def _build_overview_brand_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> OverviewBrandAwarenessData:
    validate_date_range(start_date, end_date)
    return await OverviewBrandAwarenessData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


async def _build_overview_remarketing_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> OverviewRemarketingPerformanceData:
    validate_date_range(start_date, end_date)
    return await OverviewRemarketingPerformanceData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


async def _load_active_users_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    source: str,
) -> dict[str, object]:
    overview_data = await _build_overview_data(session, start_date, end_date, source)
    return await fetch_overview_active_users_payload(
        overview_data=overview_data,
        start_date=start_date,
        end_date=end_date,
    )


async def _load_campaign_cost_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    campaign_cost_data = await _build_overview_campaign_cost_data(session, start_date, end_date)
    return await fetch_overview_campaign_cost_payload(
        campaign_cost_data=campaign_cost_data,
        start_date=start_date,
        end_date=end_date,
    )


async def _load_leads_acquisition_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    leads_data = await _build_overview_leads_data(session, start_date, end_date)
    return await fetch_overview_leads_acquisition_payload(
        leads_data=leads_data,
        start_date=start_date,
        end_date=end_date,
    )


async def _load_brand_awareness_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    brand_data = await _build_overview_brand_data(session, start_date, end_date)
    return await fetch_overview_brand_awareness_payload(
        brand_data=brand_data,
        start_date=start_date,
        end_date=end_date,
    )


async def _load_remarketing_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    remarketing_data = await _build_overview_remarketing_data(session, start_date, end_date)
    return await fetch_overview_remarketing_payload(
        remarketing_data=remarketing_data,
        start_date=start_date,
        end_date=end_date,
    )


@router.get("/api/overview/active-users", response_model=AnalyticsResponse)
async def overview_active_users(
    start_date: date = Query(...),
    end_date: date = Query(...),
    source: Literal["app", "web", "app_web"] = Query(default="app"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES, "finance")),  # noqa: ARG001
):
    """Return active-user overview payload for the selected source and period."""
    return await build_analytics_response(
        loader=lambda: _load_active_users_payload(session, start_date, end_date, source),
        success_message="Overview active users generated.",
        logger=logger,
        failure_log_message="Failed to generate overview active users payload",
        failure_detail_message="An internal error occurred while generating overview active users.",
    )


@router.get("/api/overview/campaign-cost", response_model=AnalyticsResponse)
async def overview_campaign_cost(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES, "finance")),  # noqa: ARG001
):
    """Return campaign-cost overview payload for the selected period."""
    return await build_analytics_response(
        loader=lambda: _load_campaign_cost_payload(session, start_date, end_date),
        success_message="Overview campaign cost generated.",
        logger=logger,
        failure_log_message="Failed to generate overview campaign cost payload",
        failure_detail_message="An internal error occurred while generating overview campaign cost.",
    )


@router.get("/api/overview/leads-acquisition", response_model=AnalyticsResponse)
async def overview_leads_acquisition(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES, "finance")),  # noqa: ARG001
):
    """Return leads-acquisition overview payload for the selected period."""
    return await build_analytics_response(
        loader=lambda: _load_leads_acquisition_payload(session, start_date, end_date),
        success_message="Overview leads acquisition generated.",
        logger=logger,
        failure_log_message="Failed to generate overview leads acquisition payload",
        failure_detail_message="An internal error occurred while generating overview leads acquisition.",
    )


@router.get("/api/overview/brand-awareness", response_model=AnalyticsResponse)
async def overview_brand_awareness(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES, "finance")),  # noqa: ARG001
):
    """Return brand-awareness overview payload for the selected period."""
    return await build_analytics_response(
        loader=lambda: _load_brand_awareness_payload(session, start_date, end_date),
        success_message="Overview brand awareness generated.",
        logger=logger,
        failure_log_message="Failed to generate overview brand awareness payload",
        failure_detail_message="An internal error occurred while generating overview brand awareness.",
    )


@router.get("/api/overview/remarketing", response_model=AnalyticsResponse)
async def overview_remarketing(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES, "finance")),  # noqa: ARG001
):
    """Return remarketing overview payload sourced from ``data_ms_deposit``."""
    return await build_analytics_response(
        loader=lambda: _load_remarketing_payload(session, start_date, end_date),
        success_message="Overview remarketing generated.",
        logger=logger,
        failure_log_message="Failed to generate overview remarketing payload",
        failure_detail_message="An internal error occurred while generating overview remarketing.",
    )
