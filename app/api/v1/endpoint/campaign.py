"""Campaign module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

import logging
from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import (
    build_analytics_response,
    require_roles_dep,
    validate_date_range,
)
from app.api.v1.functions.fetch_campaign import (
    fetch_brand_awareness_overview_payload,
    fetch_remarketing_overview_payload,
    fetch_user_acquisition_overview_payload,
)
from app.api.v1.functions.fetch_internal_register import fetch_internal_register_payload
from app.api.v1.functions.fetch_login_activity import fetch_login_activity_payload
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.responses import AnalyticsResponse
from app.utils.campaign import CampaignData
from app.utils.rbac import ANALYTICS_ROLES, FINANCE_ANALYTICS_ROLES

router = APIRouter()
logger = logging.getLogger(__name__)


async def _build_campaign_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> CampaignData:
    """Validate date input and preload campaign analytics service."""
    validate_date_range(start_date, end_date)
    return await CampaignData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


async def _load_user_acquisition_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    campaign_data = await _build_campaign_data(
        session=session,
        start_date=start_date,
        end_date=end_date,
    )
    return await fetch_user_acquisition_overview_payload(
        campaign_data=campaign_data,
        start_date=start_date,
        end_date=end_date,
    )


async def _load_brand_awareness_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    campaign_data = await _build_campaign_data(
        session=session,
        start_date=start_date,
        end_date=end_date,
    )
    return await fetch_brand_awareness_overview_payload(
        campaign_data=campaign_data,
        start_date=start_date,
        end_date=end_date,
    )


async def _load_remarketing_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    campaign_data = await _build_campaign_data(
        session=session,
        start_date=start_date,
        end_date=end_date,
    )
    return await fetch_remarketing_overview_payload(
        campaign_data=campaign_data,
        start_date=start_date,
        end_date=end_date,
    )


async def _load_internal_register_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    source: str,
) -> dict[str, object]:
    validate_date_range(start_date, end_date)
    return await fetch_internal_register_payload(
        session=session,
        start_date=start_date,
        end_date=end_date,
        source=source,
    )


async def _load_login_activity_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    source: str,
) -> dict[str, object]:
    validate_date_range(start_date, end_date)
    return await fetch_login_activity_payload(
        session=session,
        start_date=start_date,
        end_date=end_date,
        source=source,
    )


@router.get("/api/campaign/user-acquisition", response_model=AnalyticsResponse)
async def user_acquisition_overview(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*FINANCE_ANALYTICS_ROLES)),  # noqa: ARG001
):
    """Generate User Acquisition payload for dashboard rendering."""
    return await build_analytics_response(
        loader=lambda: _load_user_acquisition_payload(session, start_date, end_date),
        success_message="User acquisition overview generated.",
        logger=logger,
        failure_log_message="Failed to generate user acquisition overview payload",
        failure_detail_message="An internal error occurred while generating user acquisition overview.",
    )


@router.get("/api/campaign/brand-awareness", response_model=AnalyticsResponse)
async def brand_awareness_overview(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*FINANCE_ANALYTICS_ROLES)),  # noqa: ARG001
):
    """Generate Brand Awareness payload for dashboard rendering."""
    return await build_analytics_response(
        loader=lambda: _load_brand_awareness_payload(session, start_date, end_date),
        success_message="Brand awareness overview generated.",
        logger=logger,
        failure_log_message="Failed to generate brand awareness overview payload",
        failure_detail_message="An internal error occurred while generating brand awareness overview.",
    )


@router.get("/api/campaign/remarketing", response_model=AnalyticsResponse)
async def remarketing_overview(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*FINANCE_ANALYTICS_ROLES)),  # noqa: ARG001
):
    """Generate Remarketing payload for dashboard rendering."""
    return await build_analytics_response(
        loader=lambda: _load_remarketing_payload(session, start_date, end_date),
        success_message="Remarketing overview generated.",
        logger=logger,
        failure_log_message="Failed to generate remarketing overview payload",
        failure_detail_message="An internal error occurred while generating remarketing overview.",
    )


@router.get("/api/campaign/internal-register", response_model=AnalyticsResponse)
async def internal_register_overview(
    start_date: date = Query(...),
    end_date: date = Query(...),
    source: str = Query("all"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES)),  # noqa: ARG001
):
    """Generate Internal Register payload for dashboard rendering."""
    return await build_analytics_response(
        loader=lambda: _load_internal_register_payload(session, start_date, end_date, source),
        success_message="Internal register overview generated.",
        logger=logger,
        failure_log_message="Failed to generate internal register overview payload",
        failure_detail_message="An internal error occurred while generating internal register overview.",
    )


@router.get("/api/campaign/login-activity", response_model=AnalyticsResponse)
async def login_activity_overview(
    start_date: date = Query(...),
    end_date: date = Query(...),
    source: str = Query("all"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES)),  # noqa: ARG001
):
    """Generate Login activity payload for dashboard rendering."""
    return await build_analytics_response(
        loader=lambda: _load_login_activity_payload(session, start_date, end_date, source),
        success_message="Login activity overview generated.",
        logger=logger,
        failure_log_message="Failed to generate login activity overview payload",
        failure_detail_message="An internal error occurred while generating login activity overview.",
    )
