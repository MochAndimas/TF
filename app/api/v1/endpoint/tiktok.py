"""TikTok analytics endpoints."""

from __future__ import annotations

from datetime import date
import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import build_analytics_response, require_roles_dep, validate_date_range
from app.api.v1.functions.fetch_tiktok import fetch_tiktok_analytics_payload
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.responses import AnalyticsResponse
from app.utils.rbac import SOCMED_ANALYTICS_ROLES

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/api/tiktok/analytics", response_model=AnalyticsResponse)
async def tiktok_analytics(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*SOCMED_ANALYTICS_ROLES)),  # noqa: ARG001
):
    """Generate TikTok analytics payload for dashboard rendering."""
    validate_date_range(start_date, end_date)
    return await build_analytics_response(
        loader=lambda: fetch_tiktok_analytics_payload(
            session=session,
            start_date=start_date,
            end_date=end_date,
        ),
        success_message="TikTok analytics generated.",
        logger=logger,
        failure_log_message="Failed to generate TikTok analytics payload",
        failure_detail_message="An internal error occurred while generating TikTok analytics.",
    )
