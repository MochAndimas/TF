"""Google Play Console install analytics endpoints."""

from __future__ import annotations

from datetime import date
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import build_analytics_response, validate_date_range
from app.api.v1.functions.fetch_install import fetch_install_analytics_payload
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.responses import AnalyticsResponse
from app.utils.user_utils import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)
INSTALL_ANALYTICS_ROLES = {"superadmin", "analyst", "digital_marketing", "tech_it"}


async def require_install_analytics_role(current_user: TfUser = Depends(get_current_user)) -> TfUser:
    """Require exact roles that may access the Install analytics page."""
    role = (current_user.role or "").strip().lower()
    if role not in INSTALL_ANALYTICS_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this resource.",
        )
    return current_user


@router.get("/api/install/analytics", response_model=AnalyticsResponse)
async def install_analytics(
    start_date: date = Query(...),
    end_date: date = Query(...),
    package_name: str = Query(default="all"),
    country: str = Query(default="all"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_install_analytics_role),  # noqa: ARG001
):
    """Generate Google Play Console install analytics payload."""
    validate_date_range(start_date, end_date)
    return await build_analytics_response(
        loader=lambda: fetch_install_analytics_payload(
            session=session,
            start_date=start_date,
            end_date=end_date,
            package_name=package_name,
            country=country,
        ),
        success_message="Install analytics generated.",
        logger=logger,
        failure_log_message="Failed to generate install analytics payload",
        failure_detail_message="An internal error occurred while generating install analytics.",
    )
