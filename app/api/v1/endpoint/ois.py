"""Protected partnership analytics using persisted GA4 acquisition facts."""
import logging
from datetime import date
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.v1.endpoint.common import build_analytics_response, require_roles_dep
from app.api.v1.functions.fetch_ois import fetch_ois_payload, fetch_ois_registrations
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.responses import AnalyticsResponse
from app.utils.rbac import ANALYTICS_ROLES

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/api/ois/analytics", response_model=AnalyticsResponse)
async def ois_analytics(
    start_date: date = Query(...), end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES)),
):
    return await build_analytics_response(
        loader=lambda: fetch_ois_payload(session, start_date, end_date),
        success_message="OIS analytics generated.", logger=logger,
        failure_log_message="Failed to generate OIS analytics",
        failure_detail_message="Unable to load OIS analytics.",
    )


@router.get("/api/ois/registrations", response_model=AnalyticsResponse)
async def ois_registrations(
    start_date: date = Query(...), end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES)),
):
    return await build_analytics_response(
        loader=lambda: fetch_ois_registrations(session, start_date, end_date),
        success_message="OIS register and revenue generated.", logger=logger,
        failure_log_message="Failed to generate OIS register and revenue",
        failure_detail_message="Unable to load OIS register and revenue.",
    )
