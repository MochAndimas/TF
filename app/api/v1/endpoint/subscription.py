"""Protected subscription revenue reporting."""
import logging
from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import build_analytics_response, require_roles_dep
from app.api.v1.functions.fetch_subscription import fetch_subscription_payload
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.responses import AnalyticsResponse
from app.utils.rbac import FINANCE_ANALYTICS_ROLES

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/api/subscription/analytics", response_model=AnalyticsResponse)
async def subscription_analytics(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*FINANCE_ANALYTICS_ROLES)),
):
    return await build_analytics_response(
        loader=lambda: fetch_subscription_payload(session, start_date, end_date),
        success_message="Subscription analytics generated.",
        logger=logger,
        failure_log_message="Failed to generate subscription analytics",
        failure_detail_message="An internal error occurred while generating subscription analytics.",
    )
