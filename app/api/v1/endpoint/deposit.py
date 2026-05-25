"""Deposit module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

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
from app.api.v1.functions.fetch_deposit import fetch_deposit_daily_overview_payload
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.responses import AnalyticsResponse
from app.utils.deposit_utils import DepositData
from app.utils.rbac import FINANCE_ANALYTICS_ROLES
from app.utils.remarketing_deposit_utils import RemarketingDepositData

router = APIRouter()
logger = logging.getLogger(__name__)


async def _build_deposit_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> DepositData:
    """Validate date range then preload deposit data service."""
    validate_date_range(start_date, end_date)
    return await DepositData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


async def _build_remarketing_deposit_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> RemarketingDepositData:
    """Validate date range then preload remarketing deposit data service."""
    validate_date_range(start_date, end_date)
    return await RemarketingDepositData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


async def _load_deposit_daily_report_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    campaign_type: Literal["all", "user_acquisition", "brand_awareness"],
) -> dict[str, object]:
    deposit_data = await _build_deposit_data(
        session=session,
        start_date=start_date,
        end_date=end_date,
    )
    selected_type = None if campaign_type == "all" else campaign_type
    return await fetch_deposit_daily_overview_payload(
        deposit_data=deposit_data,
        start_date=start_date,
        end_date=end_date,
        campaign_type=selected_type,
    )


async def _load_remarketing_report_payload(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    campaign_type: Literal["all", "user_acquisition", "brand_awareness", "remarketing"],
) -> dict[str, object]:
    deposit_data = await _build_remarketing_deposit_data(
        session=session,
        start_date=start_date,
        end_date=end_date,
    )
    selected_type = None if campaign_type == "all" else campaign_type
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "report": await deposit_data.build_daily_report_payload(campaign_type=selected_type),
    }


@router.get("/api/deposit/daily-report", response_model=AnalyticsResponse)
async def deposit_daily_report(
    start_date: date = Query(...),
    end_date: date = Query(...),
    campaign_type: Literal["all", "user_acquisition", "brand_awareness"] = Query(default="all"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*FINANCE_ANALYTICS_ROLES)),  # noqa: ARG001
):
    """Generate daily deposit report payload."""
    return await build_analytics_response(
        loader=lambda: _load_deposit_daily_report_payload(session, start_date, end_date, campaign_type),
        success_message="Deposit daily report generated.",
        logger=logger,
        failure_log_message="Failed to generate deposit daily report payload",
        failure_detail_message="An internal error occurred while generating deposit daily report.",
    )


@router.get("/api/deposit/remarketing-report", response_model=AnalyticsResponse)
async def remarketing_deposit_report(
    start_date: date = Query(...),
    end_date: date = Query(...),
    campaign_type: Literal["all", "user_acquisition", "brand_awareness", "remarketing"] = Query(default="all"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*FINANCE_ANALYTICS_ROLES)),  # noqa: ARG001
):
    """Generate remarketing deposit report payload from ``data_ms_deposit``."""
    return await build_analytics_response(
        loader=lambda: _load_remarketing_report_payload(session, start_date, end_date, campaign_type),
        success_message="Remarketing deposit report generated.",
        logger=logger,
        failure_log_message="Failed to generate remarketing deposit report payload",
        failure_detail_message="An internal error occurred while generating remarketing deposit report.",
    )
