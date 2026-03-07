"""Deposit module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.functions.fetch_deposit import fetch_deposit_daily_overview_payload
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.deposit_utils import DepositData
from app.utils.user_utils import get_current_user

router = APIRouter()


async def _build_deposit_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> DepositData:
    """Validate date range then preload deposit data service.

    Args:
        session (AsyncSession): Injected asynchronous DB session.
        start_date (date): Inclusive start date for initial data load.
        end_date (date): Inclusive end date for initial data load.

    Returns:
        DepositData: Ready-to-use service instance containing preloaded rows.

    Raises:
        HTTPException: Raised with ``400`` status when date order is invalid.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date cannot be after end_date.",
        )
    return await DepositData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


@router.get("/api/deposit/daily-report")
async def deposit_daily_report(
    start_date: date = Query(...),
    end_date: date = Query(...),
    campaign_type: Literal["all", "user_acquisition", "brand_awareness"] = Query(default="all"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Generate daily deposit cross-tab report payload.

    Args:
        start_date (date): Inclusive start date from query parameters.
        end_date (date): Inclusive end date from query parameters.
        campaign_type (Literal["all", "user_acquisition", "brand_awareness"]):
            Campaign type selector for report filtering.
        session (AsyncSession): Injected asynchronous DB session.
        current_user (TfUser): Authenticated user resolved from access token.

    Returns:
        JSONResponse: Success payload containing report metadata and
        aggregated daily deposit table structure.

    Raises:
        HTTPException: ``400`` for invalid input/date validation errors.
        HTTPException: ``500`` for unexpected internal processing errors.
    """
    try:
        deposit_data = await _build_deposit_data(
            session=session,
            start_date=start_date,
            end_date=end_date,
        )
        selected_type = None if campaign_type == "all" else campaign_type
        data = await fetch_deposit_daily_overview_payload(
            deposit_data=deposit_data,
            start_date=start_date,
            end_date=end_date,
            campaign_type=selected_type,
        )
        return JSONResponse(
            content={
                "success": True,
                "message": "Deposit daily report generated.",
                "data": data,
            }
        )
    except HTTPException:
        raise
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        )
    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while generating deposit daily report: {error}",
        )
