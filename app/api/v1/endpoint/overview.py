"""HTTP endpoints for the Overview dashboard.

This module exposes read-only API handlers that assemble overview analytics for:
- Firebase active users
- Campaign ad cost spend
- Campaign user acquisition
- Campaign brand awareness

Each handler validates period boundaries, loads the corresponding service object,
and returns a normalized JSON payload consumed by Streamlit FE.
"""

from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.functions.fetch_overview import fetch_overview_active_users_payload
from app.api.v1.functions.fetch_overview_brand import fetch_overview_brand_awareness_payload
from app.api.v1.functions.fetch_overview_campaign import fetch_overview_campaign_cost_payload
from app.api.v1.functions.fetch_overview_leads import fetch_overview_leads_acquisition_payload
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.overview_utils import (
    OverviewBrandAwarenessData,
    OverviewCampaignCostData,
    OverviewData,
    OverviewLeadsAcquisitionData,
)
from app.utils.user_utils import get_current_user

router = APIRouter()


async def _build_overview_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    source: str,
) -> OverviewData:
    """Create preloaded active-user overview service for a valid date range.

    Raises:
        HTTPException: If ``start_date`` is after ``end_date``.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date cannot be after end_date.",
        )
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
    """Create preloaded campaign-cost overview service for a valid date range.

    Raises:
        HTTPException: If ``start_date`` is after ``end_date``.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date cannot be after end_date.",
        )
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
    """Create preloaded leads-acquisition overview service for a valid date range.

    Raises:
        HTTPException: If ``start_date`` is after ``end_date``.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date cannot be after end_date.",
        )
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
    """Create preloaded brand-awareness overview service for a valid date range.

    Raises:
        HTTPException: If ``start_date`` is after ``end_date``.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date cannot be after end_date.",
        )
    return await OverviewBrandAwarenessData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


@router.get("/api/overview/active-users")
async def overview_active_users(
    start_date: date = Query(...),
    end_date: date = Query(...),
    source: Literal["app", "web"] = Query(default="app"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Return active-user overview payload for the selected source and period.

    Query Params:
        start_date: Inclusive period start date.
        end_date: Inclusive period end date.
        source: GA4 source bucket (``app`` or ``web``).
    """
    try:
        overview_data = await _build_overview_data(
            session=session,
            start_date=start_date,
            end_date=end_date,
            source=source,
        )
        data = await fetch_overview_active_users_payload(
            overview_data=overview_data,
            start_date=start_date,
            end_date=end_date,
        )
        return JSONResponse(
            content={
                "success": True,
                "message": "Overview active users generated.",
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
            detail=f"An error occurred while generating overview active users: {error}",
        )


@router.get("/api/overview/campaign-cost")
async def overview_campaign_cost(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Return campaign-cost overview payload for the selected period.

    The payload includes metric cards with period growth and pie-chart
    breakdowns by campaign type and platform.
    """
    try:
        campaign_cost_data = await _build_overview_campaign_cost_data(
            session=session,
            start_date=start_date,
            end_date=end_date,
        )
        data = await fetch_overview_campaign_cost_payload(
            campaign_cost_data=campaign_cost_data,
            start_date=start_date,
            end_date=end_date,
        )
        return JSONResponse(
            content={
                "success": True,
                "message": "Overview campaign cost generated.",
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
            detail=f"An error occurred while generating overview campaign cost: {error}",
        )


@router.get("/api/overview/leads-acquisition")
async def overview_leads_acquisition(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Return leads-acquisition overview payload for the selected period.

    The payload includes KPI cards with growth, leads-by-source table and pie,
    plus daily trend charts.
    """
    try:
        leads_data = await _build_overview_leads_data(
            session=session,
            start_date=start_date,
            end_date=end_date,
        )
        data = await fetch_overview_leads_acquisition_payload(
            leads_data=leads_data,
            start_date=start_date,
            end_date=end_date,
        )
        return JSONResponse(
            content={
                "success": True,
                "message": "Overview leads acquisition generated.",
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
            detail=f"An error occurred while generating overview leads acquisition: {error}",
        )


@router.get("/api/overview/brand-awareness")
async def overview_brand_awareness(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Return brand-awareness overview payload for the selected period.

    The payload includes KPI cards with growth, daily spend chart, and a mixed
    performance chart (impressions/clicks bars + CTR/CPM/CPC lines).
    """
    try:
        brand_data = await _build_overview_brand_data(
            session=session,
            start_date=start_date,
            end_date=end_date,
        )
        data = await fetch_overview_brand_awareness_payload(
            brand_data=brand_data,
            start_date=start_date,
            end_date=end_date,
        )
        return JSONResponse(
            content={
                "success": True,
                "message": "Overview brand awareness generated.",
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
            detail=f"An error occurred while generating overview brand awareness: {error}",
        )
