"""Campaign module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.functions.fetch_campaign import (
    fetch_brand_awareness_overview_payload,
    fetch_user_acquisition_overview_payload,
)
from app.api.v1.functions.fetch_internal_register import fetch_internal_register_payload
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.campaign import CampaignData
from app.utils.user_utils import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)


async def _build_campaign_data(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> CampaignData:
    """Validate date input and preload campaign analytics service.

    Args:
        session (AsyncSession): Injected asynchronous DB session.
        start_date (date): Inclusive report start date.
        end_date (date): Inclusive report end date.

    Returns:
        CampaignData: Preloaded service instance for campaign analytics.

    Raises:
        HTTPException: Raised with ``400`` when date range is invalid.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date cannot be after end_date.",
        )
    return await CampaignData.load_data(
        session=session,
        from_date=start_date,
        to_date=end_date,
    )


@router.get("/api/campaign/user-acquisition")
async def user_acquisition_overview(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Generate User Acquisition payload for dashboard rendering.

    Args:
        start_date (date): Start of requested reporting window (inclusive).
        end_date (date): End of requested reporting window (inclusive).
        session (AsyncSession): Injected asynchronous database session.
        current_user (TfUser): Authenticated user resolved from access token.

    Returns:
        JSONResponse: Success response containing:
            - ``success`` (bool): ``True`` when payload creation succeeds.
            - ``message`` (str): Human-readable success message.
            - ``data`` (dict[str, object]): Aggregated campaign overview payload.

    Raises:
        HTTPException: ``400`` when date range is invalid or validation fails.
        HTTPException: ``500`` when unexpected server-side processing fails.
    """
    try:
        campaign_data = await _build_campaign_data(
            session=session,
            start_date=start_date,
            end_date=end_date,
        )

        data = await fetch_user_acquisition_overview_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        )

        return JSONResponse(
            content={
                "success": True,
                "message": "User acquisition overview generated.",
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
    except Exception:
        logger.exception("Failed to generate user acquisition overview payload")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An internal error occurred while generating user acquisition overview.",
        )


@router.get("/api/campaign/brand-awareness")
async def brand_awareness_overview(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Generate Brand Awareness payload for dashboard rendering.

    Args:
        start_date (date): Start of requested reporting window (inclusive).
        end_date (date): End of requested reporting window (inclusive).
        session (AsyncSession): Injected asynchronous database session.
        current_user (TfUser): Authenticated user resolved from access token.

    Returns:
        JSONResponse: Success response with aggregated Brand Awareness data.

    Raises:
        HTTPException: ``400`` for validation errors.
        HTTPException: ``500`` for unexpected processing failures.
    """
    try:
        campaign_data = await _build_campaign_data(
            session=session,
            start_date=start_date,
            end_date=end_date,
        )

        data = await fetch_brand_awareness_overview_payload(
            campaign_data=campaign_data,
            start_date=start_date,
            end_date=end_date,
        )

        return JSONResponse(
            content={
                "success": True,
                "message": "Brand awareness overview generated.",
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
    except Exception:
        logger.exception("Failed to generate brand awareness overview payload")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An internal error occurred while generating brand awareness overview.",
        )


@router.get("/api/campaign/internal-register")
async def internal_register_overview(
    start_date: date = Query(...),
    end_date: date = Query(...),
    source: str = Query("all"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Generate Internal Register payload for dashboard rendering."""
    try:
        if start_date > end_date:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="start_date cannot be after end_date.",
            )
        data = await fetch_internal_register_payload(
            session=session,
            start_date=start_date,
            end_date=end_date,
            source=source,
        )
        return JSONResponse(
            content={
                "success": True,
                "message": "Internal register overview generated.",
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
    except Exception:
        logger.exception("Failed to generate internal register overview payload")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An internal error occurred while generating internal register overview.",
        )
