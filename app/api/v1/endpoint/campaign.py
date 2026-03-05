from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.functions.fetch_campaign import (
    fetch_campaign_overview_payload
)
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.campaign import ChartType
from app.utils.campaign_utils import CampaignData
from app.utils.user_utils import get_current_user

router = APIRouter()


@router.get("/api/campaign")
async def campaign_overview(
    start_date: date = Query(...),
    end_date: date = Query(...),
    chart: ChartType = Query("both"),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),  # noqa: ARG001
):
    """Return combined campaign overview payload for dashboard initial render."""
    try:
        if start_date > end_date:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="start_date cannot be after end_date.",
            )

        campaign_data = await CampaignData.load_data(
            session=session,
            from_date=start_date,
            to_date=end_date,
        )

        data = await fetch_campaign_overview_payload(
            campaign_data=campaign_data,
            chart=chart,
            start_date=start_date,
            end_date=end_date,
        )

        return JSONResponse(
            content={
                "success": True,
                "message": "Campaign overview generated.",
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
            detail=f"An error occurred while generating campaign overview: {error}",
        )
