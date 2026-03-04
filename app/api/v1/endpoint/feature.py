from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.utils.user_utils import get_current_user
from app.db.models.user import TfUser
from app.schemas.feature import UpdateData, UpdateDataResponse
from app.utils.api_utils import GoogleSheetApi, unique_campaign
from app.db.models.external_api import GoogleAds, FacebookAds, TikTokAds


router = APIRouter()


@router.post("/api/feature-data/update-external-api", response_model=UpdateDataResponse)
async def update_data(
    response: UpdateData,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user)
):
    """Run data update jobs for selected external source and date range.

    Args:
        response (UpdateData): Request payload containing source type and period.
        session (AsyncSession): Database session injected by FastAPI.
        current_user (TfUser): Authenticated user allowed to trigger update.

    Returns:
        JSONResponse: Message describing update status.

    Raises:
        HTTPException: Raised when source type is invalid or update process fails.
    """
    try:
        start_date = response.start_date
        end_date = response.end_date
        types = response.types
        gsheet = GoogleSheetApi()
        message = ""

        if response.data == "unique_campaign":
            message = await unique_campaign(session=session)
        elif response.data == "data_depo":
            message = await gsheet.data_depo(
                types=types,
                start_date=start_date, 
                end_date=end_date, 
                session=session)
        elif response.data == "google_ads":
            message = await gsheet.campaign_ads(
                types=types, 
                range_name="'Google Ads Campaign'!A:I", 
                start_date=start_date, end_date=end_date, 
                session=session, 
                classes=GoogleAds)
        elif response.data == "facebook_ads":
            message = await gsheet.campaign_ads(
                types=types, 
                range_name="'Meta Ads Campaign'!A:I", 
                start_date=start_date, 
                end_date=end_date, 
                session=session, 
                classes=FacebookAds)
        elif response.data == "tiktok_ads":
            message = await gsheet.campaign_ads(
                types=types, 
                range_name="'TikTok Ads Campaign'!A:I", 
                start_date=start_date, 
                end_date=end_date, 
                session=session, 
                classes=TikTokAds)
        else:
            raise HTTPException(
                status_code=404,
                detail="Please chose one data to update!"
            )
        
        if not message:
            raise HTTPException(
                status_code=404,
                detail="Something is error, data update is failed!"
            )
        return JSONResponse(
            content={
                "message": message
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while updating data: {str(e)}"
        )
