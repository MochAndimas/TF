from datetime import datetime, timedelta
from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.utils.user_utils import get_current_user
from app.db.models.user import TfUser
from app.schemas.feature import UpdateData
from app.utils.api_utils import GoogleSheetApi, unique_campaign
from app.db.models.external_api import GoogleAds, FacebookAds, TikTokAds


router = APIRouter()


@router.post("/api/feature-data/update-external-api", response_model=UpdateData)
async def update_data(
    response: UpdateData,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user)
):
    """
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
                range_name="'Data Depo RAW'!A:F", 
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
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"an error occured: {str(e)}"
        )
