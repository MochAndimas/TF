from datetime import datetime, timedelta
from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.utils.user_utils import get_current_user
from app.db.models.user import TfUser
from app.schemas.feature import UpdateData
from app.utils.api_utils import GoogleSheetApi
from app.db.models.external_api import GoogleAds, FacebookAds, TikTokAds


router = APIRouter()


@router.get("/api/feature-data/update-external-api", response_model=UpdateData)
async def update_data(
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
    data: str = Query(..., description="External data to update!")
):
    """
    """
    try:
        gsheet = GoogleSheetApi()
        message = ""

        if data == "data_depo":
            message = await gsheet.data_depo(range_name="'Data Depo RAW'!A:F", session=session)
        elif data == "google_ads":
            message = await gsheet.campaign_ads(range_name="'Google Ads Campaign'!A:I", session=session, classes=GoogleAds)
        elif data == "facebook_ads":
            message = await gsheet.campaign_ads(range_name="'Meta Ads Campaign'!A:I", session=session, classes=FacebookAds)
        elif data == "tiktok_ads":
            message = await gsheet.campaign_ads(range_name="'TikTok Ads Campaign'!A:I", session=session, classes=TikTokAds)
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
