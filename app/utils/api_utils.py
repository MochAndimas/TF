from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from decouple import config
from sqlalchemy import select, delete
from app.db.models.external_api import GsheetApi


class GoogleSheetApi:
    def __init__(self):
        creds = Credentials(
            None,
            refresh_token=config("GSHEET_REFRESH_TOKEN", cast=str),
            token_uri=config("GSHEET_TOKEN_URI", cast=str),
            client_id=config("GSHEET_CLIENT_ID", cast=str),
            client_secret=config("GSHEET_CLIENT_SECRET", cast=str),
            # scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )

        self.service = build("sheets", version="v4", credentials=creds)
        self.sheet_id = config("GSHEET_SHEET_ID", cast=int)

    async def read_sheet(
            self, 
            range_name: str,
            session: AsyncSession,
            date: datetime.date,
            types: str = "auto",
        ):
        """
        """
        try:
            query = select(GsheetApi).where(GsheetApi.pull_date != datetime.now().date())
            result_query = await session.execute(query)
            gsheet_data = result_query.fetchall()
            print("____________________________")
            if types == "auto":
                if gsheet_data:
                    return "Data is already updated!"
                else:
                    await session.execute(
                        delete(GsheetApi).filter(GsheetApi.pull_date == datetime.now().today())
                    )

                    result = self.service.spreadsheets().values().get(
                        spreadsheetId=self.sheet_id,
                        range=range_name
                    ).execute()

                    data = result.get("values", [])
                    headers = data[0]
                    rows = data[1:]

                    values = [
                        dict(zip(headers, row))
                        for row in rows
                    ]

                    for row in values:
                        gsheet = GsheetApi(
                            campaign_id=row["campaign_id"],
                            campaign_name=row["campaign_name"],
                            status=row["status"],
                            email=row["email"],
                            first_depo=row["first_depo"],
                            bulan=row["bulan"],
                            pull_date=datetime.now().today()
                        )
                        await session.add(gsheet)
                
                    await session.commit()

            return "Data is being updated!"
        except ZeroDivisionError as e:
            raise HTTPException(500, f"Google Sheets error: {str(e)}")
