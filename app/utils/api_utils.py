import pandas as pd
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from decouple import config
from sqlalchemy import select, delete
from app.db.session import sqlite_engine
from app.db.models.external_api import DataDepo, GoogleAds


class GoogleSheetApi:
    def __init__(self):
        creds = Credentials(
            None,
            refresh_token=config("GSHEET_REFRESH_TOKEN", cast=str),
            token_uri=config("GSHEET_TOKEN_URI", cast=str),
            client_id=config("GSHEET_CLIENT_ID", cast=str),
            client_secret=config("GSHEET_CLIENT_SECRET", cast=str)
        )

        self.service = build("sheets", version="v4", credentials=creds)
        self.sheet_id = config("GSHEET_SHEET_ID", cast=str)

    async def data_depo(
            self, 
            range_name: str,
            session: AsyncSession,
            types: str = "auto",
        ):
        """
        """
        try:
            yesterday = datetime.now().date() - timedelta(1)
            query = select(DataDepo).where(DataDepo.pull_date == datetime.now().date())
            result_query = await session.execute(query)
            gsheet_data = result_query.fetchall()

            if types == "auto":
                if gsheet_data:
                    return "Data is already updated!"
                else:
                    await session.execute(
                        delete(DataDepo)
                    )

                    result = self.service.spreadsheets().values().get(
                        spreadsheetId=self.sheet_id,
                        range=range_name
                    ).execute()

                    data = result.get("values", [])
                    df = pd.DataFrame(data[1:], columns=data[0])
                    df = df.fillna(0)

                    df["campaign_id"] = df["campaign_id"].astype(str)
                    df["campaign_name"] = df["campaign_name"].astype(str)
                    df["status"] = df["status"].astype(str)
                    df["email"] = df["email"].astype(str)
                    df["first_depo"] = df["first_depo"].astype(float)
                    df["bulan"] = pd.to_datetime(df["bulan"], format="%b-%Y").dt.date

                    for head,row in df.iterrows():
                        gsheet = DataDepo(
                            campaign_id=row["campaign_id"],
                            campaign_name=row["campaign_name"],
                            status=row["status"],
                            email=row["email"],
                            first_depo=row["first_depo"],
                            bulan=row["bulan"],
                            pull_date=datetime.now().today()
                        )
                        session.add(gsheet)
                
                    await session.commit()

            return "Data is being updated!"
        except Exception as e:
            raise HTTPException(500, f"Google Sheets error: {str(e)}")


    async def campaign_ads(
            self, 
            range_name: str,
            session: AsyncSession,
            classes: classmethod,
            start_date: datetime = datetime.now().date() - timedelta(1),
            end_date: datetime = datetime.now().date() - timedelta(1),
            types: str = "auto"
        ):
        """
        """
        try:
            if types == "auto":
                yesterday = datetime.now().date() - timedelta(1)
                query = select(classes).where(classes.date == yesterday)
                result_query = await session.execute(query)
                gsheet_data = result_query.fetchall()

                if gsheet_data:
                    return "Data is already updated!"
                else:
                    await session.execute(
                        delete(classes).filter(classes.date == yesterday)
                    )

                    result = self.service.spreadsheets().values().get(
                        spreadsheetId=self.sheet_id,
                        range=range_name
                    ).execute()

                    data = result.get("values", [])
                    df = pd.DataFrame(data[1:], columns=data[0])

                    df = df.fillna(0)
                    df["date"] = pd.to_datetime(df["date"]).dt.date
                    df["campaign_id"] = df["campaign_id"].astype(str)
                    df["cost"] = pd.to_numeric(df["cost"])
                    df["impressions"] = pd.to_numeric(df["impressions"])
                    df["clicks"] = pd.to_numeric(df["clicks"])
                    df["leads"] = pd.to_numeric(df["leads"])
                    df["pull_date"] = pd.Timestamp.now().date()

                    df_filter = df[df["date"] == yesterday]

                    for head,row in df_filter.iterrows():
                        gsheet = classes(
                            date=row["date"],
                            campaign_id=row["campaign_id"],
                            campaign_name=row["campaign_name"],
                            ad_group=row["ad_group"],
                            ad_name=row["ad_name"],
                            cost=row["cost"],
                            impressions=row["impressions"],
                            clicks=row["clicks"],
                            leads=row["leads"],
                            pull_date=datetime.now().date()
                        )
                        session.add(gsheet)

                    await session.commit()

            elif types == "manual":
                await session.execute(
                    delete(classes).filter(classes.date.between(start_date.date(), end_date.date()))
                )

                result = self.service.spreadsheets().values().get(
                    spreadsheetId=self.sheet_id,
                    range=range_name
                ).execute()

                data = result.get("values", [])
                df = pd.DataFrame(data[1:], columns=data[0])

                df = df.fillna(0)
                df["date"] = pd.to_datetime(df["date"]).dt.date
                df["campaign_id"] = df["campaign_id"].astype(str)
                df["cost"] = pd.to_numeric(df["cost"])
                df["impressions"] = pd.to_numeric(df["impressions"])
                df["clicks"] = pd.to_numeric(df["clicks"])
                df["leads"] = pd.to_numeric(df["leads"])
                df["pull_date"] = pd.Timestamp.now().date()
                
                df_filter = df[(df["date"] >= start_date.date()) & (df["date"] <= end_date.date())]

                for head,row in df_filter.iterrows():
                    gsheet = classes(
                        date=row["date"],
                        campaign_id=row["campaign_id"],
                        campaign_name=row["campaign_name"],
                        ad_group=row["ad_group"],
                        ad_name=row["ad_name"],
                        cost=row["cost"],
                        impressions=row["impressions"],
                        clicks=row["clicks"],
                        leads=row["leads"],
                        pull_date=datetime.now().date()
                    )
                    session.add(gsheet)
                    
                await session.commit()

            return "Data is being updated!"
        except Exception as e:
            raise HTTPException(500, f"Google Sheets error: {str(e)}")

