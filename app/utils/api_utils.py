import pandas as pd
import numpy as np
import requests
import asyncio
from datetime import date, datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from decouple import config
from sqlalchemy import select, delete, union_all, case, insert, func, literal
from app.db.models.external_api import Campaign, DataDepo
from app.db.models.external_api import  GoogleAds, FacebookAds, TikTokAds


async def unique_campaign(
        session: AsyncSession
):
    """Rebuild campaign dimension table from all ad-source fact tables.

    Args:
        session (AsyncSession): Async database session.

    Returns:
        str: Status message indicating campaign data has been refreshed.
    """
    campaign_union = union_all(
        select(
            GoogleAds.campaign_id,
            GoogleAds.campaign_name
        ),
        select(
            FacebookAds.campaign_id,
            FacebookAds.campaign_name
        ),
        select(
            TikTokAds.campaign_id,
            TikTokAds.campaign_name
        )
    ).subquery()

    query = select(
        campaign_union.c.campaign_id.distinct().label("campaign_id"),
        campaign_union.c.campaign_name,
        case(
            (campaign_union.c.campaign_name.like("GG%"), "google_ads"),
            (campaign_union.c.campaign_name.like("FB%"), "facebook_ads"),
            (campaign_union.c.campaign_name.like("TT%"), "tiktok_ads"),
            else_="unknown"
        ).label("ad_source"),
        case(
            (campaign_union.c.campaign_name.like("%- UA -%"), "user_acquisition"),
            (campaign_union.c.campaign_name.like("%- BA -%"), "brand_awareness"),
            else_="unknown"
        ).label("ad_type"),
        literal(datetime.now()).label("created_at")
    )
    try :
        insert_query = insert(Campaign).from_select(
            ["campaign_id", "campaign_name", "ad_source", "ad_type", "created_at"],
            query
        )

        await session.execute(insert_query)
        await session.commit()
    except IntegrityError:
        await session.execute(
            delete(Campaign)
        )

        insert_query = insert(Campaign).from_select(
            ["campaign_id", "campaign_name", "ad_source", "ad_type", "created_at"],
            query
        )

        await session.execute(insert_query)
        await session.commit()

    return "Data is being updated!"


class GoogleSheetApi:
    """Service class for pulling and persisting external spreadsheet/API data.

    The class centralizes data ingestion flow for:
    1. Deposit data from external JSON endpoint.
    2. Campaign ads data from Google Sheets ranges.

    Attributes:
        service: Google Sheets API client from `googleapiclient`.
        sheet_id (str): Spreadsheet identifier used for ads-range reads.
        depo_source_url (str): External JSON endpoint for deposit source payload.
    """

    def __init__(self):
        """Initialize Google API clients and external source settings.

        Reads required credentials and source URLs from environment variables,
        then prepares reusable clients/config used by ingestion methods.
        """
        creds = Credentials(
            None,
            refresh_token=config("GSHEET_REFRESH_TOKEN", cast=str),
            token_uri=config("GSHEET_TOKEN_URI", cast=str),
            client_id=config("GSHEET_CLIENT_ID", cast=str),
            client_secret=config("GSHEET_CLIENT_SECRET", cast=str),
        )
        self.service = build("sheets", version="v4", credentials=creds)
        self.sheet_id = config("GSHEET_SHEET_ID", cast=str)
        self.depo_source_url = config(
            "DEPO_SOURCE_URL",
            default=(
                "https://script.googleusercontent.com/macros/echo?"
                "user_content_key=AY5xjrTjdihXxTA5m1lfYwe5_8C9SGIK6Z95X4LtG9s5KT5tF_5-6iY1zwHLI16hdXFCudT2CueRQ2OccG7qM_a5wHVEAGAMXpIClsE1jpruGO8l0GwHFHdDcAFUyRws2G4E_ChFM62UL_bGfkUsWK0wyIBVZMb7eIu5oNR20HhxEYLLwlPp8WD4gpXF3mYnC9LchGTvoeKfR_KiBhq78s8_dgKUvw4S_Rr0h0bqgXz7EsbwZ-JYsmPGNPt_HxLTKGLRIAEf9lzrlEcThO6L8b2HSBzhB7yG7g"
                "&lib=M2A7k1cML9_qiTb3aF9ZIZKiUcBrFPiXa"
            ),
            cast=str,
        )

    @staticmethod
    def _normalize_date(value) -> date:
        """Convert date-like input into `datetime.date`.

        Args:
            value: Date input (`date`, `datetime`, or parseable string).

        Returns:
            date: Normalized date object.

        Raises:
            ValueError: Raised when value is missing or cannot be parsed.
        """
        if value is None:
            raise ValueError("Date value is required.")
        if isinstance(value, datetime):
            return value.date()
        if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
            return value
        return pd.to_datetime(value, errors="raise").date()

    def _resolve_date_window(
            self,
            types: str,
            start_date,
            end_date
    ) -> tuple[date, date]:
        """Resolve target date window from mode and user input.

        Args:
            types (str): Update mode (`auto` or `manual`).
            start_date: Start date value for manual mode.
            end_date: End date value for manual mode.

        Returns:
            tuple[date, date]: Inclusive start and end dates used for filtering.

        Raises:
            HTTPException: Raised when mode is invalid or date range is reversed.
        """
        if types not in {"auto", "manual"}:
            raise HTTPException(400, "Invalid update type. Use 'auto' or 'manual'.")

        if types == "auto":
            yesterday = datetime.now().date() - timedelta(1)
            return yesterday, yesterday

        target_start = self._normalize_date(start_date)
        target_end = self._normalize_date(end_date)
        if target_start > target_end:
            raise HTTPException(400, "start_date cannot be greater than end_date.")
        return target_start, target_end

    async def _fetch_json_url(self, url: str) -> list:
        """Fetch JSON payload from URL without blocking event loop.

        Args:
            url (str): Target endpoint returning JSON array/object.

        Returns:
            list: Parsed JSON response payload.

        Raises:
            requests.RequestException: Raised by `requests` for network/HTTP failures.
        """
        def _request():
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.json()

        return await asyncio.to_thread(_request)

    async def _fetch_sheet_values(self, range_name: str) -> list:
        """Fetch raw row values from Google Sheets for one range.

        Args:
            range_name (str): A1 notation range name in spreadsheet.

        Returns:
            list: Two-dimensional array from Sheets API (`values` field).
        """
        def _request():
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range=range_name,
            ).execute()
            return result.get("values", [])

        return await asyncio.to_thread(_request)

    @staticmethod
    def _normalize_columns(columns: list[str]) -> list[str]:
        """Normalize source header names into snake_case keys.

        Args:
            columns (list[str]): Raw header labels from source.

        Returns:
            list[str]: Normalized header labels.
        """
        normalized = []
        for column in columns:
            normalized.append(
                str(column).strip().lower().replace("\n", " ").replace(" ", "_")
            )
        return normalized

    @staticmethod
    def _parse_depo_dataframe(raw_data: list) -> pd.DataFrame:
        """Parse deposit source payload into normalized DataFrame.

        Args:
            raw_data (list): JSON payload from deposit source endpoint.

        Returns:
            pd.DataFrame: Cleaned DataFrame ready for `DataDepo` model mapping.

        Raises:
            ValueError: Raised when required source columns are missing.
        """
        df = pd.DataFrame(raw_data)
        if df.empty:
            return df

        required_columns = [
            "id", "tgl_regis", "fullname", "email", "phone", "Status\nNew / Existing",
            "campaignid", "tag", "protection", "Assign Date", "Analyst",
            "First Depo Date", "First Depo $", "Time To Closing", "NMI",
            "Lot", "Cabang", "Pool",
        ]
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in depo source: {missing_columns}")

        null_markers = {"null", "none", "nan", ""}
        object_columns = df.select_dtypes(include=["object"]).columns
        for column in object_columns:
            normalized = df[column].astype(str).str.strip().str.lower()
            df.loc[normalized.isin(null_markers), column] = None
        df = df[df["tag"].notna()]
        df.fillna(
            {
                "id": 0,
                "campaignid": 0,
                "protection": 0,
                "Analyst": 0,
                "NMI": 0,
                "Lot": 0,
                "First Depo $": 0.0,
            },
            inplace=True,
        )

        df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)
        df["tgl_regis"] = pd.to_datetime(
            df["tgl_regis"],
            format="%Y-%m-%dT%H:%M:%S.%fZ",
            utc=True,
            errors="coerce",
        ).dt.tz_localize(None).dt.date
        df["fullname"] = df["fullname"].astype(str)
        df["email"] = df["email"].astype(str)
        df["phone"] = df["phone"].astype(str)
        df["Status\nNew / Existing"] = df["Status\nNew / Existing"].astype(str)
        df["campaignid"] = pd.to_numeric(
            df["campaignid"], errors="coerce"
        ).fillna(0).astype(int).astype(str)
        df["tag"] = df["tag"].astype(str)
        df["protection"] = pd.to_numeric(df["protection"], errors="coerce").fillna(0).astype(int)
        df["Assign Date"] = pd.to_datetime(
            df["Assign Date"],
            format="%Y-%m-%dT%H:%M:%S.%fZ",
            utc=True,
            errors="coerce",
        ).dt.tz_localize(None).dt.date
        df["Analyst"] = pd.to_numeric(df["Analyst"], errors="coerce").fillna(0).astype(int)
        df["First Depo Date"] = pd.to_datetime(
            df["First Depo Date"],
            format="%Y-%m-%dT%H:%M:%S.%fZ",
            utc=True,
            errors="coerce",
        ).dt.tz_localize(None)
        df["First Depo $"] = pd.to_numeric(
            df["First Depo $"], errors="coerce"
        ).fillna(0.0).astype(float)
        df["Time To Closing"] = df["Time To Closing"].astype(str)
        df["NMI"] = pd.to_numeric(df["NMI"], errors="coerce").fillna(0).astype(int)
        df["Lot"] = pd.to_numeric(df["Lot"], errors="coerce").fillna(0).astype(int)
        df["Cabang"] = df["Cabang"].astype(str)
        df["Pool"] = df["Pool"].apply(
            lambda value: str(value).strip().lower() in {"true", "1", "yes", "y"}
        )
        df = df[df["tgl_regis"].notna()]
        df.replace({np.nan: None}, inplace=True)
        return df

    @staticmethod
    def _parse_ads_dataframe(raw_rows: list) -> pd.DataFrame:
        """Parse Google Sheets rows into normalized ads DataFrame.

        Args:
            raw_rows (list): Row matrix returned by Sheets API (`values`).

        Returns:
            pd.DataFrame: Cleaned ads DataFrame with normalized columns and types.

        Raises:
            ValueError: Raised when required ads columns are missing.
        """
        if not raw_rows:
            return pd.DataFrame()
        if len(raw_rows) < 2:
            return pd.DataFrame()

        headers = GoogleSheetApi._normalize_columns(raw_rows[0])
        df = pd.DataFrame(raw_rows[1:], columns=headers)
        required_columns = [
            "date", "campaign_id", "campaign_name", "ad_group",
            "ad_name", "cost", "impressions", "clicks", "leads",
        ]
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in ads sheet: {missing_columns}")

        null_markers = {"null", "none", "nan", ""}
        object_columns = df.select_dtypes(include=["object"]).columns
        for column in object_columns:
            normalized = df[column].astype(str).str.strip().str.lower()
            df.loc[normalized.isin(null_markers), column] = None
        df.fillna(
            {
                "campaign_id": "-",
                "cost": 0,
                "impressions": 0,
                "clicks": 0,
                "leads": 0,
            },
            inplace=True,
        )
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["campaign_id"] = df["campaign_id"].astype(str)
        df["campaign_name"] = df["campaign_name"].astype(str)
        df["ad_group"] = df["ad_group"].astype(str)
        df["ad_name"] = df["ad_name"].astype(str)
        df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0)
        df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0).astype(int)
        df["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0).astype(int)
        df["leads"] = pd.to_numeric(df["leads"], errors="coerce").fillna(0).astype(int)
        df = df[df["date"].notna()]
        return df

    @staticmethod
    def _build_depo_models(df: pd.DataFrame, pull_date: date) -> list[DataDepo]:
        """Convert normalized deposit DataFrame into `DataDepo` model instances.

        Args:
            df (pd.DataFrame): Normalized deposit DataFrame.
            pull_date (date): Date stamp assigned to inserted rows.

        Returns:
            list[DataDepo]: ORM model instances ready for bulk add.
        """
        models = []
        for _, row in df.iterrows():
            models.append(
                DataDepo(
                    user_id=row["id"],
                    tanggal_regis=row["tgl_regis"],
                    fullname=row["fullname"],
                    email=row["email"],
                    phone=row["phone"],
                    user_status=row["Status\nNew / Existing"],
                    campaign_id=row["campaignid"],
                    tag=row["tag"],
                    protection=row["protection"],
                    assign_date=row["Assign Date"],
                    analyst=row["Analyst"],
                    first_depo_date=row["First Depo Date"],
                    first_depo=row["First Depo $"],
                    time_to_closing=row["Time To Closing"],
                    nmi=row["NMI"],
                    lot=row["Lot"],
                    cabang=row["Cabang"],
                    pool=row["Pool"],
                    pull_date=pull_date,
                )
            )
        return models

    @staticmethod
    def _build_ads_models(df: pd.DataFrame, model_cls, pull_date: date) -> list:
        """Convert normalized ads DataFrame into ORM model instances.

        Args:
            df (pd.DataFrame): Normalized ads DataFrame.
            model_cls: Target SQLAlchemy model class (GoogleAds/FacebookAds/TikTokAds).
            pull_date (date): Date stamp assigned to inserted rows.

        Returns:
            list: ORM model instances ready for bulk add.
        """
        models = []
        for _, row in df.iterrows():
            models.append(
                model_cls(
                    date=row["date"],
                    campaign_id=row["campaign_id"],
                    campaign_name=row["campaign_name"],
                    ad_group=row["ad_group"],
                    ad_name=row["ad_name"],
                    cost=row["cost"],
                    impressions=row["impressions"],
                    clicks=row["clicks"],
                    leads=row["leads"],
                    pull_date=pull_date,
                )
            )
        return models

    async def data_depo(
            self,
            session: AsyncSession,
            start_date=None,
            end_date=None,
            types: str = "auto",
    ) -> str:
        """Fetch deposit payload, filter by date, and persist into `DataDepo`.

        Args:
            session (AsyncSession): Active async database session.
            start_date: Manual mode start date value.
            end_date: Manual mode end date value.
            types (str): Update mode (`auto` or `manual`).

        Returns:
            str: Status message for update result.

        Raises:
            HTTPException: Raised for invalid input or ingestion failures.
        """
        try:
            target_start, target_end = self._resolve_date_window(types, start_date, end_date)

            if types == "auto":
                existing_rows = await session.execute(
                    select(DataDepo.id).where(
                        DataDepo.tanggal_regis.between(target_start, target_end)
                    )
                )
                if existing_rows.first():
                    return "Data is already updated!"

            raw_data = await self._fetch_json_url(self.depo_source_url)
            df = self._parse_depo_dataframe(raw_data)
            if df.empty:
                return "No data found from source."

            df = df[(df["tgl_regis"] >= target_start) & (df["tgl_regis"] <= target_end)]

            await session.execute(
                delete(DataDepo).where(
                    DataDepo.tanggal_regis.between(target_start, target_end)
                )
            )

            if df.empty:
                await session.commit()
                return "No data found for selected date range."

            models = self._build_depo_models(df=df, pull_date=datetime.now().date())
            session.add_all(models)
            await session.commit()
            return "Data is being updated!"
        except HTTPException:
            raise
        except Exception as error:
            raise HTTPException(500, f"Google Sheets error: {str(error)}")

    async def campaign_ads(
            self,
            range_name: str,
            session: AsyncSession,
            classes: type,
            start_date=None,
            end_date=None,
            types: str = "auto",
    ) -> str:
        """Fetch ads payload from Sheets, filter by date, and persist into table.

        Args:
            range_name (str): A1 notation range to fetch from spreadsheet.
            session (AsyncSession): Active async database session.
            classes (type): Target SQLAlchemy model class for persistence.
            start_date: Manual mode start date value.
            end_date: Manual mode end date value.
            types (str): Update mode (`auto` or `manual`).

        Returns:
            str: Status message for update result.

        Raises:
            HTTPException: Raised for invalid input or ingestion failures.
        """
        try:
            target_start, target_end = self._resolve_date_window(types, start_date, end_date)

            if types == "auto":
                existing_rows = await session.execute(
                    select(classes.id).where(classes.date.between(target_start, target_end))
                )
                if existing_rows.first():
                    return "Data is already updated!"

            raw_rows = await self._fetch_sheet_values(range_name)
            df = self._parse_ads_dataframe(raw_rows)
            if df.empty:
                return "No data found from source."

            df = df[(df["date"] >= target_start) & (df["date"] <= target_end)]

            await session.execute(
                delete(classes).where(classes.date.between(target_start, target_end))
            )

            if df.empty:
                await session.commit()
                return "No data found for selected date range."

            models = self._build_ads_models(df=df, model_cls=classes, pull_date=datetime.now().date())
            session.add_all(models)
            await session.commit()
            return "Data is being updated!"
        except HTTPException:
            raise
        except Exception as error:
            raise HTTPException(500, f"Google Sheets error: {str(error)}")

