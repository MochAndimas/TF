import pandas as pd 
import plotly
import json
import asyncio
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
import plotly.graph_objects as go
from sqlalchemy import func, select


class CampaignData:
    """
    This class is used to process and analyze campaign data from various 
    advertising platforms, including Google Ads, Facebook Ads, TikTok Ads. 
    It allows querying data based on specific campaign names and date ranges.

    Attributes:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for the install data query.
        to_date (datetime.date): The end date for the install data query.
        df_google (pandas.DataFrame, optional): A pandas DataFrame containing 
            processed Google Ads data (impressions, clicks, spend, installs), 
            optionally filtered by a list of campaign names. Defaults to None.
        df_facebook (pandas.DataFrame, optional): A pandas DataFrame containing 
            processed Facebook Ads data (impressions, clicks, spend, installs), 
            optionally filtered by a list of campaign names. Defaults to None.
        df_tiktok (pandas.DataFrame, optional): A pandas DataFrame containing 
            processed TikTok Ads data (impressions, clicks, spend, installs), 
            optionally filtered by a list of campaign names. Defaults to None.
    """
    def __init__(
            self, 
            session: AsyncSession, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Initializes an `campaignData` object.

        Args:
            session (AsyncSession): The asynchronous SQLite session.
            from_date (datetime.date): The start date for the install data query.
            to_date (datetime.date): The end date for the install data query.
        """
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_google = pd.DataFrame()
        self.df_facebook = pd.DataFrame()
        self.df_tiktok = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: datetime.date, to_date: datetime.date):
        """
        Asynchronously loads and processes acquisition data for a given date range.

        Creates an `AcquisitionData` instance and fetches the acquisition data from multiple sources asynchronously 
        based on the specified date range.

        Args:
            session (AsyncSession): The asynchronous SQLAlchemy session used for database operations.
            from_date (datetime.date): The start date for filtering the install data.
            to_date (datetime.date): The end date for filtering the install data.

        Returns:
            AcquisitionData: An instance of `AcquisitionData` with the fetched data.
        """
        instance = cls(session, from_date, to_date)
        await instance._fetch_data()
        return instance
    
    async def _fetch_data(self):
        """
        Fetch the data from database.

        Parameters:
            from_date (datetime.date): The start date of data to fetch.
            to_date (datetime.date): The end date of data to fetch.
        """
        pass

    async def _read_db(
            self, 
            data: classmethod, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            list_campaign: list = []):
        """
        Reads and processes campaign data for a specific platform within the specified date range,
        optionally filtering by a list of campaign names.

        Args:
            data (class): The data source (e.g., "google", "facebook", "tiktok").
            from_date (datetime.date): The start date for the data query.
            to_date (datetime.date): The end date for the data query.
            list_campaign (list, optional): A list of campaign names to filter the data by. 
                An empty list (`[]`) retrieves data for all campaigns. Defaults to an empty list.

        Returns:
            pandas.DataFrame: A DataFrame containing the processed data with columns like 
                "date", "campaign_name", "impressions", "clicks", "spend", "install", 
                and "cost/install".

        Raises:
            ValueError: If an unsupported data source is provided.
        """
        pass
