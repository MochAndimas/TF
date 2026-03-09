"""Extraction utilities for external API pipelines."""

from __future__ import annotations

import asyncio
from datetime import date

import requests
from decouple import config
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


class ExternalApiExtractor:
    """Extract raw payloads from external ETL sources.

    This extractor centralizes source-specific API clients used by the ETL
    pipelines, including:
        - generic JSON endpoint for deposit source,
        - Google Sheets values API for ads source,
        - GA4 Analytics Data API for app/web user metrics.
    """

    def __init__(self) -> None:
        creds = Credentials(
            None,
            refresh_token=config("GSHEET_REFRESH_TOKEN", cast=str),
            token_uri=config("GSHEET_TOKEN_URI", cast=str),
            client_id=config("GSHEET_CLIENT_ID", cast=str),
            client_secret=config("GSHEET_CLIENT_SECRET", cast=str),
        )
        self.service = build("sheets", version="v4", credentials=creds, cache_discovery=False)
        self.sheet_id = config("GSHEET_SHEET_ID", cast=str)
        self.ga4_property_id = config("GA4_PROPERTY_ID", default=None, cast=str)
        ga4_refresh_token = config("GA4_REFRESH_TOKEN", default=None, cast=str)
        ga4_token_uri = config("GA4_TOKEN_URI", default=None, cast=str)
        ga4_client_id = config("GA4_CLIENT_ID", default=None, cast=str)
        ga4_client_secret = config("GA4_CLIENT_SECRET", default=None, cast=str)
        self.ga4_service = None
        if (
            self.ga4_property_id
            and ga4_refresh_token
            and ga4_token_uri
            and ga4_client_id
            and ga4_client_secret
        ):
            ga4_creds = Credentials(
                None,
                refresh_token=ga4_refresh_token,
                token_uri=ga4_token_uri,
                client_id=ga4_client_id,
                client_secret=ga4_client_secret,
                scopes=["https://www.googleapis.com/auth/analytics.readonly"],
            )
            self.ga4_service = build(
                "analyticsdata",
                version="v1beta",
                credentials=ga4_creds,
                cache_discovery=False,
            )
        self.depo_source_url = config(
            "DEPO_SOURCE_URL",
            default=(
                "https://script.googleusercontent.com/macros/echo?"
                "user_content_key=AY5xjrTjdihXxTA5m1lfYwe5_8C9SGIK6Z95X4LtG9s5KT5tF_5-6iY1zwHLI16hdXFCudT2CueRQ2OccG7qM_a5wHVEAGAMXpIClsE1jpruGO8l0GwHFHdDcAFUyRws2G4E_ChFM62UL_bGfkUsWK0wyIBVZMb7eIu5oNR20HhxEYLLwlPp8WD4gpXF3mYnC9LchGTvoeKfR_KiBhq78s8_dgKUvw4S_Rr0h0bqgXz7EsbwZ-JYsmPGNPt_HxLTKGLRIAEf9lzrlEcThO6L8b2HSBzhB7yG7g"
                "&lib=M2A7k1cML9_qiTb3aF9ZIZKiUcBrFPiXa"
            ),
            cast=str,
        )

    async def fetch_json_url(self, url: str) -> list:
        """Fetch JSON payload from a URL without blocking the event loop.

        Args:
            url (str): HTTP/HTTPS endpoint that returns JSON array payload.

        Returns:
            list: Parsed JSON payload from the response body.

        Raises:
            requests.HTTPError: Raised when remote endpoint returns non-2xx.
            requests.RequestException: Raised for transport-level failures.
        """

        def _request():
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.json()

        return await asyncio.to_thread(_request)

    async def fetch_sheet_values(self, range_name: str) -> list:
        """Fetch raw row values from one Google Sheets range.

        Args:
            range_name (str): A1 notation sheet range (for example
                ``'Google Ads Campaign'!A:I``).

        Returns:
            list: Raw values payload as list of rows, where the first row is
            expected to be the header.

        Raises:
            googleapiclient.errors.HttpError: Raised when Sheets API request fails.
        """

        def _request():
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range=range_name,
            ).execute()
            return result.get("values", [])

        return await asyncio.to_thread(_request)

    async def fetch_ga4_daily_metrics(self, start_date: date, end_date: date) -> list[dict]:
        """Fetch GA4 daily user metrics by platform in a date range.

        Args:
            start_date (date): Inclusive report start date.
            end_date (date): Inclusive report end date.

        Returns:
            list[dict]: Normalized raw rows containing ``date``, ``platform``,
            ``daily_active_users``, ``monthly_active_users``, and ``active_users``.

        Raises:
            ValueError: Raised when GA4 credentials are not fully configured.
            googleapiclient.errors.HttpError: Raised when GA4 API request fails.
        """
        if self.ga4_service is None or not self.ga4_property_id:
            raise ValueError(
                "GA4 credentials are not fully configured. "
                "Required env vars: GA4_PROPERTY_ID, GA4_REFRESH_TOKEN, "
                "GA4_TOKEN_URI, GA4_CLIENT_ID, GA4_CLIENT_SECRET."
            )

        def _request():
            response = (
                self.ga4_service.properties()
                .runReport(
                    property=f"properties/{self.ga4_property_id}",
                    body={
                        "dateRanges": [
                            {
                                "startDate": start_date.isoformat(),
                                "endDate": end_date.isoformat(),
                            }
                        ],
                        "dimensions": [{"name": "date"}, {"name": "platform"}],
                        "metrics": [
                            {"name": "active1DayUsers"},
                            {"name": "active28DayUsers"},
                            {"name": "activeUsers"},
                        ],
                        "limit": 100000,
                    },
                )
                .execute()
            )
            rows = response.get("rows", [])
            parsed_rows: list[dict] = []
            for row in rows:
                dimensions = row.get("dimensionValues", [])
                metrics = row.get("metricValues", [])
                parsed_rows.append(
                    {
                        "date": dimensions[0].get("value") if len(dimensions) > 0 else None,
                        "platform": dimensions[1].get("value") if len(dimensions) > 1 else None,
                        "daily_active_users": metrics[0].get("value") if len(metrics) > 0 else None,
                        "monthly_active_users": metrics[1].get("value") if len(metrics) > 1 else None,
                        "active_users": metrics[2].get("value") if len(metrics) > 2 else None,
                    }
                )
            return parsed_rows

        return await asyncio.to_thread(_request)

