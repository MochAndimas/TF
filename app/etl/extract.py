"""Extraction utilities for active external API pipelines."""

from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path

from decouple import config
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
import httpx


class ExternalApiExtractor:
    """Extract raw payloads from active ETL sources.

    This extractor centralizes source-specific API clients used by the ETL
    pipelines, including:
        - Google Sheets values API for ads source,
        - GA4 Analytics Data API for app/web user metrics.
    """

    def __init__(self) -> None:
        self.service = None
        self.sheet_id = config("GSHEET_SHEET_ID", default="", cast=str).strip() or None
        raw_gsheet_creds = config("GSHEET_SA_CREDS", default="", cast=str).strip()
        if raw_gsheet_creds:
            gsheet_sa_creds = self._load_service_account_info("GSHEET_SA_CREDS")
            creds = ServiceAccountCredentials.from_service_account_info(
                gsheet_sa_creds,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
            self.service = build("sheets", version="v4", credentials=creds, cache_discovery=False)
        self.first_deposit_url = config(
            "FIRST_DEPOSIT_API_URL",
            default=(
                "https://script.googleusercontent.com/macros/echo?"
                "user_content_key=AY5xjrTjdihXxTA5m1lfYwe5_8C9SGIK6Z95X4LtG9s5KT5tF_5-6iY1zwHLI16hdXFCudT2CueRQ2OccG7qM_a5wHVEAGAMXpIClsE1jpruGO8l0GwHFHdDcAFUyRws2G4E_ChFM62UL_bGfkUsWK0wyIBVZMb7eIu5oNR20HhxEYLLwlPp8WD4gpXF3mYnC9LchGTvoeKfR_KiBhq78s8_dgKUvw4S_Rr0h0bqgXz7EsbwZ-JYsmPGNPt_HxLTKGLRIAEf9lzrlEcThO6L8b2HSBzhB7yG7g"
                "&lib=M2A7k1cML9_qiTb3aF9ZIZKiUcBrFPiXa"
            ),
            cast=str,
        )
        self.ga4_property_id = config("GA4_PROPERTY_ID", default=None, cast=str)
        raw_ga4_sa_creds = config("GA4_SA_CREDS", default="", cast=str).strip()
        self.ga4_service = None
        if self.ga4_property_id and raw_ga4_sa_creds:
            ga4_sa_creds = self._load_service_account_info("GA4_SA_CREDS")
            ga4_creds = ServiceAccountCredentials.from_service_account_info(
                ga4_sa_creds,
                scopes=["https://www.googleapis.com/auth/analytics.readonly"],
            )
            self.ga4_service = build(
                "analyticsdata",
                version="v1beta",
                credentials=ga4_creds,
                cache_discovery=False,
            )
        self.google_ads_customer_id = self._normalize_customer_id(
            config("GOOGLE_ADS_CUSTOMER_ID", default="", cast=str)
        )
        self.google_ads_login_customer_id = self._normalize_customer_id(
            config("GOOGLE_ADS_LOGIN_CUSTOMER_ID", default="", cast=str)
        )
        self.google_ads_client = None

    @staticmethod
    def _load_service_account_info(env_key: str) -> dict:
        """Load service-account credentials from env.

        Supported formats:
        - full JSON string on a single line
        - filesystem path to a service-account JSON file

        Args:
            env_key (str): Environment variable name containing the JSON string
                or path to the service-account credentials file.

        Returns:
            dict: Parsed service-account credentials payload.

        Raises:
            ValueError: If the env value is missing or not a valid JSON/path.
        """
        raw_value = config(env_key, default="", cast=str).strip()
        if not raw_value:
            raise ValueError(f"{env_key} is required for service-account auth.")

        normalized = raw_value.strip().strip("'").strip('"')
        if normalized.startswith("{"):
            try:
                return json.loads(normalized)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"{env_key} JSON string is invalid. Make sure it is valid single-line JSON."
                ) from exc

        path_candidate = Path(normalized)
        try:
            if path_candidate.exists():
                return json.loads(path_candidate.read_text(encoding="utf-8"))
        except OSError:
            # Some OSes raise "file name too long" when a raw JSON blob is probed as a path.
            pass

        try:
            return json.loads(normalized)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{env_key} must be either a single-line JSON string or a path to a service-account JSON file."
            ) from exc

    @staticmethod
    def _normalize_customer_id(value: str | None) -> str | None:
        """Normalize a Google Ads customer identifier by removing separators."""
        normalized = str(value or "").strip().replace("-", "")
        return normalized or None

    def _build_google_ads_client(self) -> GoogleAdsClient | None:
        """Create Google Ads API client from environment variables when configured."""
        developer_token = config("GOOGLE_ADS_DEVELOPER_TOKEN", default="", cast=str).strip()
        refresh_token = config("GOOGLE_ADS_REFRESH_TOKEN", default="", cast=str).strip()
        client_id = config("GOOGLE_ADS_CLIENT_ID", default="", cast=str).strip()
        client_secret = config("GOOGLE_ADS_CLIENT_SECRET", default="", cast=str).strip()
        if not all([developer_token, refresh_token, client_id, client_secret]):
            return None

        client_config: dict[str, str | bool] = {
            "developer_token": developer_token,
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "use_proto_plus": True,
        }
        if self.google_ads_login_customer_id:
            client_config["login_customer_id"] = self.google_ads_login_customer_id
        return GoogleAdsClient.load_from_dict(client_config)

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

        if self.service is None or not self.sheet_id:
            raise ValueError(
                "Google Sheets credentials are not fully configured. "
                "Required env vars: GSHEET_SA_CREDS and GSHEET_SHEET_ID."
            )

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
                "Required env vars: GA4_PROPERTY_ID and GA4_SA_CREDS."
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

    async def fetch_google_ads_metrics(self, start_date: date, end_date: date) -> list[dict]:
        """Fetch Google Ads metrics by ad for the requested date range."""
        if self.google_ads_client is None:
            self.google_ads_client = self._build_google_ads_client()

        if self.google_ads_client is None or not self.google_ads_customer_id:
            raise ValueError(
                "Google Ads credentials are not fully configured. "
                "Required env vars: GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, "
                "GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN, GOOGLE_ADS_CUSTOMER_ID."
            )

        standard_query = f"""
            SELECT
              segments.date,
              campaign.id,
              campaign.name,
              ad_group.name,
              ad_group_ad.ad.id,
              metrics.cost_micros,
              metrics.impressions,
              metrics.clicks,
              metrics.conversions
            FROM ad_group_ad
            WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
              AND campaign.advertising_channel_type != 'PERFORMANCE_MAX'
              AND campaign.status != 'REMOVED'
              AND ad_group.status != 'REMOVED'
              AND ad_group_ad.status != 'REMOVED'
        """

        pmax_query = f"""
            SELECT
              segments.date,
              campaign.id,
              campaign.name,
              metrics.cost_micros,
              metrics.impressions,
              metrics.clicks,
              metrics.conversions
            FROM campaign
            WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
              AND campaign.advertising_channel_type = 'PERFORMANCE_MAX'
              AND campaign.status != 'REMOVED'
        """

        def _request() -> list[dict]:
            service = self.google_ads_client.get_service("GoogleAdsService")
            parsed_rows: list[dict] = []

            standard_stream = service.search_stream(
                customer_id=self.google_ads_customer_id,
                query=standard_query,
            )
            for batch in standard_stream:
                for row in batch.results:
                    parsed_rows.append(
                        {
                            "date": str(row.segments.date),
                            "campaign_id": str(row.campaign.id),
                            "campaign_name": row.campaign.name or "-",
                            "ad_group": row.ad_group.name or "-",
                            "ad_name": str(row.ad_group_ad.ad.id or "-"),
                            "cost": float(row.metrics.cost_micros or 0) / 1_000_000,
                            "impressions": int(row.metrics.impressions or 0),
                            "clicks": int(row.metrics.clicks or 0),
                            "leads": int(round(float(row.metrics.conversions or 0))),
                        }
                    )

            pmax_stream = service.search_stream(
                customer_id=self.google_ads_customer_id,
                query=pmax_query,
            )
            for batch in pmax_stream:
                for row in batch.results:
                    parsed_rows.append(
                        {
                            "date": str(row.segments.date),
                            "campaign_id": str(row.campaign.id),
                            "campaign_name": row.campaign.name or "-",
                            "ad_group": "pmax",
                            "ad_name": "pmax",
                            "cost": float(row.metrics.cost_micros or 0) / 1_000_000,
                            "impressions": int(row.metrics.impressions or 0),
                            "clicks": int(row.metrics.clicks or 0),
                            "leads": int(round(float(row.metrics.conversions or 0))),
                        }
                    )
            return parsed_rows

        return await asyncio.to_thread(_request)

    async def fetch_first_deposit_records(self) -> list[dict]:
        """Fetch raw first-deposit rows from the configured JSON endpoint."""
        async with httpx.AsyncClient(timeout=600, follow_redirects=True) as client:
            response = await client.get(self.first_deposit_url)
            response.raise_for_status()
            payload = response.json()

        if not isinstance(payload, list):
            raise ValueError("First deposit API returned unexpected payload shape.")
        return payload
