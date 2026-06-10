"""Extraction utilities for active external API pipelines."""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta

from decouple import config
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
import httpx

from app.core.security import decrypt_secret
from app.db.models.external_api import ManagedSecret
from app.db.session import sqlite_async_session
from app.etl.extract_helpers import (
    extract_meta_leads,
    load_service_account_info,
    normalize_customer_id,
    normalize_meta_ad_account_id,
)


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
            gsheet_sa_creds = load_service_account_info("GSHEET_SA_CREDS")
            creds = ServiceAccountCredentials.from_service_account_info(
                gsheet_sa_creds,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
            self.service = build("sheets", version="v4", credentials=creds, cache_discovery=False)
        self.first_deposit_sheet_id = config(
            "FIRST_DEPOSIT_SHEET_ID",
            default="1avonZ9znYOExrqPUMpQJsRC8LA2MjbVPFTfmCoapA4g",
            cast=str,
        ).strip()
        self.first_deposit_sheet_range = config(
            "FIRST_DEPOSIT_SHEET_RANGE",
            default="'RAW Regis'!A:T",
            cast=str,
        ).strip()
        self.ms_deposit_sheet_range = config(
            "MS_DEPOSIT_SHEET_RANGE",
            default="'RAW Regis'!A:W",
            cast=str,
        ).strip()
        self.daily_regis_sheet_id = config(
            "DAILY_REGIS_SHEET_ID",
            default="1avonZ9znYOExrqPUMpQJsRC8LA2MjbVPFTfmCoapA4g",
            cast=str,
        ).strip()
        self.daily_regis_sheet_range = config(
            "DAILY_REGIS_SHEET_RANGE",
            default="'RAW Regis'!A:G",
            cast=str,
        ).strip()
        self.ga4_property_id = config("GA4_PROPERTY_ID", default=None, cast=str)
        raw_ga4_sa_creds = config("GA4_SA_CREDS", default="", cast=str).strip()
        self.ga4_service = None
        if self.ga4_property_id and raw_ga4_sa_creds:
            ga4_sa_creds = load_service_account_info("GA4_SA_CREDS")
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
        self.google_ads_customer_id = normalize_customer_id(
            config("GOOGLE_ADS_CUSTOMER_ID", default="", cast=str)
        )
        self.google_ads_login_customer_id = normalize_customer_id(
            config("GOOGLE_ADS_LOGIN_CUSTOMER_ID", default="", cast=str)
        )
        self.meta_app_id = config("META_APP_ID", default="", cast=str).strip() or None
        self.meta_app_secret = config("META_APP_SECRET", default="", cast=str).strip() or None
        self.meta_api_version = config("META_API_VERSION", default="v22.0", cast=str).strip() or "v22.0"
        self.meta_ad_id = config("META_AD_ID", default="", cast=str).strip() or None
        self.instagram_user_id = config("INSTAGRAM_USER_ID", default="", cast=str).strip() or None
        self.instagram_media_insight_concurrency = config(
            "INSTAGRAM_MEDIA_INSIGHT_CONCURRENCY",
            default=5,
            cast=int,
        )
        self.google_ads_client = None

    async def _load_managed_secret(self, secret_key: str) -> str:
        """Load and decrypt a managed secret from backend storage when present."""
        async with sqlite_async_session() as session:
            stored_secret = await session.get(ManagedSecret, secret_key)
            if stored_secret is None:
                return ""
            return decrypt_secret(stored_secret.secret_value).strip()

    async def _build_google_ads_client(self) -> GoogleAdsClient | None:
        """Create Google Ads API client from environment variables when configured."""
        developer_token = config("GOOGLE_ADS_DEVELOPER_TOKEN", default="", cast=str).strip()
        refresh_token = await self._load_managed_secret("google_ads_refresh_token")
        if not refresh_token:
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
            self.google_ads_client = await self._build_google_ads_client()

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

    async def fetch_facebook_ads_metrics(self, start_date: date, end_date: date) -> list[dict]:
        """Fetch Meta Ads Insights metrics by ad for the requested date range."""
        meta_ad_account_id = normalize_meta_ad_account_id(self.meta_ad_id)
        access_token = await self._load_managed_secret("meta_ads_access_token")
        if not access_token:
            access_token = config("META_ACCESS_TOKEN", default="", cast=str).strip()

        if not meta_ad_account_id or not access_token:
            raise ValueError(
                "Meta Ads credentials are not fully configured. "
                "Required env vars: META_AD_ID and stored `meta_ads_access_token` "
                "or fallback `META_ACCESS_TOKEN`."
            )

        fields = ",".join(
            [
                "date_start",
                "date_stop",
                "campaign_id",
                "campaign_name",
                "adset_name",
                "ad_name",
                "spend",
                "impressions",
                "clicks",
                "actions",
            ]
        )
        params = {
            "access_token": access_token,
            "level": "ad",
            "time_increment": 1,
            "action_attribution_windows": json.dumps(
                ["7d_click", "1d_view", "1d_ev"],
                separators=(",", ":"),
            ),
            "time_range": json.dumps(
                {"since": start_date.isoformat(), "until": end_date.isoformat()},
                separators=(",", ":"),
            ),
            "fields": fields,
            "limit": 500,
        }
        request_url = f"https://graph.facebook.com/{self.meta_api_version}/{meta_ad_account_id}/insights"
        parsed_rows: list[dict] = []

        async with httpx.AsyncClient(timeout=120) as client:
            while request_url:
                response = await client.get(request_url, params=params if request_url.endswith("/insights") else None)
                response.raise_for_status()
                payload = response.json()
                for row in payload.get("data", []):
                    parsed_rows.append(
                        {
                            "date": row.get("date_start"),
                            "campaign_id": str(row.get("campaign_id") or "-"),
                            "campaign_name": str(row.get("campaign_name") or "-"),
                            "ad_group": str(row.get("adset_name") or "-"),
                            "ad_name": str(row.get("ad_name") or "-"),
                            "cost": float(row.get("spend") or 0),
                            "impressions": int(float(row.get("impressions") or 0)),
                            "clicks": int(float(row.get("clicks") or 0)),
                            "leads": extract_meta_leads(row.get("actions")),
                        }
                    )
                request_url = payload.get("paging", {}).get("next")
                params = None

        return parsed_rows

    async def fetch_instagram_insights(self, start_date: date, end_date: date) -> list[dict]:
        """Fetch Instagram daily insight metrics for the requested date range."""
        access_token = await self._load_managed_secret("instagram_access_token")
        if not access_token:
            access_token = config("INSTAGRAM_ACCESS_TOKEN", default="", cast=str).strip()

        if not access_token:
            raise ValueError(
                "Instagram credentials are not fully configured. "
                "Store `instagram_access_token` from the Instagram Token page "
                "or set fallback `INSTAGRAM_ACCESS_TOKEN`."
            )

        base_url = f"https://graph.instagram.com/{self.meta_api_version}"
        rows_by_date = {
            target_date: {
                "date": target_date.isoformat(),
                "total_followers": 0,
                "new_followers": 0,
                "total_engagement": 0,
                "likes": 0,
                "comments": 0,
                "shares": 0,
                "saves": 0,
            }
            for target_date in self._iter_dates(start_date, end_date)
        }

        async with httpx.AsyncClient(timeout=120) as client:
            profile = await self._fetch_instagram_profile(
                client=client,
                base_url=base_url,
                access_token=access_token,
            )
            instagram_user_path = self.instagram_user_id or "me"
            if instagram_user_path != "me" and not str(instagram_user_path).strip():
                raise ValueError("Instagram user id is missing. Set `INSTAGRAM_USER_ID` or validate the stored token.")

            total_followers = int(float(profile.get("followers_count") or 0))
            for row in rows_by_date.values():
                row["total_followers"] = total_followers

            metric_values = await self._fetch_instagram_account_metric_values(
                client=client,
                base_url=base_url,
                instagram_user_path=instagram_user_path,
                access_token=access_token,
                start_date=start_date,
                end_date=end_date,
            )
            for metric_name, values_by_date in metric_values.items():
                target_column = "new_followers" if metric_name == "follower_count" else metric_name
                if target_column == "saved":
                    target_column = "saves"
                for metric_date, value in values_by_date.items():
                    if metric_date in rows_by_date and target_column in rows_by_date[metric_date]:
                        rows_by_date[metric_date][target_column] = int(float(value or 0))

            missing_engagement_columns = [
                column
                for column in ("likes", "comments", "shares", "saves")
                if not any(row[column] for row in rows_by_date.values())
            ]
            if missing_engagement_columns:
                media_totals = await self._fetch_instagram_media_engagement_totals(
                    client=client,
                    base_url=base_url,
                    instagram_user_path=instagram_user_path,
                    access_token=access_token,
                    start_date=start_date,
                    end_date=end_date,
                )
                for metric_date, totals in media_totals.items():
                    if metric_date not in rows_by_date:
                        continue
                    for column in missing_engagement_columns:
                        rows_by_date[metric_date][column] = int(totals.get(column) or 0)

        for row in rows_by_date.values():
            if not row["total_engagement"]:
                row["total_engagement"] = (
                    int(row["likes"])
                    + int(row["comments"])
                    + int(row["shares"])
                    + int(row["saves"])
                )

        return [rows_by_date[target_date] for target_date in sorted(rows_by_date)]

    @staticmethod
    def _iter_dates(start_date: date, end_date: date):
        current_date = start_date
        while current_date <= end_date:
            yield current_date
            current_date += timedelta(days=1)

    async def _fetch_instagram_profile(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        access_token: str,
    ) -> dict:
        response = await client.get(
            f"{base_url}/me",
            params={
                "fields": "id,username,account_type,followers_count",
                "access_token": access_token,
            },
        )
        if response.status_code >= 400:
            self._raise_instagram_api_error(response, "profile")
        return response.json()

    async def _fetch_instagram_account_metric_values(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        instagram_user_path: str,
        access_token: str,
        start_date: date,
        end_date: date,
    ) -> dict[str, dict[date, int]]:
        metric_aliases = {
            "follower_count": ("follower_count",),
            "total_interactions": ("total_interactions",),
            "likes": ("likes",),
            "comments": ("comments",),
            "shares": ("shares",),
            "saves": ("saves", "saved"),
        }
        metric_values: dict[str, dict[date, int]] = {}
        since = start_date.isoformat()
        until = (end_date + timedelta(days=1)).isoformat()

        for target_name, candidate_metrics in metric_aliases.items():
            for candidate_metric in candidate_metrics:
                response = await client.get(
                    f"{base_url}/{instagram_user_path}/insights",
                    params={
                        "metric": candidate_metric,
                        "period": "day",
                        "since": since,
                        "until": until,
                        "access_token": access_token,
                    },
                )
                if response.status_code >= 400:
                    continue
                payload = response.json()
                values_by_date: dict[date, int] = {}
                for metric_payload in payload.get("data", []):
                    for item in metric_payload.get("values", []):
                        metric_date = self._instagram_metric_date(item.get("end_time"))
                        if metric_date is None:
                            continue
                        values_by_date[metric_date] = int(float(item.get("value") or 0))
                if values_by_date:
                    metric_values[target_name] = values_by_date
                    break
        return metric_values

    @staticmethod
    def _parse_instagram_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        normalized = str(value).replace("Z", "+00:00")
        if len(normalized) >= 5 and normalized[-5] in {"+", "-"} and normalized[-3] != ":":
            normalized = f"{normalized[:-2]}:{normalized[-2:]}"
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    def _instagram_metric_date(self, end_time: str | None) -> date | None:
        parsed = self._parse_instagram_datetime(end_time)
        if parsed is None:
            return None
        return parsed.date()

    async def _fetch_instagram_media_engagement_totals(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        instagram_user_path: str,
        access_token: str,
        start_date: date,
        end_date: date,
    ) -> dict[date, dict[str, int]]:
        totals = {
            target_date: {"likes": 0, "comments": 0, "shares": 0, "saves": 0}
            for target_date in self._iter_dates(start_date, end_date)
        }
        request_url = f"{base_url}/{instagram_user_path}/media"
        params = {
            "fields": "id,timestamp,like_count,comments_count",
            "access_token": access_token,
            "limit": 100,
        }
        media_items: list[tuple[date, str]] = []
        reached_older_media = False
        while request_url:
            response = await client.get(request_url, params=params)
            if response.status_code >= 400:
                self._raise_instagram_api_error(response, "media")
            payload = response.json()
            for media in payload.get("data", []):
                timestamp = media.get("timestamp")
                if not timestamp:
                    continue
                parsed_timestamp = self._parse_instagram_datetime(timestamp)
                if parsed_timestamp is None:
                    continue
                media_date = parsed_timestamp.date()
                if media_date < start_date:
                    reached_older_media = True
                    continue
                if media_date > end_date:
                    continue
                totals[media_date]["likes"] += int(float(media.get("like_count") or 0))
                totals[media_date]["comments"] += int(float(media.get("comments_count") or 0))
                media_items.append((media_date, str(media.get("id") or "")))

            if reached_older_media:
                break
            request_url = payload.get("paging", {}).get("next")
            params = None

        semaphore = asyncio.Semaphore(max(1, self.instagram_media_insight_concurrency))

        async def fetch_one_media_insight(media_date: date, media_id: str) -> tuple[date, dict[str, int]]:
            async with semaphore:
                return media_date, await self._fetch_instagram_media_insights(
                    client=client,
                    base_url=base_url,
                    media_id=media_id,
                    access_token=access_token,
                )

        if media_items:
            insight_results = await asyncio.gather(
                *(fetch_one_media_insight(media_date, media_id) for media_date, media_id in media_items)
            )
            for media_date, insight_totals in insight_results:
                totals[media_date]["shares"] += int(insight_totals.get("shares") or 0)
                totals[media_date]["saves"] += int(insight_totals.get("saves") or 0)
        return totals

    @staticmethod
    def _raise_instagram_api_error(response: httpx.Response, context: str) -> None:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        error_payload = payload.get("error", {}) if isinstance(payload, dict) else {}
        if isinstance(error_payload, dict):
            message = error_payload.get("message") or error_payload.get("error_user_msg")
        else:
            message = None
        raise ValueError(f"Instagram {context} request failed ({response.status_code}): {message or 'Unknown error'}")

    async def _fetch_instagram_media_insights(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        media_id: str,
        access_token: str,
    ) -> dict[str, int]:
        if not media_id:
            return {}
        totals: dict[str, int] = {}
        for metric in ("shares", "saves", "saved"):
            response = await client.get(
                f"{base_url}/{media_id}/insights",
                params={
                    "metric": metric,
                    "access_token": access_token,
                },
            )
            if response.status_code >= 400:
                continue
            payload = response.json()
            for metric_payload in payload.get("data", []):
                name = metric_payload.get("name")
                values = metric_payload.get("values") or []
                value = values[0].get("value") if values else 0
                if name == "saved":
                    name = "saves"
                if name in {"shares", "saves"}:
                    totals[name] = int(float(value or 0))
        return totals

    async def fetch_first_deposit_records(self) -> list[dict]:
        """Fetch raw first-deposit rows from the configured Google Sheet."""
        if self.service is None or not self.first_deposit_sheet_id:
            raise ValueError(
                "First deposit Google Sheet credentials are not fully configured. "
                "Required env vars: GSHEET_SA_CREDS and FIRST_DEPOSIT_SHEET_ID."
            )

        def _request():
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.first_deposit_sheet_id,
                range=self.first_deposit_sheet_range,
            ).execute()
            return result.get("values", [])

        values = await asyncio.to_thread(_request)
        if not values:
            return []

        headers = [str(header).strip() for header in values[0]]
        source_columns = {
            "id",
            "email",
            "phone",
            "fullname",
            "tgl_regis",
            "tag",
            "campaignid",
            "protection",
            "Status\nNew / Existing",
            "Assign Date",
            "Analyst",
            "First Depo Date",
            "First Depo $",
            "Time To Closing",
            "NMI",
            "Lot",
            "Cabang",
            "Pool",
        }
        records: list[dict] = []
        for row in values[1:]:
            padded_row = list(row) + [""] * (len(headers) - len(row))
            record = {
                header: padded_row[index]
                for index, header in enumerate(headers)
                if header in source_columns
            }
            if any(str(value).strip() for value in record.values()):
                records.append(record)

        return records

    async def fetch_ms_deposit_records(self) -> list[dict]:
        """Fetch raw MS1 deposit/activity rows from the configured Google Sheet."""
        if self.service is None or not self.first_deposit_sheet_id:
            raise ValueError(
                "MS deposit Google Sheet credentials are not fully configured. "
                "Required env vars: GSHEET_SA_CREDS and FIRST_DEPOSIT_SHEET_ID."
            )

        def _request():
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.first_deposit_sheet_id,
                range=self.ms_deposit_sheet_range,
            ).execute()
            return result.get("values", [])

        values = await asyncio.to_thread(_request)
        if not values:
            return []

        headers = [str(header).strip() for header in values[0]]
        source_columns = {
            "email",
            "tag",
            "campaignid",
            "Status\nNew / Existing",
            "First Depo $",
            "Time To Closing",
            "Last Depo",
            "Last Depo Amount",
            "Last Activity",
        }
        records: list[dict] = []
        for row in values[1:]:
            padded_row = list(row) + [""] * (len(headers) - len(row))
            record = {
                header: padded_row[index]
                for index, header in enumerate(headers)
                if header in source_columns
            }
            if any(str(value).strip() for value in record.values()):
                records.append(record)

        return records

    async def fetch_daily_register_rows(self) -> list:
        """Fetch raw daily registration rows from the configured Google Sheet."""
        if self.service is None or not self.daily_regis_sheet_id:
            raise ValueError(
                "Daily register Google Sheet credentials are not fully configured. "
                "Required env vars: GSHEET_SA_CREDS and DAILY_REGIS_SHEET_ID."
            )

        def _request():
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.daily_regis_sheet_id,
                range=self.daily_regis_sheet_range,
            ).execute()
            return result.get("values", [])

        return await asyncio.to_thread(_request)
