"""Extraction utilities for active external API pipelines."""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from decouple import config
from google.ads.googleads.client import GoogleAdsClient
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
        self.meta_api_version = config("META_API_VERSION", default="v24.0", cast=str).strip() or "v24.0"
        self.meta_ad_id = config("META_AD_ID", default="", cast=str).strip() or None
        self.facebook_page_id = config("FB_PAGE_ID", default="", cast=str).strip() or None
        self.instagram_user_id = config("INSTAGRAM_USER_ID", default="", cast=str).strip() or None
        self.youtube_channel_id = config("YOUTUBE_CHANNEL_ID", default="", cast=str).strip() or None
        self.instagram_media_insight_concurrency = config(
            "INSTAGRAM_MEDIA_INSIGHT_CONCURRENCY",
            default=5,
            cast=int,
        )
        self.instagram_follow_activity_concurrency = config(
            "INSTAGRAM_FOLLOW_ACTIVITY_CONCURRENCY",
            default=10,
            cast=int,
        )
        self.youtube_media_insight_concurrency = config(
            "YOUTUBE_MEDIA_INSIGHT_CONCURRENCY",
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

    async def fetch_instagram_insights(
        self,
        start_date: date,
        end_date: date,
        cached_follow_activity: dict[date, dict[str, int]] | None = None,
        cached_media_totals: dict[date, dict[str, int]] | None = None,
    ) -> list[dict]:
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
                "unfollowers": 0,
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

            for metric_date, totals in (cached_media_totals or {}).items():
                if metric_date not in rows_by_date:
                    continue
                for column in ("likes", "comments", "shares", "saves"):
                    rows_by_date[metric_date][column] = int(totals.get(column) or 0)

            follow_activity = dict(cached_follow_activity or {})
            uncached_dates = [
                target_date
                for target_date in self._iter_dates(start_date, end_date)
                if target_date not in follow_activity
            ]
            if uncached_dates:
                follow_activity.update(
                    await self._fetch_instagram_follow_activity_values(
                        client=client,
                        base_url=base_url,
                        instagram_user_path=instagram_user_path,
                        access_token=access_token,
                        start_date=start_date,
                        end_date=end_date,
                        target_dates=uncached_dates,
                    )
                )
            for metric_date, values in follow_activity.items():
                if metric_date not in rows_by_date:
                    continue
                if "followers" in values:
                    rows_by_date[metric_date]["new_followers"] = int(values["followers"] or 0)
                if "unfollowers" in values:
                    rows_by_date[metric_date]["unfollowers"] = int(values["unfollowers"] or 0)

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

    async def fetch_youtube_daily_insight(
        self,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """Fetch daily channel metrics from the YouTube Analytics API."""
        async with httpx.AsyncClient(timeout=120) as client:
            access_token = await self._refresh_youtube_access_token(client)

            response = await client.get(
                "https://youtubeanalytics.googleapis.com/v2/reports",
                params={
                    "ids": "channel==MINE",
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "dimensions": "day",
                    "sort": "day",
                    "metrics": (
                        "views,estimatedMinutesWatched,subscribersGained,"
                        "subscribersLost,likes,comments,shares,averageViewDuration"
                    ),
                },
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if response.status_code >= 400:
                payload = response.json()
                error_payload = payload.get("error") or {}
                detail = error_payload.get("message") or "unknown error"
                raise ValueError(f"YouTube Analytics request failed: {detail}")

        payload = response.json()
        headers = [item.get("name") for item in payload.get("columnHeaders", [])]
        rows = []
        for values in payload.get("rows") or []:
            item = dict(zip(headers, values))
            subscribers_gained = int(float(item.get("subscribersGained") or 0))
            subscribers_lost = int(float(item.get("subscribersLost") or 0))
            estimated_minutes_watched = float(item.get("estimatedMinutesWatched") or 0)
            rows.append(
                {
                    "date": item.get("day"),
                    "views": int(float(item.get("views") or 0)),
                    "watch_hours": estimated_minutes_watched / 60,
                    "subscribers_gained": subscribers_gained,
                    "subscribers_lost": subscribers_lost,
                    "net_subscribers": subscribers_gained - subscribers_lost,
                    "likes": int(float(item.get("likes") or 0)),
                    "comments": int(float(item.get("comments") or 0)),
                    "shares": int(float(item.get("shares") or 0)),
                    "average_view_duration": float(item.get("averageViewDuration") or 0),
                }
            )
        return rows

    async def _refresh_youtube_access_token(self, client: httpx.AsyncClient) -> str:
        """Exchange the stored YouTube refresh token for a short-lived access token."""
        refresh_token = await self._load_managed_secret("youtube_refresh_token")
        client_id = config("YOUTUBE_CLIENT_ID", default="", cast=str).strip()
        client_secret = config("YOUTUBE_CLIENT_SECRET", default="", cast=str).strip()
        if not refresh_token or not client_id or not client_secret:
            raise ValueError(
                "YouTube credentials are not fully configured. Connect the channel "
                "from the YouTube Token page and configure the OAuth client."
            )

        token_response = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )
        if token_response.status_code >= 400:
            payload = token_response.json()
            detail = payload.get("error_description") or payload.get("error") or "unknown error"
            raise ValueError(f"YouTube OAuth token refresh failed: {detail}")

        access_token = token_response.json().get("access_token")
        if not access_token:
            raise ValueError("YouTube OAuth token refresh did not return an access token.")
        return access_token

    async def fetch_youtube_media_insight(
        self,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """Fetch lifetime YouTube content snapshots for videos published in a window."""
        if not self.youtube_channel_id:
            raise ValueError("YouTube channel ID is missing. Configure YOUTUBE_CHANNEL_ID.")

        async with httpx.AsyncClient(timeout=120) as client:
            access_token = await self._refresh_youtube_access_token(client)
            uploads_playlist_id = await self._youtube_uploads_playlist_id(
                client=client,
                access_token=access_token,
            )
            media_rows = await self._youtube_upload_rows(
                client=client,
                access_token=access_token,
                uploads_playlist_id=uploads_playlist_id,
                start_date=start_date,
                end_date=end_date,
            )

            semaphore = asyncio.Semaphore(max(1, self.youtube_media_insight_concurrency))

            async def enrich_media(row: dict) -> dict:
                async with semaphore:
                    metrics = await self._youtube_video_analytics(
                        client=client,
                        access_token=access_token,
                        video_id=row["video_id"],
                        published_date=date.fromisoformat(row["date"]),
                    )
                enriched = dict(row)
                enriched.update(metrics)
                return enriched

            if media_rows:
                media_rows = list(await asyncio.gather(*(enrich_media(row) for row in media_rows)))

        return sorted(media_rows, key=lambda row: (row["date"], row["video_id"]))

    async def _youtube_uploads_playlist_id(
        self,
        *,
        client: httpx.AsyncClient,
        access_token: str,
    ) -> str:
        """Resolve the uploads playlist for the configured YouTube channel."""
        response = await client.get(
            "https://www.googleapis.com/youtube/v3/channels",
            params={
                "part": "contentDetails",
                "id": self.youtube_channel_id,
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if response.status_code >= 400:
            payload = response.json()
            detail = (payload.get("error") or {}).get("message") or "unknown error"
            raise ValueError(f"YouTube channel lookup failed: {detail}")
        items = response.json().get("items") or []
        if not items:
            raise ValueError("Configured YouTube channel was not accessible to the OAuth account.")
        playlist_id = (
            items[0].get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads")
        )
        if not playlist_id:
            raise ValueError("YouTube uploads playlist was not returned for the configured channel.")
        return playlist_id

    async def _youtube_upload_rows(
        self,
        *,
        client: httpx.AsyncClient,
        access_token: str,
        uploads_playlist_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """List channel uploads published inside the requested ETL window."""
        rows: list[dict] = []
        page_token: str | None = None
        reached_older_video = False
        while not reached_older_video:
            params = {
                "part": "snippet,contentDetails",
                "playlistId": uploads_playlist_id,
                "maxResults": 50,
            }
            if page_token:
                params["pageToken"] = page_token
            response = await client.get(
                "https://www.googleapis.com/youtube/v3/playlistItems",
                params=params,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if response.status_code >= 400:
                payload = response.json()
                detail = (payload.get("error") or {}).get("message") or "unknown error"
                raise ValueError(f"YouTube uploads request failed: {detail}")

            payload = response.json()
            for item in payload.get("items") or []:
                snippet = item.get("snippet") or {}
                content_details = item.get("contentDetails") or {}
                published_at = self._parse_youtube_datetime(
                    content_details.get("videoPublishedAt") or snippet.get("publishedAt")
                )
                if published_at is None:
                    continue
                published_date = published_at.date()
                if published_date < start_date:
                    reached_older_video = True
                    continue
                if published_date > end_date:
                    continue

                video_id = str(
                    content_details.get("videoId")
                    or (snippet.get("resourceId") or {}).get("videoId")
                    or ""
                ).strip()
                if not video_id:
                    continue
                rows.append(
                    {
                        "date": published_date.isoformat(),
                        "video_id": video_id,
                        "title": str(snippet.get("title") or "Untitled").strip(),
                        "published_at": published_at.isoformat(),
                        "content_type": "UNKNOWN",
                        "thumbnail_url": self._youtube_thumbnail_url(snippet.get("thumbnails")),
                        "permalink": f"https://www.youtube.com/watch?v={video_id}",
                        "views": 0,
                        "watch_hours": 0.0,
                        "average_view_percentage": 0.0,
                        "likes": 0,
                        "comments": 0,
                        "shares": 0,
                        "subscribers_gained": 0,
                    }
                )

            page_token = payload.get("nextPageToken")
            if not page_token:
                break
        return rows

    async def _youtube_video_analytics(
        self,
        *,
        client: httpx.AsyncClient,
        access_token: str,
        video_id: str,
        published_date: date,
    ) -> dict:
        """Fetch a lifetime metric snapshot and content type for one video."""
        response = await client.get(
            "https://youtubeanalytics.googleapis.com/v2/reports",
            params={
                "ids": "channel==MINE",
                "startDate": published_date.isoformat(),
                "endDate": datetime.now().date().isoformat(),
                "dimensions": "creatorContentType",
                "filters": f"video=={video_id}",
                "metrics": (
                    "views,estimatedMinutesWatched,averageViewPercentage,"
                    "likes,comments,shares,subscribersGained"
                ),
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if response.status_code >= 400:
            payload = response.json()
            detail = (payload.get("error") or {}).get("message") or "unknown error"
            raise ValueError(f"YouTube video analytics failed for {video_id}: {detail}")

        payload = response.json()
        headers = [item.get("name") for item in payload.get("columnHeaders", [])]
        values = (payload.get("rows") or [])[0] if payload.get("rows") else []
        item = dict(zip(headers, values))
        return {
            "content_type": str(item.get("creatorContentType") or "UNKNOWN").upper(),
            "views": int(float(item.get("views") or 0)),
            "watch_hours": float(item.get("estimatedMinutesWatched") or 0) / 60,
            "average_view_percentage": float(item.get("averageViewPercentage") or 0),
            "likes": int(float(item.get("likes") or 0)),
            "comments": int(float(item.get("comments") or 0)),
            "shares": int(float(item.get("shares") or 0)),
            "subscribers_gained": int(float(item.get("subscribersGained") or 0)),
        }

    @staticmethod
    def _parse_youtube_datetime(value: str | None) -> datetime | None:
        """Parse one RFC3339 timestamp returned by YouTube."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _youtube_thumbnail_url(thumbnails: dict | None) -> str | None:
        """Pick the highest-quality available thumbnail URL."""
        values = thumbnails or {}
        for key in ("maxres", "standard", "high", "medium", "default"):
            url = (values.get(key) or {}).get("url")
            if url:
                return str(url)
        return None

    async def fetch_facebook_page_insights(self, start_date: date, end_date: date) -> list[dict]:
        """Fetch Facebook Page daily insight metrics for the requested date range."""
        meta_access_token = await self._load_managed_secret("meta_ads_access_token")
        if not meta_access_token:
            meta_access_token = config("META_ADS_ACCESS_TOKEN", default="", cast=str).strip()

        if not meta_access_token or not self.facebook_page_id:
            raise ValueError(
                "Facebook Page credentials are not fully configured. "
                "Required: managed secret `meta_ads_access_token` and env `FB_PAGE_ID`."
            )

        base_url = f"https://graph.facebook.com/{self.meta_api_version}"
        metric_columns = [
            "page_fans",
            "page_fan_adds",
            "page_fan_removes",
            "page_impressions",
            "page_impressions_unique",
            "page_impressions_paid",
            "page_impressions_organic_v2",
            "page_post_engagements",
            "page_video_views",
            "page_views_total",
        ]
        reaction_columns = [
            "reaction_like",
            "reaction_love",
            "reaction_wow",
            "reaction_haha",
            "reaction_sorry",
            "reaction_anger",
        ]
        rows_by_date = {
            target_date: {
                "page_id": self.facebook_page_id,
                "date": target_date.isoformat(),
                **{column: 0 for column in metric_columns + reaction_columns},
            }
            for target_date in self._iter_dates(start_date, end_date)
        }
        metric_aliases = {
            "page_fans": ("page_follows", "page_fans"),
            "page_fan_adds": ("page_daily_follows", "page_fan_adds"),
            "page_fan_removes": ("page_daily_unfollows", "page_fan_removes"),
            "page_impressions": ("page_posts_impressions", "page_impressions"),
            "page_impressions_unique": ("page_impressions_unique",),
            "page_impressions_paid": ("page_posts_impressions_paid", "page_impressions_paid"),
            "page_impressions_organic_v2": ("page_posts_impressions_organic", "page_impressions_organic_v2", "page_impressions_organic"),
            "page_post_engagements": ("page_post_engagements",),
            "page_video_views": ("page_video_views",),
            "page_views_total": ("page_views_total",),
            "page_actions_post_reactions_total": ("page_actions_post_reactions_total",),
        }
        fetched_metric_count = 0

        async with httpx.AsyncClient(timeout=120) as client:
            access_token = await self._resolve_facebook_page_access_token(
                client=client,
                base_url=base_url,
                user_access_token=meta_access_token,
            )
            for target_name, candidates in metric_aliases.items():
                for metric_name in candidates:
                    values_by_date = await self._fetch_facebook_page_metric_values(
                        client=client,
                        base_url=base_url,
                        access_token=access_token,
                        metric_name=metric_name,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    resolved = False
                    for metric_date, value in values_by_date.items():
                        if metric_date not in rows_by_date:
                            continue
                        if target_name == "page_actions_post_reactions_total":
                            reaction_values = value if isinstance(value, dict) else {}
                            for reaction_key, column in {
                                "like": "reaction_like",
                                "love": "reaction_love",
                                "wow": "reaction_wow",
                                "haha": "reaction_haha",
                                "sorry": "reaction_sorry",
                                "anger": "reaction_anger",
                            }.items():
                                rows_by_date[metric_date][column] = int(float(reaction_values.get(reaction_key) or 0))
                        else:
                            rows_by_date[metric_date][target_name] = int(float(value or 0))
                        resolved = True
                    if resolved:
                        fetched_metric_count += 1
                        break

        if fetched_metric_count == 0:
            raise ValueError(
                "Facebook Page insights request returned no usable metrics. "
                "Make sure `meta_ads_access_token` can resolve a Page Access Token for `FB_PAGE_ID` "
                "and has pages_read_engagement/read_insights permissions."
            )

        return [rows_by_date[target_date] for target_date in sorted(rows_by_date)]

    async def fetch_instagram_media_insights(self, start_date: date, end_date: date) -> list[dict]:
        """Fetch Instagram post and reels media insight snapshots for a date range."""
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
        instagram_user_path = self.instagram_user_id or "me"
        media_rows: list[dict] = []

        async with httpx.AsyncClient(timeout=120) as client:
            request_url = f"{base_url}/{instagram_user_path}/media"
            params = {
                "fields": (
                    "id,caption,media_type,media_product_type,timestamp,permalink,"
                    "media_url,thumbnail_url,like_count,comments_count"
                ),
                "access_token": access_token,
                "limit": 100,
            }
            reached_older_media = False
            while request_url:
                response = await client.get(request_url, params=params)
                if response.status_code >= 400:
                    self._raise_instagram_api_error(response, "media")
                payload = response.json()
                for media in payload.get("data", []):
                    parsed_timestamp = self._parse_instagram_datetime(media.get("timestamp"))
                    if parsed_timestamp is None:
                        continue
                    media_date = parsed_timestamp.date()
                    if media_date < start_date:
                        reached_older_media = True
                        continue
                    if media_date > end_date:
                        continue

                    normalized_product_type = self._instagram_media_product_type(media)
                    if normalized_product_type not in {"FEED", "REELS"}:
                        continue

                    media_rows.append(
                        {
                            "date": media_date.isoformat(),
                            "media_id": str(media.get("id") or "").strip(),
                            "media_type": str(media.get("media_type") or "").strip().upper() or "UNKNOWN",
                            "media_product_type": normalized_product_type,
                            "timestamp": parsed_timestamp.isoformat(),
                            "caption": media.get("caption"),
                            "permalink": media.get("permalink"),
                            "media_url": media.get("media_url"),
                            "thumbnail_url": media.get("thumbnail_url"),
                            "likes": int(float(media.get("like_count") or 0)),
                            "comments": int(float(media.get("comments_count") or 0)),
                            "shares": 0,
                            "saves": 0,
                            "reach": 0,
                            "impressions": 0,
                            "plays": 0,
                            "total_engagement": 0,
                        }
                    )

                if reached_older_media:
                    break
                request_url = payload.get("paging", {}).get("next")
                params = None

            semaphore = asyncio.Semaphore(max(1, self.instagram_media_insight_concurrency))

            async def enrich_media(row: dict) -> dict:
                async with semaphore:
                    insights = await self._fetch_instagram_media_insight_metrics(
                        client=client,
                        base_url=base_url,
                        media_id=row["media_id"],
                        access_token=access_token,
                    )
                    enriched = dict(row)
                    for metric in ("shares", "saves", "reach", "impressions", "plays"):
                        enriched[metric] = int(insights.get(metric) or 0)
                    enriched["total_engagement"] = (
                        int(enriched["likes"])
                        + int(enriched["comments"])
                        + int(enriched["shares"])
                        + int(enriched["saves"])
                    )
                    return enriched

            if media_rows:
                media_rows = list(await asyncio.gather(*(enrich_media(row) for row in media_rows)))

        return sorted(media_rows, key=lambda row: (row["date"], row["media_id"]))

    async def fetch_facebook_page_media_insights(self, start_date: date, end_date: date) -> list[dict]:
        """Fetch Facebook Page post/media lifetime insight snapshots for a date range."""
        meta_access_token = await self._load_managed_secret("meta_ads_access_token")
        if not meta_access_token:
            meta_access_token = config("META_ADS_ACCESS_TOKEN", default="", cast=str).strip()

        if not meta_access_token or not self.facebook_page_id:
            raise ValueError(
                "Facebook Page media credentials are not fully configured. "
                "Required: managed secret `meta_ads_access_token` and env `FB_PAGE_ID`."
            )

        base_url = f"https://graph.facebook.com/{self.meta_api_version}"
        media_rows: list[dict] = []

        async with httpx.AsyncClient(timeout=120) as client:
            access_token = await self._resolve_facebook_page_access_token(
                client=client,
                base_url=base_url,
                user_access_token=meta_access_token,
            )
            request_url = f"{base_url}/{self.facebook_page_id}/posts"
            params = {
                "fields": (
                    "id,message,created_time,permalink_url,shares,"
                    "attachments{media_type,media,type}"
                ),
                "since": start_date.isoformat(),
                "until": (end_date + timedelta(days=1)).isoformat(),
                "access_token": access_token,
                "limit": 100,
            }
            reached_older_post = False
            while request_url:
                response = await client.get(request_url, params=params)
                if response.status_code >= 400:
                    self._raise_facebook_api_error(response, "page posts")
                payload = response.json()
                for post in payload.get("data", []):
                    parsed_created_time = self._parse_instagram_datetime(post.get("created_time"))
                    if parsed_created_time is None:
                        continue
                    post_date = parsed_created_time.date()
                    if post_date < start_date:
                        reached_older_post = True
                        continue
                    if post_date > end_date:
                        continue

                    post_id = str(post.get("id") or "").strip()
                    if not post_id:
                        continue
                    attachment = self._facebook_primary_attachment(post)
                    media_rows.append(
                        {
                            "page_id": self.facebook_page_id,
                            "date": post_date.isoformat(),
                            "post_id": post_id,
                            "post_type": str(
                                attachment.get("media_type")
                                or attachment.get("type")
                                or "UNKNOWN"
                            ).strip().upper(),
                            "status_type": attachment.get("type"),
                            "created_time": parsed_created_time.isoformat(),
                            "message": post.get("message"),
                            "permalink_url": post.get("permalink_url"),
                            "full_picture": self._facebook_attachment_image_url(attachment),
                            "likes": 0,
                            "comments": 0,
                            "shares": int(float((post.get("shares") or {}).get("count") or 0)),
                            "reaction_like": 0,
                            "reaction_love": 0,
                            "reaction_wow": 0,
                            "reaction_haha": 0,
                            "reaction_sorry": 0,
                            "reaction_anger": 0,
                            "post_media_view": 0,
                            "post_clicks": 0,
                            "post_video_views": 0,
                            "total_engagement": 0,
                        }
                    )

                if reached_older_post:
                    break
                request_url = payload.get("paging", {}).get("next")
                params = None

            semaphore = asyncio.Semaphore(max(1, self.instagram_media_insight_concurrency))

            async def enrich_post(row: dict) -> dict:
                async with semaphore:
                    social_counts = await self._fetch_facebook_post_social_counts(
                        client=client,
                        base_url=base_url,
                        post_id=row["post_id"],
                        access_token=access_token,
                    )
                    insights = await self._fetch_facebook_post_insight_metrics(
                        client=client,
                        base_url=base_url,
                        post_id=row["post_id"],
                        access_token=access_token,
                    )
                    enriched = dict(row)
                    enriched["likes"] = int(social_counts.get("likes") or 0)
                    enriched["comments"] = int(social_counts.get("comments") or 0)
                    for metric in (
                        "reaction_like",
                        "reaction_love",
                        "reaction_wow",
                        "reaction_haha",
                        "reaction_sorry",
                        "reaction_anger",
                        "post_media_view",
                        "post_clicks",
                        "post_video_views",
                    ):
                        enriched[metric] = int(insights.get(metric) or 0)
                    if not enriched["reaction_like"]:
                        enriched["reaction_like"] = int(enriched["likes"])
                    reaction_total = sum(
                        int(enriched.get(metric) or 0)
                        for metric in (
                            "reaction_like",
                            "reaction_love",
                            "reaction_wow",
                            "reaction_haha",
                            "reaction_sorry",
                            "reaction_anger",
                        )
                    )
                    enriched["total_engagement"] = (
                        max(int(enriched["likes"]), reaction_total)
                        + int(enriched["comments"])
                        + int(enriched["shares"])
                    )
                    return enriched

            if media_rows:
                media_rows = list(await asyncio.gather(*(enrich_post(row) for row in media_rows)))

        return sorted(media_rows, key=lambda row: (row["date"], row["post_id"]))

    @staticmethod
    def _facebook_primary_attachment(post: dict) -> dict:
        """Return the first attachment from a Page post Graph API payload."""
        attachments = post.get("attachments") or {}
        data = attachments.get("data") if isinstance(attachments, dict) else None
        if not isinstance(data, list) or not data or not isinstance(data[0], dict):
            return {}
        return data[0]

    @staticmethod
    def _facebook_attachment_image_url(attachment: dict) -> str | None:
        """Extract the image URL from a nested Page post attachment payload."""
        media = attachment.get("media") or {}
        image = media.get("image") if isinstance(media, dict) else None
        if not isinstance(image, dict):
            return None
        image_url = str(image.get("src") or "").strip()
        return image_url or None

    @staticmethod
    def _instagram_media_product_type(media: dict) -> str:
        product_type = str(media.get("media_product_type") or "").strip().upper()
        media_type = str(media.get("media_type") or "").strip().upper()
        if product_type == "REELS":
            return "REELS"
        if product_type == "FEED":
            return "FEED"
        if media_type in {"IMAGE", "VIDEO", "CAROUSEL_ALBUM"}:
            return "FEED"
        return product_type or media_type or "UNKNOWN"

    @staticmethod
    def _iter_dates(start_date: date, end_date: date):
        current_date = start_date
        while current_date <= end_date:
            yield current_date
            current_date += timedelta(days=1)

    @staticmethod
    def _instagram_day_timestamp(target_date: date) -> int:
        """Return the Unix timestamp for midnight in the reporting timezone."""
        reporting_timezone = ZoneInfo("Asia/Jakarta")
        return int(
            datetime.combine(
                target_date,
                datetime.min.time(),
                tzinfo=reporting_timezone,
            ).timestamp()
        )

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
        date_windows = list(self._iter_date_windows(start_date, end_date, max_days=30))

        for target_name, candidate_metrics in metric_aliases.items():
            for candidate_metric in candidate_metrics:
                values_by_date: dict[date, int] = {}
                candidate_supported = True
                for window_start, window_end in date_windows:
                    response = await client.get(
                        f"{base_url}/{instagram_user_path}/insights",
                        params={
                            "metric": candidate_metric,
                            "period": "day",
                            "since": self._instagram_day_timestamp(window_start),
                            "until": self._instagram_day_timestamp(window_end + timedelta(days=1)),
                            "access_token": access_token,
                        },
                    )
                    if response.status_code >= 400:
                        candidate_supported = False
                        break
                    payload = response.json()
                    for metric_payload in payload.get("data", []):
                        for item in metric_payload.get("values", []):
                            metric_date = self._instagram_metric_date(item.get("end_time"))
                            if metric_date is None:
                                continue
                            values_by_date[metric_date] = int(float(item.get("value") or 0))
                if not candidate_supported:
                    continue
                if values_by_date:
                    metric_values[target_name] = values_by_date
                    break
        return metric_values

    async def _fetch_instagram_follow_activity_values(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        instagram_user_path: str,
        access_token: str,
        start_date: date,
        end_date: date,
        target_dates: list[date] | None = None,
    ) -> dict[date, dict[str, int]]:
        semaphore = asyncio.Semaphore(max(1, self.instagram_follow_activity_concurrency))
        requested_dates = sorted(
            set(target_dates or list(self._iter_dates(start_date, end_date)))
        )
        if not requested_dates:
            return {}

        results: dict[date, dict[str, int]] = {}
        for date_chunk in self._chunk_instagram_follow_dates(requested_dates):
            anchor_date = date_chunk[0] - timedelta(days=7)
            boundaries = sorted(
                {
                    boundary
                    for target_date in date_chunk
                    for boundary in (target_date - timedelta(days=1), target_date)
                }
            )

            async def fetch_cumulative(
                boundary_date: date,
                *,
                chunk_anchor: date = anchor_date,
            ) -> tuple[date, dict[str, int]]:
                async with semaphore:
                    response = await client.get(
                        f"{base_url}/{instagram_user_path}/insights",
                        params={
                            "metric": "follows_and_unfollows",
                            "period": "day",
                            "metric_type": "total_value",
                            "breakdown": "follow_type",
                            "since": self._instagram_day_timestamp(chunk_anchor),
                            "until": self._instagram_day_timestamp(boundary_date + timedelta(days=1)),
                            "access_token": access_token,
                        },
                    )
                if response.status_code >= 400:
                    self._raise_instagram_api_error(
                        response,
                        f"follows_and_unfollows through {boundary_date.isoformat()}",
                    )
                totals = self._parse_instagram_follow_breakdown(response.json())
                return boundary_date, {
                    "followers": int(totals.get("followers") or 0),
                    "unfollowers": int(totals.get("unfollowers") or 0),
                }

            cumulative = dict(
                await asyncio.gather(*(fetch_cumulative(boundary) for boundary in boundaries))
            )
            for target_date in date_chunk:
                previous = cumulative[target_date - timedelta(days=1)]
                current = cumulative[target_date]
                daily_totals = {
                    metric: current[metric] - previous[metric]
                    for metric in ("followers", "unfollowers")
                }
                if any(value < 0 for value in daily_totals.values()):
                    raise ValueError(
                        "Instagram follows_and_unfollows returned inconsistent cumulative totals "
                        f"for {target_date.isoformat()}."
                    )
                results[target_date] = daily_totals

        return results

    @staticmethod
    def _chunk_instagram_follow_dates(
        requested_dates: list[date],
        *,
        max_span_days: int = 82,
    ) -> list[list[date]]:
        """Chunk dates so a seven-day baseline keeps each Meta window under 90 days."""
        chunks: list[list[date]] = []
        for target_date in sorted(set(requested_dates)):
            if not chunks or (target_date - chunks[-1][0]).days > max_span_days:
                chunks.append([target_date])
            else:
                chunks[-1].append(target_date)
        return chunks

    @staticmethod
    def _parse_instagram_follow_breakdown(payload: dict) -> dict[str, int]:
        """Parse follows/unfollows totals from a total_value breakdown response."""
        totals: dict[str, int] = {}
        for metric_payload in payload.get("data", []):
            total_value = metric_payload.get("total_value") or {}
            for breakdown in total_value.get("breakdowns", []) or []:
                for result in breakdown.get("results", []) or []:
                    dimension_values = result.get("dimension_values") or []
                    follow_type = str(dimension_values[0] if dimension_values else "").upper()
                    value = int(float(result.get("value") or 0))
                    if follow_type == "FOLLOWER":
                        totals["followers"] = value
                    elif follow_type in {"NON_FOLLOWER", "UNFOLLOWER"}:
                        totals["unfollowers"] = value
        return totals

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

    def _facebook_metric_date(self, end_time: str | None) -> date | None:
        parsed = self._parse_instagram_datetime(end_time)
        if parsed is None:
            return None
        return (parsed - timedelta(days=1)).date()

    async def _resolve_facebook_page_access_token(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        user_access_token: str,
    ) -> str:
        """Resolve a Page Access Token from the stored Meta user token when possible."""
        stored_page_token = await self._load_managed_secret("facebook_page_access_token")
        if stored_page_token:
            return stored_page_token

        fallback_page_token = config("FB_PAGE_ACCESS_TOKEN", default="", cast=str).strip()
        if fallback_page_token:
            return fallback_page_token

        response = await client.get(
            f"{base_url}/{self.facebook_page_id}",
            params={
                "fields": "access_token",
                "access_token": user_access_token,
            },
        )
        if response.status_code < 400:
            page_token = response.json().get("access_token")
            if page_token:
                return page_token

        response = await client.get(
            f"{base_url}/me/accounts",
            params={
                "fields": "id,access_token",
                "access_token": user_access_token,
                "limit": 100,
            },
        )
        if response.status_code < 400:
            for page in response.json().get("data", []):
                if str(page.get("id")) == str(self.facebook_page_id) and page.get("access_token"):
                    return str(page["access_token"])

        raise ValueError(
            "Could not resolve a Facebook Page Access Token from `meta_ads_access_token`. "
            "Generate a token with `pages_show_list`, `pages_read_engagement`, and `read_insights`, "
            "or store `facebook_page_access_token` / `FB_PAGE_ACCESS_TOKEN`."
        )

    async def _fetch_facebook_page_metric_values(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        access_token: str,
        metric_name: str,
        start_date: date,
        end_date: date,
    ) -> dict[date, object]:
        """Fetch one Page Insights metric in safe date chunks."""
        values_by_date: dict[date, object] = {}
        for window_start, window_end in self._iter_date_windows(start_date, end_date, max_days=90):
            response = await client.get(
                f"{base_url}/{self.facebook_page_id}/insights",
                params={
                    "metric": metric_name,
                    "period": "day",
                    "since": window_start.isoformat(),
                    "until": (window_end + timedelta(days=1)).isoformat(),
                    "access_token": access_token,
                },
            )
            if response.status_code >= 400:
                return {}
            payload = response.json()
            for metric_payload in payload.get("data", []):
                for item in metric_payload.get("values", []):
                    metric_date = self._facebook_metric_date(item.get("end_time"))
                    if metric_date is not None:
                        values_by_date[metric_date] = item.get("value") or 0
        return values_by_date

    @staticmethod
    def _iter_date_windows(start_date: date, end_date: date, *, max_days: int):
        window_start = start_date
        while window_start <= end_date:
            window_end = min(window_start + timedelta(days=max_days - 1), end_date)
            yield window_start, window_end
            window_start = window_end + timedelta(days=1)

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

    @staticmethod
    def _raise_facebook_api_error(response: httpx.Response, context: str) -> None:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        error_payload = payload.get("error", {}) if isinstance(payload, dict) else {}
        if isinstance(error_payload, dict):
            message = error_payload.get("message") or error_payload.get("error_user_msg")
        else:
            message = None
        raise ValueError(f"Facebook {context} request failed ({response.status_code}): {message or 'Unknown error'}")

    @staticmethod
    def _facebook_summary_count(edge_payload: dict | None) -> int:
        summary = (edge_payload or {}).get("summary") or {}
        return int(float(summary.get("total_count") or 0))

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

    async def _fetch_instagram_media_insight_metrics(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        media_id: str,
        access_token: str,
    ) -> dict[str, int]:
        """Fetch lifetime media insight metrics, tolerating unavailable metrics."""
        if not media_id:
            return {}
        metric_aliases = {
            "shares": ("shares",),
            "saves": ("saves", "saved"),
            "reach": ("reach",),
            "impressions": ("impressions",),
            "plays": ("plays",),
        }
        totals: dict[str, int] = {}
        for target_name, candidates in metric_aliases.items():
            for metric in candidates:
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
                resolved = False
                for metric_payload in payload.get("data", []):
                    values = metric_payload.get("values") or []
                    value = values[0].get("value") if values else 0
                    totals[target_name] = int(float(value or 0))
                    resolved = True
                if resolved:
                    break
        return totals

    async def _fetch_facebook_post_insight_metrics(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        post_id: str,
        access_token: str,
    ) -> dict[str, int]:
        """Fetch lifetime Facebook post insight metrics, tolerating unavailable metrics."""
        if not post_id:
            return {}
        metric_aliases = {
            "post_media_view": ("post_media_view",),
            "post_clicks": ("post_clicks",),
            "post_video_views": ("post_video_views",),
            "post_reactions_by_type_total": ("post_reactions_by_type_total",),
        }
        totals: dict[str, int] = {}
        for target_name, candidates in metric_aliases.items():
            for metric in candidates:
                response = await client.get(
                    f"{base_url}/{post_id}/insights",
                    params={
                        "metric": metric,
                        "access_token": access_token,
                    },
                )
                if response.status_code >= 400:
                    continue
                payload = response.json()
                resolved = False
                for metric_payload in payload.get("data", []):
                    values = metric_payload.get("values") or []
                    value = values[0].get("value") if values else 0
                    if target_name == "post_reactions_by_type_total":
                        reaction_values = value if isinstance(value, dict) else {}
                        for reaction_key, column in {
                            "like": "reaction_like",
                            "love": "reaction_love",
                            "wow": "reaction_wow",
                            "haha": "reaction_haha",
                            "sorry": "reaction_sorry",
                            "anger": "reaction_anger",
                        }.items():
                            totals[column] = int(float(reaction_values.get(reaction_key) or 0))
                    else:
                        totals[target_name] = int(float(value or 0))
                    resolved = True
                if resolved:
                    break
        return totals

    async def _fetch_facebook_post_social_counts(
        self,
        *,
        client: httpx.AsyncClient,
        base_url: str,
        post_id: str,
        access_token: str,
    ) -> dict[str, int]:
        """Fetch optional Facebook post social counters without failing the media ETL."""
        if not post_id:
            return {}
        totals: dict[str, int] = {}
        for edge, target_name in (("likes", "likes"), ("comments", "comments")):
            response = await client.get(
                f"{base_url}/{post_id}/{edge}",
                params={
                    "summary": "true",
                    "limit": 0,
                    "access_token": access_token,
                },
            )
            if response.status_code >= 400:
                continue
            payload = response.json()
            totals[target_name] = self._facebook_summary_count(payload)
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
