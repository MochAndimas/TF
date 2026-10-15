"""Read-only Ads API client with bounded retry and cursor validation."""
import asyncio
import json
from datetime import timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import httpx
from fastapi import HTTPException
from app.core.config import settings
from app.etl.gpt_ads.config import report_fields

BASE_URL = "https://api.ads.openai.com/v1"


class GptAdsClient:
    def __init__(self, *, api_key=None, transport=None, sleep=asyncio.sleep):
        key = api_key if api_key is not None else settings.GPT_ADS_API_KEY
        if not key or not key.strip():
            raise HTTPException(503, "GPT_ADS_API_KEY is not configured")
        self.http = httpx.AsyncClient(base_url=BASE_URL, headers={"Authorization": f"Bearer {key.strip()}"},
                                      timeout=httpx.Timeout(60, connect=15), transport=transport,
                                      follow_redirects=False)
        self.sleep = sleep

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.http.aclose()

    async def get(self, path, params=None):
        for attempt in range(4):
            try:
                response = await self.http.get(path, params=params)
            except httpx.RequestError:
                if attempt == 3:
                    raise HTTPException(502, "GPT Ads API connection failed after retries") from None
                await self.sleep(2 ** attempt)
                continue
            if response.status_code in {408, 429} or response.status_code >= 500:
                if attempt == 3:
                    raise HTTPException(502, f"GPT Ads API unavailable (HTTP {response.status_code})")
                try:
                    delay = max(0, min(float(response.headers.get("Retry-After", 2 ** attempt)), 60))
                except ValueError:
                    delay = 2 ** attempt
                await self.sleep(delay)
                continue
            if not response.is_success:
                # Never forward upstream bodies/headers that could expose credentials.
                raise HTTPException(502, f"GPT Ads API rejected report request (HTTP {response.status_code})")
            try:
                payload = response.json()
            except ValueError:
                raise HTTPException(502, "GPT Ads API returned invalid JSON") from None
            if not isinstance(payload, dict):
                raise HTTPException(502, "GPT Ads API returned an invalid response object")
            return payload

    async def account(self):
        account = await self.get("/ad_account")
        if not all(isinstance(account.get(k), str) and account[k].strip() for k in ("id", "currency_code", "timezone")):
            raise HTTPException(502, "GPT Ads API account identity, currency or timezone is missing")
        try:
            ZoneInfo(account["timezone"])
        except ZoneInfoNotFoundError:
            raise HTTPException(502, "GPT Ads API returned an invalid account timezone") from None
        if account["timezone"] != settings.GPT_ADS_REPORTING_TIMEZONE:
            raise HTTPException(422, "GPT_ADS_REPORTING_TIMEZONE must match the ad account timezone")
        return account

    async def report(self, level, start, end, timezone):
        """Yield bounded pages, splitting long windows to keep conversion reports valid."""
        chunk_start = start
        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=30), end)
            params = [("aggregation_level", level), ("time_granularity", "daily"),
                      ("time_ranges[]", json.dumps(dict(type="date_range", since=chunk_start.isoformat(), until=chunk_end.isoformat(), timezone=timezone))),
                      ("limit", "500"),
                      ("attribution_window_days", "30"), ("view_through_attribution_window_days", "1"),
                      ("attribution_time_basis", "ad_event_time")]
            params += [("fields[]", field) for field in report_fields(level)]
            cursor = None
            seen = set()
            for _ in range(10000):
                payload = await self.get("/ad_account/insights", params + ([("after", cursor)] if cursor else []))
                rows = payload.get("data")
                if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows) or not isinstance(payload.get("has_more"), bool):
                    raise HTTPException(502, "GPT Ads API returned an invalid report page")
                yield rows, chunk_start, chunk_end
                if not payload["has_more"]:
                    break
                cursor = payload.get("last_id")
                if not rows or not isinstance(cursor, str) or not cursor or cursor in seen:
                    raise HTTPException(502, "GPT Ads API pagination did not advance")
                seen.add(cursor)
            else:
                raise HTTPException(502, "GPT Ads API pagination limit exceeded")
            chunk_start = chunk_end + timedelta(days=1)
