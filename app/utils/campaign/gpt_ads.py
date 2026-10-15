"""Adapt GPT Ads reporting grains to the existing UA analytics contract."""
import asyncio
import pandas as pd
from sqlalchemy import select
from app.db.models.gpt_ads import GptAdsDaily


class GptAdsCampaignAdapter:
    def __init__(self, *, execute_query, cache, read_register, allocator):
        self.execute_query = execute_query
        self.cache = cache
        self.read_register = read_register
        self.allocator = allocator
        self._load_lock = asyncio.Lock()

    async def frame(self, start, end, report_level="campaign"):
        # UA builds many charts concurrently; load each grain/window once.
        async with self._load_lock:
            return await self._read_frame(start, end, report_level)

    async def _read_frame(self, start, end, report_level):
        key = self.cache.make_key("gpt_ads_ua", report_level, start, end)
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        model = GptAdsDaily
        columns = ["date", "account_id", "campaign_id", "campaign_name", "ad_group_id",
                   "ad_group_name", "ad_id", "ad_name", "report_level", "currency", "timezone", "spend", "impressions", "clicks"]
        result = await self.execute_query(select(*(getattr(model, name) for name in columns)).where(
            model.date.between(start, end), model.report_level == report_level))
        frame = pd.DataFrame([dict(row._mapping) for row in result.fetchall()], columns=columns)
        frame = frame.rename(columns={"spend": "cost", "ad_group_name": "ad_group"})
        frame["campaign_source"] = "gpt_ads"
        frame["campaign_type"] = frame["campaign_name"].fillna("").str.upper().apply(
            lambda name: "user_acquisition" if "- UA -" in name else "unknown")
        for metric in ("cost", "impressions", "clicks"):
            frame[metric] = pd.to_numeric(frame[metric], errors="coerce").fillna(0)
        for dimension in ("campaign_name", "ad_group", "ad_name"):
            frame[dimension] = frame[dimension].fillna("N/A").replace("", "N/A")
        if not frame.empty:
            for dimension, id_column in (("ad_group", "ad_group_id"), ("ad_name", "ad_id")):
                identities = frame[[dimension, "account_id", "campaign_id", id_column]].drop_duplicates()
                repeated = identities[dimension].value_counts()
                ambiguous = frame[dimension].isin(repeated[repeated > 1].index)
                if ambiguous.any():
                    frame.loc[ambiguous, dimension] = frame.loc[ambiguous].apply(
                        lambda row: f"{row[dimension]} ({row[id_column]} / {row['campaign_id']})", axis=1)
            # Current UA reporting is in IDR/WIB; never silently relabel other units.
            if set(frame["currency"]) != {"IDR"} or set(frame["timezone"]) != {"Asia/Jakarta"}:
                raise ValueError("GPT Ads UA reporting requires IDR currency and Asia/Jakarta timezone.")
        activity = await self.read_register(from_date=start, to_date=end)
        frame = self.allocator.attach_activity_leads(frame, activity)
        return self.cache.set(key, frame)

    async def details(self, start, end, *, report_level=None, ad_type=None):
        levels = [report_level] if report_level else ["campaign", "ad_group", "ad"]
        frames = [await self.frame(start, end, level) for level in levels]
        frame = pd.concat(frames, ignore_index=True)
        if ad_type:
            frame = frame.loc[frame["campaign_type"] == ad_type]
        dimensions = ["account_id", "report_level", "campaign_source", "campaign_id", "campaign_name",
                      "ad_group_id", "ad_group", "ad_id", "ad_name"]
        if frame.empty:
            return pd.DataFrame(columns=dimensions + ["spend", "impressions", "clicks", "leads"])
        return frame.groupby(dimensions, dropna=False, as_index=False)[["cost", "impressions", "clicks", "leads"]].sum().rename(columns={"cost": "spend"}).fillna({"ad_group_id": "", "ad_id": ""})
