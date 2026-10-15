# GPT Ads reporting ETL

The `gpt_ads` source reads ChatGPT Advertiser API reports at campaign, ad_group and ad aggregation levels. It plugs into the existing Update Data endpoint, sequential scheduler, single-run ETL lock and etl_run history. It does not instantiate GoogleSheetApi or require Google credentials.

## Configuration

- `GPT_ADS_API_KEY`: Advertiser API key scoped to one ad account; supports `GPT_ADS_API_KEY_FILE`. Keep it server-side and out of version control.
- `GPT_ADS_REPORTING_TIMEZONE`: defaults to `Asia/Jakarta`; must match the account timezone because conversion reports require complete account reporting days.
- `GPT_ADS_LOOKBACK_DAYS`: defaults to 31 completed days, valid from 1 to 365. Auto refresh reconciles delayed conversions. Older conversion updates outside this lookback require manual reconciliation.

Use Settings → Update Data → **GPT Ads (Campaign / Ad Group / Ads)**. Manual dates are inclusive, must be complete days, within the last 365 days, and span at most 365 days. Auto refreshes H-31 through H-1 by default in the account timezone. `run_scheduled_etl.py --sources gpt_ads` uses the same runner; the source is also in the standard daily source list.

## Storage and metric semantics

All report levels are stored in **`gpt_ads_daily`**. `report_level` is `campaign`, `ad_group` or `ad`. The primary key is **account_id + date + report_level + entity_id**.

The table contains parent IDs/names, account currency/timezone, impressions, clicks, spend, ctr, cpc, cpm, conversions, attribution settings, run_id and updated_at. Lower-level IDs are NULL where inapplicable. Date is the **ad account reporting date**, not a forced conversion to another timezone. updated_at is UTC.

CTR is stored as a ratio (0.04 = 4%). Monetary fields use major currency units, not budget micros. Preserve API-returned CTR/CPC/CPM rather than recomputing them from recent delivery metrics. Decimal fields use SQLAlchemy Numeric (SQLite numeric affinity); this is suitable for reporting, not an arbitrary-precision financial ledger. Missing nullable metrics remain NULL; missing requested columns fail validation. Conversions are campaign goal conversions with 30-day click, 1-day view, ad_event_time attribution. Goal definitions depend on the ad account configuration; this is not a count of every custom event.

Always filter `report_level` when aggregating. Do not add campaign, ad group and ad totals together: these are three views of the same delivery. Spend/currency cannot be mixed across accounts without currency conversion. Existing Google/Meta/TikTok aggregation and unique_campaign are not modified by this ETL.

## Reliability

1. Verify account identity, currency and matching timezone.
2. Pull daily reports using 31-day request chunks and cursor pagination. Retry network errors, 408, 429 and 5xx up to four attempts; other upstream errors fail the run without logging response bodies or credentials.
3. Stage raw report records in stg_ads_raw with source=gpt_ads, level, account_id, run_id and the existing staging retention policy. Each completed page is committed before normalization, retaining audit data even on a later failure.
4. Validate daily identity, date range, timezone, nonnegative finite metrics and unique grain. Spool normalized data to temporary disk above 8 MB.
5. Only after all three reports succeed, replace the target account/date window in gpt_ads_daily in one transaction. A complete empty report clears that account/window; malformed, partial or failed extraction does not clear existing facts. Loading failure rolls back the whole reporting window. Other accounts and dates are preserved.

Zero-impression expansion is not requested; there is no synthesized daily zero row for every configured entity. API pages must have data[] and boolean has_more; advancing pages must provide a new last_id. Request formats use canonical fields such as campaign.id and campaign.impressions; returned flat keys are normalized.

Tests: `python -m unittest scripts.verification.test_gpt_ads`.

Official references:
- https://developers.openai.com/ads/api-reference/authentication
- https://developers.openai.com/ads/api-reference/insights
- https://developers.openai.com/ads/reporting


## User Acquisition dashboard

Campaign → User Acquisition includes **GPT Ads** in Performance Source. A focused campaign adapter reads gpt_ads_daily without requiring a Campaign registry row. Names containing `- UA -` (case-insensitive) are included; other campaigns are excluded from UA metrics.

Summary cards and overview charts read `report_level=campaign`. Dimension charts read the matching campaign/ad_group/ad grain. Detail payload includes report_level; the UI selects the matching level before grouping. The three reporting levels are never summed together. Duplicate ad/group names are disambiguated with their IDs. Parent/account IDs are retained in detail aggregation.

Register uses the existing DailyRegister source matched on reporting date and campaign_id. The existing spend-weighted registration allocation is applied independently within each reporting level; when no internal registration matches, Register is zero. API conversions are not renamed to Register. Currency and reporting timezone must be IDR/Asia-Jakarta to match the existing page's currency/date presentation.

GPT adapter frames are cached by level and range, with serialized loading for concurrently built charts. Successful GPT ETL updates invalidate the campaign dataframe cache. Existing non-GPT sources, Brand Awareness and Remarketing source lists remain unchanged.
