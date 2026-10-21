"""Extract paginated acquisition facts and run the standard window pipeline."""
import asyncio
import math
from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

import pandas as pd
from sqlalchemy import insert

from app.db.models.ga4 import Ga4UserAcquisition
from app.etl.ga4.config import ALLOWED_SOURCES, DIMENSIONS, INTEGER_METRICS, KEY_COLUMNS, METRICS, SOURCE
from app.etl.load import _iter_row_chunks, delete_rows_in_date_window
from app.etl.pipeline_core import DateWindowPipelineRunner, DateWindowPipelineSpec
from app.etl.staging import stage_ads_raw
from app.etl.quality import MIN_HISTORICAL_DATE


if TYPE_CHECKING:
    from app.etl.extract import ExternalApiExtractor


async def fetch_acquisition(
    extractor: "ExternalApiExtractor", start_date: date, end_date: date,
) -> list[dict]:
    """Filter at the API and paginate; reject incomplete responses before loading."""
    if extractor.ga4_service is None or not extractor.ga4_property_id:
        raise ValueError("GA4 requires GA4_PROPERTY_ID and GA4_SA_CREDS.")

    def request():
        rows = []
        offset = 0
        while True:
            response = extractor.ga4_service.properties().runReport(
                property=f"properties/{extractor.ga4_property_id}",
                body={
                    "dateRanges": [{"startDate": start_date.isoformat(), "endDate": end_date.isoformat()}],
                    "dimensions": [{"name": name} for name in DIMENSIONS],
                    "metrics": [{"name": name} for name in METRICS],
                    "dimensionFilter": {"filter": {"fieldName": "firstUserSource", "inListFilter": {
                        "values": list(ALLOWED_SOURCES), "caseSensitive": True}}},
                    "orderBys": [{"dimension": {"dimensionName": name}} for name in DIMENSIONS],
                    "limit": 10000, "offset": offset,
                },
            ).execute(num_retries=3)
            page = response.get("rows", [])
            for row in page:
                values = row.get("dimensionValues", []) + row.get("metricValues", [])
                if len(row.get("dimensionValues", [])) != len(DIMENSIONS) or len(row.get("metricValues", [])) != len(METRICS):
                    raise ValueError("DQ failed: incomplete GA4 acquisition row.")
                rows.append(dict(zip((*DIMENSIONS.values(), *METRICS.values()),
                                     (value.get("value") for value in values))))
            offset += len(page)
            if offset >= int(response.get("rowCount", 0)):
                return rows
            if not page:
                raise ValueError("DQ failed: GA4 acquisition pagination ended prematurely.")

    return await asyncio.to_thread(request)


def parse_acquisition(raw_rows: list[dict]) -> pd.DataFrame:
    """Validate original values before any coercion can hide data-quality issues."""
    if not raw_rows:
        return pd.DataFrame()
    df = pd.DataFrame(raw_rows)
    required = set(KEY_COLUMNS) | set(METRICS.values())
    if required - set(df.columns):
        raise ValueError("DQ failed: missing GA4 acquisition columns.")
    if df[list(KEY_COLUMNS)].isna().any().any():
        raise ValueError("DQ failed: missing GA4 acquisition business keys.")
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").dt.date
    if df["date"].isna().any() or (df["date"] < MIN_HISTORICAL_DATE).any():
        raise ValueError("DQ failed: invalid GA4 acquisition dates.")
    if not df["first_user_source"].isin(ALLOWED_SOURCES).all():
        raise ValueError("DQ failed: unexpected GA4 first user source.")
    for column in METRICS.values():
        values = pd.to_numeric(df[column], errors="coerce")
        if not values.map(lambda value: math.isfinite(value) and value >= 0).all():
            raise ValueError(f"DQ failed: invalid GA4 acquisition metric {column}.")
        if column in INTEGER_METRICS:
            if (values % 1 != 0).any():
                raise ValueError(f"DQ failed: fractional GA4 count {column}.")
            values = values.astype('int64')
        df[column] = values
    validate_acquisition(df)
    return df


def validate_acquisition(df: pd.DataFrame) -> None:
    """Enforce report grain and rate bounds before window replacement."""
    if df.empty:
        return
    if (df["user_key_event_rate"] > 1).any():
        raise ValueError("DQ failed: GA4 user key event rate exceeds 1.")
    if df.duplicated(list(KEY_COLUMNS)).any():
        raise ValueError("DQ failed: duplicate GA4 acquisition business key.")


async def run_acquisition(
    extractor: "ExternalApiExtractor", session: AsyncSession, types: str = "auto",
    start_date: date | None = None, end_date: date | None = None, run_id: str | None = None,
) -> str:
    """Replace daily facts only after all pages pass validation; staging survives failure."""
    async def extract(start, end):
        return await fetch_acquisition(extractor, start, end)

    async def stage(session, rows, run_id):
        return await stage_ads_raw(session, rows, run_id=run_id, source=SOURCE, range_name=SOURCE)

    async def delete_window(session, start, end):
        return await delete_rows_in_date_window(session, Ga4UserAcquisition, window_start=start, window_end=end)

    async def load(session, rows):
        for chunk in _iter_row_chunks(rows, len(KEY_COLUMNS) + len(METRICS) + 1):
            await session.execute(insert(Ga4UserAcquisition), chunk)

    spec = DateWindowPipelineSpec(
        label=SOURCE, source=SOURCE, empty_metric_name="GA4 acquisition", date_column="date",
        auto_skip_model=Ga4UserAcquisition, extract=extract, stage=stage, parse=parse_acquisition,
        validate=validate_acquisition,
        build_rows=lambda df, pull_date: df.assign(pull_date=pull_date).to_dict("records"),
        delete_window=delete_window, load_rows=load,
    )
    return await DateWindowPipelineRunner()._run_date_window_pipeline(
        spec=spec, session=session, types=types, start_date=start_date, end_date=end_date, run_id=run_id)
