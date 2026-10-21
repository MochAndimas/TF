"""Run OIS through the shared GSheet staging and atomic date-window runner."""
from datetime import date

from sqlalchemy import delete
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.ois import OisRegistration
from app.etl.load import _iter_row_chunks, _normalize_sql_value
from app.etl.ois.transform import parse_rows, validate_rows
from app.etl.pipeline_core import DateWindowPipelineRunner, DateWindowPipelineSpec
from app.etl.staging import stage_ads_raw

SOURCE = "ois_registrations"


def build_rows(df, pull_date: date) -> list[dict]:
    return [{**{key: _normalize_sql_value(value) for key, value in row.items()}, "pull_date": pull_date}
            for row in df.to_dict("records")]


async def upsert_rows(session: AsyncSession, rows: list[dict]) -> None:
    if not rows:
        return
    for chunk in _iter_row_chunks(rows, len(rows[0])):
        statement = insert(OisRegistration).values(chunk)
        await session.execute(statement.on_conflict_do_update(
            index_elements=["tf_id"],
            set_={field: getattr(statement.excluded, field) for field in rows[0] if field != "tf_id"},
        ))


async def run_ois(extractor, session: AsyncSession, types="auto", start_date=None, end_date=None, run_id=None):
    async def extract(_start, _end):
        return await extractor.fetch_ois_rows()

    async def stage(session, rows, run_id):
        return await stage_ads_raw(session, rows, run_id=run_id, source=SOURCE,
                                   range_name=extractor.ois_sheet_range)

    async def delete_window(session, start, end):
        result = await session.execute(delete(OisRegistration).where(
            OisRegistration.registration_date.between(start, end)))
        return int(result.rowcount or 0)

    return await DateWindowPipelineRunner()._run_date_window_pipeline(
        spec=DateWindowPipelineSpec(
            label=SOURCE, source=SOURCE, empty_metric_name="OIS registrations", date_column="registration_date",
            auto_skip_model=OisRegistration, extract=extract, stage=stage, parse=parse_rows, validate=validate_rows,
            build_rows=build_rows, delete_window=delete_window, load_rows=upsert_rows,
        ), session=session, types=types, start_date=start_date, end_date=end_date, run_id=run_id,
    )
