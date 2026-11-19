"""Verify paid app attribution mapping and shared-table replacement isolation."""
import asyncio
from datetime import date

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import SqliteBase
import app.db.models  # noqa: F401
from app.db.models.external_api import DataDepo
from app.etl.load import upsert_first_deposit_rows, delete_first_deposit_rows_in_window

@pytest.fixture
def db_session_factory(tmp_path):
    """Return an isolated async-session factory backed by a temp SQLite file."""

    async def make_async_factory():
        db_path = tmp_path / "etl_integration.db"
        engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
        async with engine.begin() as connection:
            await connection.run_sync(SqliteBase.metadata.create_all)
        return engine, sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)

    engine, factory = asyncio.run(make_async_factory())
    try:
        yield factory
    finally:
        asyncio.run(engine.dispose())


def test_app_first_deposit_maps_exact_sources_and_campaigns():
    from app.etl.transform import parse_app_first_deposit_dataframe
    base = {"id": "10", "email": "test@example.com", "tgl_regis": "2026-11-01",
            "campaign_id": "120250377992030267", "Status\nNew / Existing": "New",
            "First Depo $": "25.50", "tracker_token": "ignored"}
    rows = [{**base, "id": str(i), "source_name": source}
            for i, source in enumerate((" Google ", "facebook", "off_facebook", "organic",
                                        "tiktok", "facebook_extra", "", None), 1)]
    parsed = parse_app_first_deposit_dataframe(rows)
    assert list(parsed.tag) == ["google", "facebook", "off_facebook"]
    assert list(parsed.campaign_id) == ["120250377992030267"] * 3
    assert list(parsed.first_depo) == [25.5] * 3
    assert "tracker_token" not in parsed.columns
    assert list(parsed.protection) == [0, 0, 0]
    assert parse_app_first_deposit_dataframe([{**base, "source_name": "google", "First Depo $": "0"}]).empty


def test_app_first_deposit_rejects_missing_attribution_columns():
    from app.etl.transform import parse_app_first_deposit_dataframe
    with pytest.raises(ValueError, match="Missing app first deposit columns"):
        parse_app_first_deposit_dataframe([{"id": "1"}])

def test_first_deposit_source_replacement_preserves_other_tab(db_session_factory):
    from app.etl.load import build_first_deposit_rows
    from app.etl.transform import parse_first_deposit_dataframe

    async def run_test():
        async with db_session_factory() as session:
            raw = {"id": "1", "email": "a@example.com", "tgl_regis": "2026-11-01",
                   "campaignid": "C1", "Status\nNew / Existing": "New", "First Depo $": "10"}
            frame = parse_first_deposit_dataframe(
                [{**raw, "id": str(i), "tag": tag} for i, tag in enumerate(
                    ("CP1Google", "google", "facebook", "off_facebook"), 1)],
                tag_pattern=r".*",
            )
            rows = build_first_deposit_rows(frame, date(2026, 11, 2))
            await upsert_first_deposit_rows(session, rows)
            await session.commit()
            await delete_first_deposit_rows_in_window(
                session, window_start=date(2026, 11, 1), window_end=date(2026, 11, 1))
            assert set((await session.scalars(select(DataDepo.tag))).all()) == {
                "google", "facebook", "off_facebook"}
            await upsert_first_deposit_rows(session, [rows[0]])
            await delete_first_deposit_rows_in_window(
                session, window_start=date(2026, 11, 1), window_end=date(2026, 11, 1), app_source=True)
            assert (await session.scalars(select(DataDepo.tag))).all() == ["CP1Google"]
            await session.commit()
    asyncio.run(run_test())


def test_app_pipeline_is_idempotent_and_keeps_raw_regis(db_session_factory):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from app.etl.pipelines import GoogleSheetApi
    from app.etl.pipeline_core import DateWindowPipelineRunner
    from app.etl.transform import parse_first_deposit_dataframe
    from app.etl.load import build_first_deposit_rows

    async def run_test():
        base = {"id": "10", "email": "a@example.com", "tgl_regis": "2026-11-01",
                "campaign_id": "123", "Status\nNew / Existing": "New", "First Depo $": "25"}
        extractor = SimpleNamespace(fetch_app_first_deposit_records=AsyncMock(return_value=[
            {**base, "source_name": "google"},
            {**base, "id": "11", "source_name": "organic"},
        ]))
        pipeline = GoogleSheetApi.__new__(GoogleSheetApi)
        DateWindowPipelineRunner.__init__(pipeline)
        pipeline.extractor = extractor
        async with db_session_factory() as session:
            raw = parse_first_deposit_dataframe([
                {**base, "id": "9", "campaignid": "123", "tag": "CP1Google"}])
            await upsert_first_deposit_rows(session, build_first_deposit_rows(raw, date(2026, 11, 2)))
            await session.commit()
            for _ in range(2):
                await pipeline.app_first_deposit(session, date(2026, 11, 1), date(2026, 11, 1), "manual")
            rows = (await session.scalars(select(DataDepo))).all()
            assert sorted((r.user_id, r.tag, r.first_depo) for r in rows) == [
                (9, "CP1Google", 25.0), (10, "google", 25.0)]
            extractor.fetch_app_first_deposit_records.return_value = []
            await pipeline.app_first_deposit(session, date(2026, 11, 1), date(2026, 11, 1), "manual")
            assert (await session.scalars(select(DataDepo.tag))).all() == ["CP1Google"]
    asyncio.run(run_test())
