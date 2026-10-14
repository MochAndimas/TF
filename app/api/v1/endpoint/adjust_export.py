"""Protected, allowlisted exports of Adjust facts and raw callbacks."""
import csv
import json
from datetime import date, datetime, time, timedelta, timezone
from tempfile import SpooledTemporaryFile
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import get_current_user
from app.db.models.user import TfUser
from app.db.models.adjust import (AdjustCallbackRaw, AdjustInstall, AdjustLifecycle,
    AdjustAttributionHistory, AdjustEngagement, AdjustCost, AdjustRegister,
    AdjustDeposit, AdjustFirstDeposit, AdjustSubscribeSuccess)
from app.db.session import get_db

router = APIRouter(prefix="/api/adjust-export")
async def export_admin(current_user: Annotated[TfUser, Depends(get_current_user)]):
    # Do not use legacy role aliases: admin is otherwise expanded to analyst.
    if (current_user.role or "").strip().lower() not in {"admin", "superadmin"}:
        raise HTTPException(403, "Admin access required")
    return current_user


Admin = Annotated[TfUser, Depends(export_admin)]
Database = Annotated[AsyncSession, Depends(get_db)]
MODELS = {model.__tablename__: model for model in (AdjustInstall,
    AdjustLifecycle, AdjustAttributionHistory, AdjustEngagement, AdjustCost,
    AdjustRegister, AdjustDeposit, AdjustFirstDeposit, AdjustSubscribeSuccess)}
WIB = ZoneInfo("Asia/Jakarta")


def export_query(table, start_date=None, end_date=None):
    model = MODELS.get(table)
    if model is None:
        raise HTTPException(400, "Unknown Adjust table")
    if bool(start_date) != bool(end_date) or (start_date and start_date > end_date):
        raise HTTPException(400, "Choose a valid start and end date")
    column = model.received_at if model is AdjustCallbackRaw else model.date
    filters = []
    if start_date:
        if end_date == date.max:
            raise HTTPException(400, "End date is out of range")
        if model is AdjustCallbackRaw:
            lower = datetime.combine(start_date, time.min, WIB).astimezone(timezone.utc).replace(tzinfo=None)
            upper = datetime.combine(end_date + timedelta(days=1), time.min, WIB).astimezone(timezone.utc).replace(tzinfo=None)
            filters = [column >= lower, column < upper]
        else:
            filters = [column >= start_date, column <= end_date]
    return model, column, filters


def display_date(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).astimezone(WIB).date().isoformat()
    return value.isoformat() if value else None


async def stats(session, table, start_date=None, end_date=None):
    model, column, filters = export_query(table, start_date, end_date)
    count, lower, upper = (await session.execute(select(func.count(), func.min(column), func.max(column)).select_from(model).where(*filters))).one()
    return dict(table=table, row_count=count, data_start=display_date(lower), data_end=display_date(upper),
                date_basis="received_at (WIB)" if model is AdjustCallbackRaw else "date (WIB)",
                start_date=start_date, end_date=end_date)


@router.get("/tables")
async def tables(current_user: Admin, session: Database):
    return {"tables": [await stats(session, table) for table in MODELS]}


@router.get("/summary")
async def summary(current_user: Admin, session: Database, table: str, start_date: date, end_date: date):
    return await stats(session, table, start_date, end_date)


def csv_value(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    # Spreadsheet applications must not execute user-supplied strings as formulas.
    text = str(value)
    return "'" + text if text.lstrip().startswith(("=", "+", "-", "@", "\t", "\r", "\n")) else text


@router.get("/csv")
async def export_csv(current_user: Admin, session: Database, table: str, start_date: date, end_date: date):
    model, column, filters = export_query(table, start_date, end_date)
    file = SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+", encoding="utf-8", newline="")
    try:
        file.write("\ufeff")
        writer = csv.writer(file)
        columns = list(model.__table__.columns)
        writer.writerow([c.name for c in columns])
        result = await session.stream(select(*columns).where(*filters).order_by(column, *model.__table__.primary_key.columns))
        count = 0
        async for batch in result.partitions(500):
            for row in batch:
                writer.writerow([csv_value(v) for v in row])
                count += 1
        await result.close()
        await session.rollback()  # Release the SQLite read snapshot before serving the file.
        file.seek(0)
    except BaseException:
        file.close()
        raise

    def chunks():
        try:
            while chunk := file.read(64 * 1024):
                yield chunk.encode("utf-8")
        finally:
            file.close()
    filename = f"{table}_{start_date}_{end_date}.csv"
    return StreamingResponse(chunks(), media_type="text/csv; charset=utf-8", headers={
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Export-Row-Count": str(count), "Cache-Control": "no-store"})
