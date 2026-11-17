"""Monthly tool allocations from the Summary - Cost spreadsheet."""
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import insert

from app.db.models.external_api import ToolsFixedCost
from app.etl.load import delete_rows_in_date_window
from app.etl.pipeline_core import DateWindowPipelineSpec
from app.etl.staging import stage_ads_raw
from app.etl.transform import resolve_date_window

SOURCE = "tools_fixed_cost"


def resolve_tools_fixed_cost_window(types, start_date, end_date):
    if types == "auto":
        current_month = date.today().replace(day=1)
        return (current_month - timedelta(days=1)).replace(day=1), current_month
    start, end = resolve_date_window(types, start_date, end_date)
    return start.replace(day=1), end.replace(day=1)


def _parse_tools_fixed_cost(rows: list) -> pd.DataFrame:
    if not rows:
        raise ValueError("Summary - Cost contains no rows.")

    # Monthly date headers start in D. The annual total is a text header.
    month_columns = {}
    for column, value in enumerate(rows[0][3:], start=3):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            month = date(1899, 12, 30) + timedelta(days=value)
            if value % 1 or month.day != 1 or month in month_columns.values():
                raise ValueError("Summary - Cost has an invalid or duplicate month header.")
            month_columns[column] = month
    if not month_columns:
        raise ValueError("Summary - Cost has no monthly date headers.")

    records = []
    seen_tools = set()
    in_tools = False
    for row in rows[1:]:
        name = str(row[0] or "").strip() if row else ""
        if not in_tools:
            in_tools = name.casefold() == "tools"
            continue
        if not name:
            break
        if name.casefold() in seen_tools:
            raise ValueError(f"Summary - Cost has a duplicate tool: {name}.")
        seen_tools.add(name.casefold())
        for column, month in month_columns.items():
            amount = row[column] if column < len(row) else None
            if amount is None or amount == "":
                continue
            if isinstance(amount, bool) or not isinstance(amount, (int, float)) or not 0 <= amount < float("inf"):
                raise ValueError(f"Invalid cost for {name} in {month:%Y-%m}.")
            records.append({"date": month, "tool_name": name, "amount_idr": amount})

    if not seen_tools:
        raise ValueError("Summary - Cost has no tool detail rows.")
    return pd.DataFrame(records, columns=["date", "tool_name", "amount_idr"])


async def run_tools_fixed_cost(gsheet, session, types, start_date, end_date, run_id):
    async def extract(_start, _end):
        return await gsheet.extractor.fetch_tools_fixed_cost_rows()

    async def stage(session_, rows, run_id_):
        return await stage_ads_raw(
            session_, [{"row": row} for row in rows], run_id=run_id_,
            source=SOURCE, range_name=gsheet.extractor.summary_cost_sheet_range,
        )

    async def delete_window(session_, first_month, last_month):
        return await delete_rows_in_date_window(
            session_, ToolsFixedCost, window_start=first_month, window_end=last_month,
        )

    async def load_rows(session_, rows):
        if rows:
            await session_.execute(insert(ToolsFixedCost), rows)

    return await gsheet._run_date_window_pipeline(
        spec=DateWindowPipelineSpec(
            label=SOURCE, source=SOURCE, empty_metric_name="Tools Fixed Cost",
            date_column="date", auto_skip_model=ToolsFixedCost,
            extract=extract, stage=stage, parse=_parse_tools_fixed_cost,
            validate=lambda _: None,
            build_rows=lambda frame, pull_date: frame.assign(pull_date=pull_date).to_dict("records"),
            delete_window=delete_window, load_rows=load_rows,
            resolve_window=resolve_tools_fixed_cost_window,
        ),
        session=session, types=types, start_date=start_date, end_date=end_date, run_id=run_id,
    )
