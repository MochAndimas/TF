"""Append register facts with durable receipts and recovery after ambiguous writes.

Runs under the ETL runner's single-run lock. Never retry an append blindly:
reconcile the sheet first on the next run if the response was lost.
"""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import hashlib
import re

from decouple import config
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import httplib2
from sqlalchemy import select

from app.db.models.adjust import AdjustRegister
from app.db.models.adjust_sheet import AdjustRegisterSheetReceipt
from app.etl.extract_helpers import load_service_account_info

HEADERS = ["id", "email", "phone", "fullname", "tgl_regis", "tracker_token", "campaign_id", "source_name"]
FIELDS = ["tf_id", "email", "nomor_telepon", "username", "date", "tracker_token", "campaign_id", "source_name"]
BATCH_SIZE = 200


def row_values(row):
    return ["" if row[field] is None else str(row[field]) for field in FIELDS]


class RegisterSheet:
    def __init__(self, spreadsheet_id, range_name):
        match = re.fullmatch(r"(?:'((?:[^']|'')+)'|([^'!]+))!A:H", range_name)
        if not match:
            raise ValueError("APP_INSTALL_GSHEET_RANGE must be an explicit tab followed by !A:H")
        self.title = (match[1] or match[2]).replace("''", "'")
        self.range = "'" + self.title.replace("'", "''") + "'!A:H"
        self.spreadsheet_id = spreadsheet_id
        credentials = Credentials.from_service_account_info(
            load_service_account_info("GSHEET_SA_CREDS"),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
        from google_auth_httplib2 import AuthorizedHttp
        self.service = build("sheets", "v4", http=AuthorizedHttp(credentials, http=httplib2.Http(timeout=60)),
                             cache_discovery=False)

    def read(self):
        meta = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id,
                                             fields="sheets.properties").execute()
        self.sheet_id = next(item["properties"]["sheetId"] for item in meta["sheets"]
                             if item["properties"]["title"] == self.title)
        values = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=self.range,
            valueRenderOption="UNFORMATTED_VALUE").execute().get("values", [])
        if not values or values[0] != HEADERS:
            raise ValueError("Register sheet headers do not match the configured eight columns")
        return [tuple(str(cell) for cell in (row + [""] * 8)[:8]) for row in values[1:]]

    def format_data(self, end_row):
        if end_row < 2:
            return
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": [{
            "repeatCell": {"range": {"sheetId": self.sheet_id, "startRowIndex": 1,
                "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": {
                    "red": 1, "green": 1, "blue": 1}}, "textFormat": {"bold": False}}},
                "fields": "userEnteredFormat.backgroundColorStyle,userEnteredFormat.textFormat.bold"}}]
        }).execute(num_retries=0)

    def append(self, rows):
        result = self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id, range=self.range, valueInputOption="RAW",
            insertDataOption="INSERT_ROWS", body={"values": rows}).execute(num_retries=0)
        updated = result["updates"]["updatedRange"]
        actual = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=updated,
            valueRenderOption="UNFORMATTED_VALUE").execute().get("values", [])
        if [(row + [""] * 8)[:8] for row in actual] != rows:
            raise ValueError("Register sheet read-back did not match appended data")
        return int(re.search(r"(\d+)$", updated)[1])


async def sync_register_sheet(session, *, client=None):
    sheet_id = config("FIRST_DEPOSIT_SHEET_ID", default="").strip()
    range_name = config("APP_INSTALL_GSHEET_RANGE", default="").strip()
    if not range_name:
        return "Register sheet sync disabled (destination not configured)."
    if not sheet_id or not range_name:
        raise ValueError("Both FIRST_DEPOSIT_SHEET_ID and APP_INSTALL_GSHEET_RANGE are required")
    destination = hashlib.sha256(f"{sheet_id}:{range_name}".encode()).hexdigest()
    cursor = ""
    client = client or await asyncio.to_thread(RegisterSheet, sheet_id, range_name)
    existing = Counter(await asyncio.to_thread(client.read))
    # Repair formatting even if an earlier append succeeded but formatting failed.
    await asyncio.to_thread(client.format_data, sum(existing.values()) + 1)
    delivered = appended = 0
    while True:
        rows = (await session.execute(select(AdjustRegister.event_key, *[
            getattr(AdjustRegister, field) for field in FIELDS]).where(
                AdjustRegister.event_key > cursor).order_by(AdjustRegister.event_key).limit(BATCH_SIZE))).mappings().all()
        if not rows:
            await session.commit()
            break
        keys = [row["event_key"] for row in rows]
        receipts = set((await session.scalars(select(AdjustRegisterSheetReceipt.event_key).where(
            AdjustRegisterSheetReceipt.destination == destination,
            AdjustRegisterSheetReceipt.event_key.in_(keys)))).all())
        await session.commit()  # No SQLite transaction is held during network requests.
        pending = []
        for row in rows:
            values = row_values(row)
            if row["event_key"] in receipts:
                if existing[tuple(values)] > 0:
                    existing[tuple(values)] -= 1
                continue
            if existing[tuple(values)] > 0:
                existing[tuple(values)] -= 1
            else:
                pending.append(values)
        if pending:
            end_row = await asyncio.to_thread(client.append, pending)
            await asyncio.to_thread(client.format_data, end_row)
            appended += len(pending)
        for key in keys:
            if key not in receipts:
                session.add(AdjustRegisterSheetReceipt(destination=destination, event_key=key,
                    delivered_at=datetime.now(timezone.utc).replace(tzinfo=None)))
                delivered += 1
        await session.commit()
        cursor = keys[-1]
    return f"Register sheet synced: {appended} appended, {delivered} delivery receipts recorded."
