"""Extraction utilities for external API pipelines."""

from __future__ import annotations

import asyncio

import requests
from decouple import config
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


class ExternalApiExtractor:
    """Extract raw data from JSON URL and Google Sheets."""

    def __init__(self) -> None:
        creds = Credentials(
            None,
            refresh_token=config("GSHEET_REFRESH_TOKEN", cast=str),
            token_uri=config("GSHEET_TOKEN_URI", cast=str),
            client_id=config("GSHEET_CLIENT_ID", cast=str),
            client_secret=config("GSHEET_CLIENT_SECRET", cast=str),
        )
        self.service = build("sheets", version="v4", credentials=creds, cache_discovery=False)
        self.sheet_id = config("GSHEET_SHEET_ID", cast=str)
        self.depo_source_url = config(
            "DEPO_SOURCE_URL",
            default=(
                "https://script.googleusercontent.com/macros/echo?"
                "user_content_key=AY5xjrTjdihXxTA5m1lfYwe5_8C9SGIK6Z95X4LtG9s5KT5tF_5-6iY1zwHLI16hdXFCudT2CueRQ2OccG7qM_a5wHVEAGAMXpIClsE1jpruGO8l0GwHFHdDcAFUyRws2G4E_ChFM62UL_bGfkUsWK0wyIBVZMb7eIu5oNR20HhxEYLLwlPp8WD4gpXF3mYnC9LchGTvoeKfR_KiBhq78s8_dgKUvw4S_Rr0h0bqgXz7EsbwZ-JYsmPGNPt_HxLTKGLRIAEf9lzrlEcThO6L8b2HSBzhB7yG7g"
                "&lib=M2A7k1cML9_qiTb3aF9ZIZKiUcBrFPiXa"
            ),
            cast=str,
        )

    async def fetch_json_url(self, url: str) -> list:
        """Fetch JSON payload from URL without blocking event loop."""

        def _request():
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.json()

        return await asyncio.to_thread(_request)

    async def fetch_sheet_values(self, range_name: str) -> list:
        """Fetch raw row values from Google Sheets for one range."""

        def _request():
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_id,
                range=range_name,
            ).execute()
            return result.get("values", [])

        return await asyncio.to_thread(_request)

