from datetime import date
from typing import Literal

from pydantic import BaseModel

ChartType = Literal["table", "pie", "both"]
"""Allowed chart selector for campaign overview endpoints.

Values:
    - ``table``: return table-only payload.
    - ``pie``: return pie-chart-only payload.
    - ``both``: return both table and pie payloads.
"""


class LeadsBySourceRequest(BaseModel):
    """Request schema for leads-by-source analytics APIs.

    Attributes:
        start_date (date): Start of reporting period (inclusive).
        end_date (date): End of reporting period (inclusive).
        chart (ChartType): Chart mode selector (`table`, `pie`, `both`).
    """

    start_date: date
    end_date: date
    chart: ChartType = "both"


class CampaignOverviewRequest(BaseModel):
    """Request schema for campaign overview API.

    Attributes:
        start_date (date): Start of reporting period (inclusive).
        end_date (date): End of reporting period (inclusive).
        chart (ChartType): Chart mode selector (`table`, `pie`, `both`).
    """

    start_date: date
    end_date: date
    chart: ChartType = "both"
