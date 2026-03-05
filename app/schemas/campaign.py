from datetime import date
from typing import Literal

from pydantic import BaseModel

ChartType = Literal["table", "pie", "both"]


class LeadsBySourceRequest(BaseModel):
    """Request payload for leads-by-source analytics endpoints."""

    start_date: date
    end_date: date
    chart: ChartType = "both"


class CampaignOverviewRequest(BaseModel):
    """Request payload for campaign overview endpoint."""

    start_date: date
    end_date: date
    chart: ChartType = "both"
