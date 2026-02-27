from pydantic import BaseModel
from typing import Dict
from datetime import datetime


class UpdateData(BaseModel):
    """
    Schemas for update data
    """
    start_date: datetime
    end_date: datetime
    data: str
    types: str = "auto"
