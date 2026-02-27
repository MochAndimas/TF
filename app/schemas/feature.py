from pydantic import BaseModel
from typing import Dict


class UpdateData(BaseModel):
    """
    Schemas for update data
    """
    message: str
