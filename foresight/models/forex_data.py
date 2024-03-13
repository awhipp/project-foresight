"""Forex Data Model used in TimeScaleDB"""

from datetime import datetime

from pydantic import BaseModel


class ForexData(BaseModel):
    """TimescaleDB model for forex data."""

    instrument: str
    time: datetime
    bid: float
    ask: float
