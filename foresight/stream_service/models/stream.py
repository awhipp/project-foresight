"""
This module contains the Pydantic models for the Steam API.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from foresight.stream_service.models.pricing import Pricing
from foresight.utils.models.forex_data import ForexData


class Stream(BaseModel):
    """A Pydantic model for the Steam API."""

    type: str = "PRICE"
    instrument: Optional[str] = None
    time: datetime
    tradeable: Optional[bool] = True
    bids: Optional[list[Pricing]] = []
    asks: Optional[list[Pricing]] = []
    closeoutBid: Optional[str] = None
    closeoutAsk: Optional[str] = None
    status: Optional[str] = None
    errorMessage: Optional[str] = None

    def to_forex_data(self):
        """Convert the stream data to forex data."""
        return ForexData(
            instrument=self.instrument,
            time=self.time,
            bid=self.bids[0].price,
            ask=self.asks[0].price,
        )
