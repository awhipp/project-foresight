"""
This module contains the Pydantic models for the Steam API.
"""

from typing import List
from typing import Optional

from pydantic import BaseModel

from foresight.models.pricing import Pricing


class Stream(BaseModel):
    """A Pydantic model for the Steam API."""

    type: str
    instrument: Optional[str] = None
    time: str
    tradeable: Optional[bool] = None
    bids: Optional[list[Pricing]] = []
    asks: Optional[list[Pricing]] = []
    closeoutBid: Optional[str] = None
    closeoutAsk: Optional[str] = None
    status: Optional[str] = None
    tradeable: Optional[bool] = False
    errorMessage: Optional[str] = None
