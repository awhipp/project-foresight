"""Pydantic models for the pricing API."""

from pydantic import BaseModel


class Pricing(BaseModel):
    """A Pydantic model for the pricing API."""

    price: float
    liquidity: int = 1
