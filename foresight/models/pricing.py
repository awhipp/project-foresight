"""Pydantic models for the pricing API."""

from pydantic import BaseModel


class Pricing(BaseModel):
    """A Pydantic model for the pricing API."""

    price: str
    liquidity: int
