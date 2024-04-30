"""Test configuration file for fixtures."""

import pytest

from foresight.utils.models.forex_data import ForexData


@pytest.fixture()
def setup_forex_data():
    """Setup forex data for testing."""
    ForexData.create_table()
    yield
    ForexData.drop_table()
