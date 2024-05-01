"""Test configuration file for fixtures."""

from datetime import datetime

import pytest

from foresight.utils.models.forex_data import ForexData
from foresight.utils.models.subscription_feed import SubscriptionFeed


@pytest.fixture()
def setup_forex_data_table():
    """Setup forex data for testing."""
    table_name = ForexData.create_table()
    yield table_name
    ForexData.drop_table(table_name=table_name)


@pytest.fixture()
def add_sample_forex_data(setup_forex_data_table):
    """Add some sample forex data."""
    ForexData(
        instrument="EUR_USD",
        time=datetime.now(),
        bid=1.2,
        ask=1.3,
    ).insert(table_name=setup_forex_data_table)


@pytest.fixture()
def setup_subscription_feed_table():
    """Setup subscription feed for testing."""
    table_name = SubscriptionFeed.create_table()
    yield table_name
    SubscriptionFeed.drop_table(table_name=table_name)
