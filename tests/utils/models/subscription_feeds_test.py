"""Test the SubscriptionFeed model."""

import pytest

from foresight.utils.database import TimeScaleService
from foresight.utils.models.subscription_feed import SubscriptionFeed


def test_valid_subscription_feed():
    """Test valid subscription feed."""

    subscription_feed = SubscriptionFeed(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/queue-name",
        instrument="EUR_USD",
        timescale="S",
        order_type="bid",
    )

    assert (
        subscription_feed.queue_url
        == "https://sqs.us-east-1.amazonaws.com/123456789012/queue-name"
    )
    assert subscription_feed.instrument == "EUR_USD"
    assert subscription_feed.timescale == "S"
    assert subscription_feed.order_type == "bid"


def test_invalid_forex_data():
    """Test invalid subscription feed. All 4 fields are required."""

    valid_definition = {
        "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/queue-name",
        "instrument": "EUR_USD",
        "timescale": "S",
        "order_type": "bid",
    }

    # Loop through all fields and remove one at a time
    for key in valid_definition.keys():
        invalid_definition = valid_definition.copy()
        invalid_definition.pop(key)
        with pytest.raises(ValueError):
            SubscriptionFeed(**invalid_definition)


def test_create_table():
    """Test the create_table method."""

    # ARRANGE / ACT
    table_name = SubscriptionFeed.create_table()

    # ASSERT
    assert table_name == table_name

    hyper_table_details = TimeScaleService().execute(
        query=f"""SELECT * FROM timescaledb_information.hypertables
        WHERE hypertable_name = '{table_name}'""",
    )

    assert len(hyper_table_details) == 0  # Not a hypertable

    table_schema = TimeScaleService().execute(
        query=f"""SELECT
            column_name, data_type, character_maximum_length, column_default, is_nullable
        FROM INFORMATION_SCHEMA.COLUMNS where table_name = '{table_name}'""",
    )

    expected_columns = {
        "queue_url": "character varying",
        "instrument": "character varying",
        "timescale": "character varying",
        "order_type": "character varying",
    }
    assert len(table_schema) == 4  # 4 columns
    for column in table_schema:
        assert column["data_type"] == expected_columns[column["column_name"]]


@pytest.mark.usefixtures("setup_subscription_feed_table")
def test_insert_and_fetch():
    """Insert and fetch subscription feed."""

    # ARRANGE
    feed = SubscriptionFeed(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/queue-name",
        instrument="EUR_USD",
        timescale="S",
        order_type="bid",
    )

    # ACT
    feed.insert()

    data = SubscriptionFeed.fetch()

    # ASSERT
    assert len(data) == 1
    assert data[0].queue_url == feed.queue_url
    assert data[0].instrument == feed.instrument
    assert data[0].timescale == feed.timescale
    assert data[0].order_type == feed.order_type
