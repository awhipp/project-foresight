"""Test configuration file for fixtures."""

import random
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest
from boto3_type_annotations.sqs import Client

from foresight.utils.aws import get_client
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

    # In one line create an array of random forex data
    # Random values are generated for the bid and ask prices
    # TZ Info UTC is used for the time
    dt = datetime.now().replace(tzinfo=timezone.utc, microsecond=0, second=0)
    forex_data = [
        ForexData(
            instrument="EUR_USD",
            time=dt - timedelta(minutes=i),
            bid=round(random.uniform(0.1, 1.0), 5),
            ask=round(random.uniform(0.1, 1.0), 5),
        )
        for i in range(5)
    ]

    # Reorganize in ascending order of time (same order that will be coming from tables)
    forex_data = sorted(forex_data, key=lambda x: x.time)

    ForexData.insert_multiple(table_name=setup_forex_data_table, data=forex_data)

    yield forex_data


@pytest.fixture()
def setup_subscription_feed_table():
    """Setup subscription feed for testing."""
    table_name = SubscriptionFeed.create_table()
    yield table_name
    SubscriptionFeed.drop_table(table_name=table_name)


# Pytest fixture that deletes all queues after all tests are run
@pytest.fixture(autouse=True)
def delete_all_queues():
    """Delete all queues after all tests are run."""
    yield
    sqs_client: Client = get_client("sqs")
    queues = sqs_client.list_queues()
    if "QueueUrls" in queues:
        for queue in queues["QueueUrls"]:
            sqs_client.delete_queue(QueueUrl=queue)
