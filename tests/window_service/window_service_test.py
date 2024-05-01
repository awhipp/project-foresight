"""Test for the window service."""

import json
import uuid
from datetime import datetime

import pytest
from boto3_type_annotations.sqs import Client

from foresight.utils.aws import get_client
from foresight.utils.models.subscription_feed import SubscriptionFeed
from foresight.window_service.app import send_data_to_queues


@pytest.fixture()
def setup_temporary_queue():
    """Setup a temporary queue with a random name."""
    sqsClient: Client = get_client("sqs")
    queue_name = f"test-queue-{uuid.uuid4()}"
    queue_url = sqsClient.create_queue(QueueName=queue_name)["QueueUrl"]
    yield queue_url
    sqsClient.delete_queue(QueueUrl=queue_url)


@pytest.fixture()
def setup_subscription_feed(setup_temporary_queue, setup_subscription_feed_table):
    """Setup a subscription feed."""
    queue_url = setup_temporary_queue

    subscription_feed = SubscriptionFeed(
        queue_url=queue_url,
        instrument="EUR_USD",
        timescale="S",
        order_type="bid",
    )

    subscription_feed.insert()

    yield subscription_feed


@pytest.mark.usefixtures("add_sample_forex_data")
def test_send_data_to_queues(setup_subscription_feed):
    """Test sending data to queues."""

    # ARRANGE
    feed: SubscriptionFeed = setup_subscription_feed

    # ACT
    send_data_to_queues()

    # ASSERT
    # Pull message from queue and check if it is the same as the data

    sqsClient: Client = get_client("sqs")
    messages = sqsClient.receive_message(
        QueueUrl=feed.queue_url,
        MaxNumberOfMessages=1,
    )
    messages = messages["Messages"]
    assert len(messages) == 1

    message = json.loads(messages[0]["Body"])

    # ? Should there be a price object
    assert message["price"] == 1.2
    assert message["instrument"] == "EUR_USD"
    # Validate time is a valid datetime string
    assert datetime.fromisoformat(message["time"])
