"""Test for the window service."""

import uuid

import pytest
from boto3_type_annotations.sqs import Client

from foresight.utils.aws import get_client
from foresight.utils.models.forex_data import ForexData
from foresight.utils.models.subscription_feed import SubscriptionFeed
from foresight.window_service.app import send_data_to_queues


@pytest.fixture()
def setup_temporary_queue():
    """Setup a temporary queue with a random name."""
    sqs_client: Client = get_client("sqs")
    queue_name = f"test-queue-{uuid.uuid4()}"
    queue_url = sqs_client.create_queue(QueueName=queue_name)["QueueUrl"]
    yield queue_url
    sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.fixture()
def setup_subscription_feed(setup_subscription_feed_table, setup_temporary_queue):
    """Setup a subscription feed."""
    queue_url = setup_temporary_queue

    subscription_feed = SubscriptionFeed(
        queue_url=queue_url,
        instrument="EUR_USD",
        timescale="S",
        order_type="bid",
    )

    subscription_feed.insertOrUpdate(table_name=setup_subscription_feed_table)

    yield subscription_feed


def test_send_data_to_queues(setup_subscription_feed, add_sample_forex_data):
    """Test sending data to queues."""

    # ARRANGE
    feed: SubscriptionFeed = setup_subscription_feed

    # ACT
    messages_sent: int = send_data_to_queues()

    # ASSERT
    assert messages_sent == len(add_sample_forex_data)

    # Pull message from queue and check if it is the same as the data
    sqs_client: Client = get_client("sqs")
    messages = sqs_client.receive_message(
        QueueUrl=feed.queue_url,
        MaxNumberOfMessages=messages_sent + 1,  # Get one extra message (if any)
    )
    messages = messages["Messages"]
    assert len(messages) == messages_sent

    messages = ForexData.model_validate_sqs_messages(messages)

    expected_forex = [
        data.convert_to_price(order_type="bid") for data in add_sample_forex_data
    ]

    for message in messages:
        found_message = False
        for expected in expected_forex:
            if message == expected:
                found_message = True
                break
        assert found_message
