"""Aggregates the data from the database and calculates one-minute averages."""

import json
import time

from boto3_type_annotations.sqs import Client

from foresight.utils.aws import get_client
from foresight.utils.logger import generate_logger
from foresight.utils.models.forex_data import ForexData
from foresight.utils.models.subscription_feed import SubscriptionFeed


logger = generate_logger(name=__name__)

sqsClient: Client = get_client("sqs")


def setup():
    """Setup for the window service."""
    SubscriptionFeed.create_table()


def wait_until_next_minute():
    """Wait until the next minute."""
    til_next_minute = round(60 - time.time() % 60, 2)
    logger.info("Sleeping for %s seconds", til_next_minute)
    time.sleep(til_next_minute)


def send_data_to_queues():
    """Based on subscriptions, gets relevant data and sends to the queues."""

    try:
        subscriptions: list[SubscriptionFeed] = SubscriptionFeed.fetch()

        # Calculate averages for each subscription
        for subscription in subscriptions:
            all_forex_data: list[ForexData] = ForexData.fetch(
                instrument=subscription.instrument,
                timescale=subscription.timescale,
            )

            order_type = subscription.order_type

            forex_data = [
                data_point.to_price_json(order_type=order_type)
                for data_point in all_forex_data
            ]

            # Only send if it has data
            if len(forex_data) > 0:
                logger.info("Publishing to Queue: %s", subscription.queue_url)
                for data_point in forex_data:
                    sqsClient.send_message(
                        QueueUrl=subscription.queue_url,
                        MessageBody=json.dumps(data_point),
                    )

    except Exception as sending_exception:  # pylint: disable=broad-except
        logger.error("Error: %s", sending_exception)


if __name__ == "__main__":
    setup()

    while True:
        send_data_to_queues()

        wait_until_next_minute()
