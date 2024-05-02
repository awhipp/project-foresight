"""Aggregates the data from the database and calculates one-minute averages."""

import time

from boto3_type_annotations.sqs import Client

from foresight.utils.aws import get_client
from foresight.utils.logger import generate_logger
from foresight.utils.models.forex_data import ForexData
from foresight.utils.models.subscription_feed import SubscriptionFeed


logger = generate_logger(name=__name__)

sqs_client: Client = get_client("sqs")


def setup():
    """Setup for the window service."""
    SubscriptionFeed.create_table()


def wait_until_next_minute():
    """Wait until the next minute."""
    til_next_minute = round(60 - time.time() % 60, 2)
    logger.info("Sleeping for %s seconds", til_next_minute)
    time.sleep(til_next_minute)


def send_data_to_queues() -> int:
    """Based on subscriptions, gets relevant data and sends to the queues.

    Returns:
        int: The number of messages sent.
    """

    try:
        subscriptions: list[SubscriptionFeed] = SubscriptionFeed.fetch()
        messages_sent: int = 0

        # Calculate averages for each subscription
        for subscription in subscriptions:
            all_forex_data: list[ForexData] = ForexData.fetch(
                instrument=subscription.instrument,
                timescale=subscription.timescale,
            )

            order_type = subscription.order_type

            forex_data = [
                data_point.convert_to_price(order_type=order_type).model_dump_json()
                for data_point in all_forex_data
            ]

            # Only send if it has data
            if len(forex_data) > 0:
                logger.info(
                    "Publishing %s to Queue: %s",
                    f"{len(forex_data)} messages",
                    subscription.queue_url,
                )
                for data_point in forex_data:
                    sqs_client.send_message(
                        QueueUrl=subscription.queue_url,
                        MessageBody=data_point,
                    )
                    messages_sent += 1
        return messages_sent

    except Exception as sending_exception:  # pylint: disable=broad-except
        logger.error("Error: %s", sending_exception)


if __name__ == "__main__":
    setup()

    while True:
        send_data_to_queues()

        wait_until_next_minute()
