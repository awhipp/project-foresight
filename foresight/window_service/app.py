"""Aggregates the data from the database and calculates one-minute averages."""

import time

from boto3_type_annotations.sqs import Client

from foresight.utils.aws import get_client
from foresight.utils.logger import generate_logger
from foresight.utils.models.forex_data import ForexData
from foresight.utils.models.subscription_feeds import SubscriptionFeeds


logger = generate_logger(name=__name__)


if __name__ == "__main__":
    SubscriptionFeeds.create_table()

    sqsClient: Client = get_client("sqs")

    while True:
        try:
            subscriptions: list[SubscriptionFeeds] = SubscriptionFeeds.fetch()

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
                    logger.info("Publishing to Queue: %s", subscription["queue_url"])
                    sqsClient.send_message(
                        QueueUrl=subscription["queue_url"],
                        MessageBody=forex_data,
                    )
        except Exception as sending_exception:  # pylint: disable=broad-except
            logger.error("Error: %s", sending_exception)

        # Run at the start of the next minutes
        til_next_minute = round(60 - time.time() % 60, 2)
        logger.info("Sleeping for %s seconds", til_next_minute)
        time.sleep(til_next_minute)
