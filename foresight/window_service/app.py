"""Aggregates the data from the database and calculates one-minute averages."""

import time

import pandas as pd
from boto3_type_annotations.sqs import Client

from foresight.utils.aws import get_client
from foresight.utils.database import TimeScaleService
from foresight.utils.logger import generate_logger
from foresight.utils.models.forex_data import ForexData


logger = generate_logger(name=__name__)


if __name__ == "__main__":
    # ! TODO create subscription_feeds class
    TimeScaleService().create_table(
        query="""CREATE TABLE IF NOT EXISTS subscription_feeds (
        queue_url VARCHAR(255) NOT NULL,
        instrument VARCHAR(10) NOT NULL,
        timescale VARCHAR(10) NOT NULL,
        order_type VARCHAR(10) NOT NULL,
        PRIMARY KEY (queue_url, instrument, timescale)
    )""",
    )

    sqsClient: Client = get_client("sqs")

    while True:
        try:
            subscriptions = TimeScaleService().execute(
                query="SELECT queue_url, instrument, timescale, order_type FROM subscription_feeds",
            )

            # Calculate averages for each subscription
            for subscription in subscriptions:
                data: pd.DataFrame = ForexData.fetch(
                    instrument=subscription["instrument"],
                    timescale=subscription["timescale"],
                )

                order_type = subscription["order_type"]
                if order_type == "ask":
                    data["price"] = data["ask"]
                    data.drop(["ask", "bid"], axis=1, inplace=True)
                elif order_type == "bid":
                    data["price"] = data["bid"]
                    data.drop(["ask", "bid"], axis=1, inplace=True)
                elif order_type == "mid":
                    data["price"] = (data["bid"] + data["ask"]) / 2.0
                    data.drop(["ask", "bid"], axis=1, inplace=True)
                # Else Both

                # Only send if it has data
                if len(data) > 0:
                    logger.info(f'Publishing to {subscription["queue_url"]}')
                    sqsClient.send_message(
                        QueueUrl=subscription["queue_url"],
                        MessageBody=data.to_json(orient="records"),
                    )
        except Exception as sending_exception:
            logger.error(f"Error: {sending_exception}")

        # Run at the start of the next minutes
        til_next_minute = round(60 - time.time() % 60, 2)
        logger.info(f"Sleeping for {til_next_minute} seconds")
        time.sleep(til_next_minute)
