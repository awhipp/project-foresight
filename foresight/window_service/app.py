"""Aggregates the data from the database and calculates one-minute averages."""

import logging
import time

import pandas as pd
from boto3_type_annotations.sqs import Client

from foresight.utils.aws import get_client
from foresight.utils.database import TimeScaleService


logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def fetch_data(instrument: str = "EUR_USD", timescale: str = "M") -> pd.DataFrame:
    """
    Fetch all data from the database and return a DataFrame.

    Parameters:
        instrument (str): The instrument to fetch
        timescale (str): The timescale to fetch (T = tick, M = minutes)

    Returns:
        dict: The data from the database
    """
    try:
        # Fetch all data from the database based on the parameters
        if timescale == "M":
            df = pd.DataFrame(
                TimeScaleService().execute(
                    query=f"""
                        SELECT TO_CHAR(date_trunc('minute', time), 'YYYY-MM-DD HH24:MI:SS') as time,
                        AVG(ask) as ask, AVG(bid) as bid
                        FROM forex_data
                        WHERE instrument = '{instrument}'
                        AND time >= NOW() - INTERVAL '60 minute'
                        GROUP BY time
                        ORDER BY time ASC
                    """,
                ),
            )

            return (
                df.groupby(df["time"]).agg({"ask": "mean", "bid": "mean"}).reset_index()
            )
        elif timescale == "T":
            return pd.DataFrame(
                TimeScaleService().execute(
                    query=f"""
                        SELECT TO_CHAR(time, 'YYYY-MM-DD HH24:MI:SS') as time,
                        ask, bid
                        FROM forex_data
                        WHERE instrument = '{instrument}'
                        AND time >= NOW() - INTERVAL '60 minute'
                        ORDER BY time ASC
                    """,
                ),
            )
        else:
            raise Exception(f"Invalid timescale: {timescale}")
    except Exception as fetch_exception:
        logger.error(f"Error: {fetch_exception}")


if __name__ == "__main__":
    # Execute SQL queries here
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
                query="""SELECT queue_url, instrument, timescale, order_type FROM subscription_feeds""",
            )

            # Calculate averages for each subscription
            for subscription in subscriptions:
                data = fetch_data(
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
        # til_next_minute = round(60 - time.time() % 60, 2)
        # logger.info(f"Sleeping for {til_next_minute} seconds")
        # time.sleep(til_next_minute)
        time.sleep(5)
