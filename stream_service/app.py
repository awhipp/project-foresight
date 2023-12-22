"""Streaming service for getting FOREX data to a data store."""
import json
import os

import requests

import dotenv

dotenv.load_dotenv(".env")

# Setup logging and log timestamp prepend
from utils.logger import generate_logger

logger = generate_logger(__name__)
from utils.database import TimeScaleService


def open_stream():
    """Open a stream to the OANDA API and send the data to the data store."""
    account_id = os.getenv("OANDA_API_ACCOUNT_ID")
    api_token = os.getenv("OANDA_API_TOKEN")
    random_walk = os.getenv("APP_RANDOM_WALK", "False") == "True"

    if random_walk:
        from random import random
        from datetime import datetime
        from time import sleep

        initial_price = 1.0

        while True:
            initial_price = initial_price * (1.0 + (random() - 0.5) * 0.1)
            record = {
                "instrument": "EUR_USD",
                "time": datetime.now().isoformat(),
                "bid": round(initial_price, 5),
                "ask": round(initial_price + 0.0001, 5),
            }

            TimeScaleService().execute(
                query="""INSERT INTO forex_data (instrument, time, bid, ask)
                VALUES (%s, %s, %s, %s)""",
                params=(
                    record["instrument"],
                    record["time"],
                    record["bid"],
                    record["ask"],
                ),
            )

            logger.info(record)

            sleep(5)

    else:
        url = f"https://stream-fxtrade.oanda.com/v3/accounts/{account_id}/pricing/stream?instruments=EUR_USD"
        head = {
            "Content-type": "application/json",
            "Accept-Datetime-Format": "RFC3339",
            "Authorization": f"Bearer {api_token}",
        }
        resp = requests.get(url, headers=head, stream=True, timeout=30).iter_lines()
        for line in resp:
            if line:
                decoded_line = line.decode("utf-8")
                obj = json.loads(decoded_line)
                if obj["type"] == "PRICE" and obj["tradeable"]:
                    record = {
                        "instrument": obj["instrument"],
                        "time": obj["time"],
                        "bid": float(obj["bids"][0]["price"]),
                        "ask": float(obj["asks"][0]["price"]),
                    }

                    TimeScaleService().execute(
                        query="""INSERT INTO forex_data (instrument, time, bid, ask)
                        VALUES (%s, %s, %s, %s)""",
                        params=(
                            record["instrument"],
                            record["time"],
                            record["bid"],
                            record["ask"],
                        ),
                    )

                    logger.info(record)


def create_table():
    """Create a table in the data store."""

    # Execute SQL queries here
    TimeScaleService().create_table(
        query="""CREATE TABLE IF NOT EXISTS forex_data (
        instrument VARCHAR(10) NOT NULL,
        time TIMESTAMPTZ NOT NULL,
        bid FLOAT NOT NULL,
        ask FLOAT NOT NULL,
        PRIMARY KEY (instrument, time)                   
    )""",
        hyper_table_name="forex_data",
        hyper_table_column="time",
    )


if __name__ == "__main__":
    # Execute SQL queries here
    TimeScaleService().create_table(
        query="""CREATE TABLE IF NOT EXISTS forex_data (
        instrument VARCHAR(10) NOT NULL,
        time TIMESTAMPTZ NOT NULL,
        bid FLOAT NOT NULL,
        ask FLOAT NOT NULL,
        PRIMARY KEY (instrument, time)                   
    )""",
        hyper_table_name="forex_data",
        hyper_table_column="time",
    )

    # Open a stream to the OANDA API and send the data to the data store.
    while True:
        try:
            open_stream()
        except Exception as e:
            logger.error(e)
            logger.info("Restarting stream...")
