"""Streaming service for getting FOREX data to a data store."""

import json
import os
import traceback
from datetime import datetime
from random import random
from time import sleep
from typing import Optional
from typing import Union

import dotenv
import requests

from foresight.models.forex_data import ForexData
from foresight.models.stream import Stream
from foresight.utils.database import TimeScaleService
from foresight.utils.logger import generate_logger


logger = generate_logger(__name__)
dotenv.load_dotenv(".env")


def open_random_walk_stream(
    sleep_between: Union[int, float] = 5,
    instrument: str = "EUR_USD",
    max_walk: int = -1,
    table_name: str = "forex_data",
):
    """
    Open a random walk stream and send the data to the data store.

    Args:
        sleep_between (Union[int, float]): The time to sleep between each record.
            Set to 0 if max_walk is defined.
        max_walk (int): The maximum number of walks to complete.
        table_name (str): The name of the table to send the data to.
    """

    if max_walk > 0:
        sleep_between = 0

    logger.info(
        f"""Starting random walk stream with sleep between:
{sleep_between} and max_walk: {max_walk}.""",
    )

    initial_price = 1.0
    walks_completed = 0

    while True:
        initial_price = initial_price * (1.0 + (random() - 0.5) * 0.1)

        record = ForexData(
            instrument=instrument,
            time=datetime.now().isoformat(),
            bid=round(initial_price, 5),
            ask=round(initial_price + 0.0001, 5),
        )

        TimeScaleService().insert_forex_data(record, table_name=table_name)

        logger.info(record)

        if max_walk > 0:
            walks_completed += 1
            if walks_completed >= max_walk:
                break

        sleep(sleep_between)


def process_stream_data(line: str, table_name: str = "forex_data"):
    """
    Process the stream data and send it to the data store.
    """
    if line:
        decoded_line = line.decode("utf-8")
        record: Stream = Stream.model_validate(json.loads(decoded_line))

        if record.errorMessage not in [None, ""]:
            logger.error(record.errorMessage)
        elif record.type == "PRICE" and record.tradeable:

            TimeScaleService().insert_forex_data(
                record.to_forex_data(),
                table_name=table_name,
            )
            logger.info(record)


def open_oanda_stream(run_forever: bool = True, limit: Optional[int] = None):
    """
    Open a stream to the OANDA API and send the data to the data store.

    Args:
        run_forever (bool): Whether to run the stream forever.
        limit (Optional[int]): The number of records to limit the stream to.
    """
    if not run_forever and limit is None:
        raise ValueError("If not running forever, limit must be greater than 0.")

    if run_forever and limit is not None:
        raise ValueError("If running forever, limit must be None.")

    account_id = os.getenv("OANDA_ACCOUNT_ID")
    api_token = os.getenv("OANDA_TOKEN")
    OANDA_API = os.getenv("OANDA_API", "https://stream-fxpractice.oanda.com/v3/")
    if not OANDA_API.endswith("/"):
        OANDA_API += "/"

    url = f"{OANDA_API}accounts/{account_id}/pricing/stream?instruments=EUR_USD"
    head = {
        "Content-type": "application/json",
        "Accept-Datetime-Format": "RFC3339",
        "Authorization": f"Bearer {api_token}",
    }
    resp = requests.get(url, headers=head, stream=True, timeout=30).iter_lines()
    for resp_idx, line in enumerate(resp):
        process_stream_data(line)

        if limit is not None and resp_idx >= limit:
            break


def open_stream():
    """Stream the data send the data to the data store.

    Uses a random walk or the OANDA API endpoint based on env."""

    random_walk = os.getenv("APP_RANDOM_WALK", "False").lower() == "true"

    execute_stream = open_random_walk_stream if random_walk else open_oanda_stream
    execute_stream()


def create_table(table_name: str = "forex_data") -> str:
    """Create a table in the data store."""

    # Execute SQL queries here
    TimeScaleService().create_table(
        query=f"""CREATE TABLE IF NOT EXISTS {table_name} (
        instrument VARCHAR(10) NOT NULL,
        time TIMESTAMPTZ NOT NULL,
        bid FLOAT NOT NULL,
        ask FLOAT NOT NULL,
        PRIMARY KEY (instrument, time)
    )""",
        table_name=table_name,
        column_name="time",
    )

    return table_name


if __name__ == "__main__":
    # Execute SQL queries here
    create_table()

    # Open a stream to the OANDA API and send the data to the data store.
    while True:
        try:
            open_stream()
        except Exception:  # pylint: disable=broad-except
            logger.error(traceback.format_exc())
            logger.error("Restarting stream...")
            continue
