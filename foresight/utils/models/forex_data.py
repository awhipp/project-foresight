"""Forex Data Model used in TimeScaleDB"""

from datetime import datetime

import pandas as pd
from pydantic import BaseModel

from foresight.utils.database import TimeScaleService
from foresight.utils.logger import generate_logger


logger = generate_logger(name=__name__)

timeMap: dict = {"S": "second", "M": "minute", "H": "hour", "D": "day"}


class ForexData(BaseModel):
    """TimescaleDB model for forex data.

    Args:
        instrument (str): The currency pair.
        time (datetime): The time of the record.
        bid (float): The bid price.
        ask (float): The ask price.
    """

    instrument: str
    time: datetime
    bid: float
    ask: float

    @staticmethod
    def create_table(table_name: str = "forex_data") -> str:
        """Create a table in the data store if it does not exist.

        Args:
            table_name (str): The name of the table to create.

        Returns:
            str: The name of the table created.
        """

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

    def insert_forex_data(self, table_name: str = "forex_data"):
        """Insert forex data into the database."""
        TimeScaleService().execute(
            query=f"""INSERT INTO {table_name} (instrument, time, bid, ask)
            VALUES (%s, %s, %s, %s)""",
            params=(
                self.instrument,
                self.time,
                self.bid,
                self.ask,
            ),
        )

    @staticmethod
    def drop_table(table_name: str = "forex_data"):
        """Drop a table in the data store.

        Args:
            table_name (str): The name of the table to drop.
        """

        # Execute SQL queries here
        TimeScaleService().execute(query=f"DROP TABLE {table_name}")

    @staticmethod
    def fetch(instrument: str = "EUR_USD", timescale: str = "M") -> list["ForexData"]:
        """
        Fetch all data from the database and return a DataFrame.

        Parameters:
            instrument (str): The instrument to fetch
            timescale (str): The timescale to fetch (S = Second, M = Minute)

        Returns:
            dict: The data from the database
        """
        try:
            df = pd.DataFrame(
                TimeScaleService().execute(
                    query=f"""
                        SELECT
                        TO_CHAR(
                            date_trunc(
                                '{timeMap[timescale.lower()]}', time
                            ),
                            'YYYY-MM-DD HH24:MI:SS'
                        ) as time,
                        AVG(ask) as ask, AVG(bid) as bid
                        FROM forex_data
                        WHERE instrument = '{instrument}'
                        AND time >= NOW() - INTERVAL '60 minute'
                        GROUP BY time
                        ORDER BY time ASC
                    """,
                ),
            )

            df = (
                df.groupby(df["time"]).agg({"ask": "mean", "bid": "mean"}).reset_index()
            )
            return [
                ForexData(
                    instrument=instrument,
                    time=datetime.strptime(row["time"], "%Y-%m-%d %H:%M:%S"),
                    bid=row["bid"],
                    ask=row["ask"],
                )
                for _, row in df.iterrows()
            ]
        except Exception as fetch_exception:  # pylint: disable=broad-except
            logger.error("Error fetching data: %s", fetch_exception)

    def to_price_json(self, order_type: str = "ask") -> dict:
        """Convert the data to JSON format.

        Args:
            order_type (str): The type of order to convert to JSON.

        Returns:
            dict: The data in JSON format.
        """
        json: dict = {
            "instrument": self.instrument,
            "time": self.time,
        }

        if order_type == "ask":
            json["price"] = self.ask
        elif order_type == "bid":
            json["price"] = self.bid
        elif order_type == "mid":
            json["price"] = round((self.bid + self.ask) / 2.0, 5)
        else:
            raise ValueError("Invalid order type")

        return json
