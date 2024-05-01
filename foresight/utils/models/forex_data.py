"""Forex Data Model used in TimeScaleDB"""

from datetime import datetime

from pydantic import BaseModel

from foresight.utils.database import TimeScaleService
from foresight.utils.logger import generate_logger


logger = generate_logger(name=__name__)

# time_map: dict = {"S": "second"}

# Generate the interval based on the timescale
interval_map: dict = {
    "S": "1 second",
}


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
        # ? Do we need a PK for INSTRUMENT and TIME?
        TimeScaleService().create_table(
            query=f"""CREATE TABLE IF NOT EXISTS {table_name} (
            instrument VARCHAR(10) NOT NULL,
            time TIMESTAMPTZ NOT NULL,
            bid FLOAT NOT NULL,
            ask FLOAT NOT NULL
        )""",
            table_name=table_name,
            column_name="time",
        )

        return table_name

    def insert(self, table_name: str = "forex_data"):
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
    def insert_multiple(data: list["ForexData"], table_name: str = "forex_data"):
        """Insert list of multiple forex data efficiently."""
        if len(data) > 0:
            TimeScaleService().execute(
                query=f"""INSERT INTO {table_name} (instrument, time, bid, ask) VALUES %s""",
                params=[
                    (
                        row.instrument,
                        row.time,
                        row.bid,
                        row.ask,
                    )
                    for row in data
                ],
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
    def fetch(instrument: str = "EUR_USD", timescale: str = "S") -> list["ForexData"]:
        """
        Fetch all data from the database and return a DataFrame.

        The goal is to great the moving average at the timescale granularity.

        Parameters:
            instrument (str): The instrument to fetch
            timescale (str): The timescale to fetch (S = Second)

        Returns:
            dict: The data from the database
        """
        try:
            query = f"""SELECT
                instrument,
                time_bucket('{interval_map[timescale]}', time) as time,
                AVG(bid) as bid,
                AVG(ask) as ask
            FROM forex_data
            WHERE instrument = '{instrument}'
            GROUP BY instrument, time
            ORDER BY time ASC"""
            results = TimeScaleService().execute(query=query)

            return [ForexData(**row) for row in results]

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
            "time": str(self.time),  # Needs to be string for JSON
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
