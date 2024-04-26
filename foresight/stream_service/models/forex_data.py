"""Forex Data Model used in TimeScaleDB"""

from datetime import datetime

from pydantic import BaseModel

from foresight.utils.database import TimeScaleService


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
