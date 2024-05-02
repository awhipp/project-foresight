"""Forex Data Model used in TimeScaleDB"""

from pydantic import BaseModel

from foresight.utils.database import TimeScaleService
from foresight.utils.logger import generate_logger


logger = generate_logger(name=__name__)


class SubscriptionFeed(BaseModel):
    """TimescaleDB model for subscription feeds.

    Args:
        instrument (str): The currency pair.
        time (datetime): The time of the record.
        bid (float): The bid price.
        ask (float): The ask price.
    """

    queue_url: str
    instrument: str
    timescale: str
    order_type: str

    @staticmethod
    def create_table(table_name: str = "subscription_feed") -> str:
        """Create a table in the data store if it does not exist.

        Args:
            table_name (str): The name of the table to create.

        Returns:
            str: The name of the table created.
        """

        TimeScaleService().create_table(
            query=f"""CREATE TABLE IF NOT EXISTS {table_name} (
                queue_url VARCHAR(255) NOT NULL,
                instrument VARCHAR(10) NOT NULL,
                timescale VARCHAR(10) NOT NULL,
                order_type VARCHAR(10) NOT NULL,
                PRIMARY KEY (queue_url)
            )""",
        )
        return table_name

    @staticmethod
    def drop_table(table_name: str = "subscription_feed"):
        """Drop a table in the data store.

        Args:
            table_name (str): The name of the table to drop.
        """

        # Execute SQL queries here
        TimeScaleService().execute(query=f"DROP TABLE {table_name}")

    def insert_or_update(self, table_name: str = "subscription_feed"):
        """
        Insert or replaces a feed record in the database.

        Args:
            table_name (str): The name of the table to insert data into.

        """
        TimeScaleService().execute(
            query=f"""
                INSERT INTO {table_name} (queue_url, instrument, timescale, order_type)
                VALUES (
                    '{self.queue_url}', '{self.instrument}', '{self.timescale}', '{self.order_type}'
                )
                ON CONFLICT (queue_url) DO UPDATE
                SET instrument = EXCLUDED.instrument,
                    timescale = EXCLUDED.timescale,
                    order_type = EXCLUDED.order_type
            """,
        )

    @staticmethod
    def fetch(table_name="subscription_feed") -> list["SubscriptionFeed"]:
        """
        Fetch all data from the table and return a DataFrame.

        Args:
            table_name (str): The name of the table to fetch data from.

        Returns:
            dict: The data from the database
        """
        try:
            active_feeds: list[dict] = TimeScaleService().execute(
                query=f"SELECT queue_url, instrument, timescale, order_type FROM {table_name}",
            )

            return [SubscriptionFeed(**feed) for feed in active_feeds]
        except Exception as fetch_exception:  # pylint: disable=broad-except
            logger.error("Error fetching data: %s", fetch_exception)
