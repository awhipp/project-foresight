"""Pydantic model that stores the Indicator results."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from foresight.utils.database import TimeScaleService


class IndicatorResult(BaseModel):
    """Model representing a result for a given component and its value.

    Args:
    """

    component_name: str
    time: Optional[datetime] = datetime.now()
    value: str

    @staticmethod
    def create_table(table_name: str = "indicator_results") -> str:
        """Create a table in the data store if it does not exist.

        Args:
            table_name (str): The name of the table to create.

        Returns:
            str: The name of the table created.
        """
        TimeScaleService().create_table(
            query=f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                component_name VARCHAR(255) NOT NULL,
                time TIMESTAMPTZ NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (component_name, time)
            )""",
            table_name=table_name,
            column_name="time",
        )

        return table_name

    def insert(self, table_name: str = "indicator_results"):
        """Insert indicator results into the database."""
        TimeScaleService().execute(
            query=f"""INSERT INTO {table_name} (component_name, time, value)
            VALUES (%s, %s, %s)""",
            params=(self.component_name, self.time, self.value),
        )
