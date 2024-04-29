"""Provides a singleton class to interact with the TimescaleDB database."""

import os

import dotenv
import psycopg2
import psycopg2.extras

from foresight.utils.logger import generate_logger


dotenv.load_dotenv(".env")


logger = generate_logger(name=__name__)


class TimeScaleService:
    """Singleton Service to interact with Timescale DB"""

    _instance = None

    def __new__(cls, *args, **kwargs):
        """Create a singleton instance of the class."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.connection = None
        return cls._instance

    def __init__(self):
        """Connect to the database."""
        # Connection parameters
        db_params = {
            "host": os.getenv("TIMESCALE_HOST"),  # Replace with your TimescaleDB host
            "port": os.getenv("TIMESCALE_PORT"),  # Replace with your TimescaleDB port
            "database": os.getenv("TIMESCALE_DB"),  # Replace with your database name
            "user": os.getenv("TIMESCALE_USER"),  # Replace with your database user
            "password": os.getenv(
                "TIMESCALE_PASSWORD",
            ),  # Replace with your database password
        }

        if self.connection is None:
            try:
                self.connection = psycopg2.connect(
                    **db_params,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                )
                self.connection.autocommit = True
                logger.info("Connected to TimescaleDB")
            except Exception as connection_exception:
                raise Exception(
                    f"Failed to connect to the database: {connection_exception}",
                )

    def create_table(self, query, table_name=None, column_name=None):
        """Create a table in the database."""
        if self.connection is not None:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query)
                    if table_name is not None and column_name is not None:
                        try:
                            cursor.execute(
                                f"SELECT create_hypertable('{table_name}', '{column_name}')",
                            )
                        except psycopg2.DatabaseError:
                            logger.info("Already created the hyper table. Skipping.")
            except Exception as table_create_exception:
                raise ValueError(
                    f"Failed to create table: {table_create_exception}",
                ) from table_create_exception

        else:
            raise Exception("Database connection not established.")

    def execute(self, query, params=None):
        """Execute a query on the database."""
        if self.connection is not None:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query, params)
                    if cursor.description is not None:
                        data = cursor.fetchall()
                        return [dict(row) for row in data]
            except Exception as query_execute_exception:
                raise Exception(f"Failed to execute query: {query_execute_exception}")
        else:
            raise Exception("Database connection not established.")

    def close(self):
        """Close the database connection."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None


# Example usage:
# # Execute a sample query against native tables.
# result = TimesScaleService().execute("SELECT * FROM pg_catalog.pg_tables")
# # Close the database connection when done.
# TimesScaleService().close()
