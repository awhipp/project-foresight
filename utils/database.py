'''Provides a singleton class to interact with the TimescaleDB database.'''

import os

import psycopg2
import psycopg2.extras

import dotenv
dotenv.load_dotenv("../.env")

import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s', 
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class TimeScaleService:
    '''Singleton Service to interact with Timescale DB'''
    _instance = None

    def __new__(cls, *args, **kwargs):
        '''Create a singleton instance of the class.'''
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.connection = None
        return cls._instance

    def __init__(self):
        '''Connect to the database.'''
        # Connection parameters
        db_params = {
            'host': os.getenv('TIMESCALE_HOST'),  # Replace with your TimescaleDB host
            'port': os.getenv('TIMESCALE_PORT'),  # Replace with your TimescaleDB port
            'database': os.getenv('TIMESCALE_DB'),  # Replace with your database name
            'user': os.getenv('TIMESCALE_USER'),      # Replace with your database user
            'password': os.getenv('TIMESCALE_PASSWORD')  # Replace with your database password
        }

        if self.connection is None:
            try:
                self.connection = psycopg2.connect(**db_params, cursor_factory=psycopg2.extras.RealDictCursor)
                self.connection.autocommit = True
                logger.info("Connected to TimescaleDB")
            except Exception as connection_exception:
                raise Exception(f"Failed to connect to the database: {connection_exception}")
            
    def create_table(self, query, hyper_table_name=None, hyper_table_column=None):
        '''Create a table in the database.'''
        if self.connection is not None:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query)
                    if hyper_table_name is not None and hyper_table_column is not None:
                        try:
                            cursor.execute(f"SELECT create_hypertable('{hyper_table_name}', '{hyper_table_column}')")
                        except psycopg2.DatabaseError:
                            logger.info("Already created the hyper table. Skipping.")
            except Exception as table_create_exception:
                raise Exception(f"Failed to create table: {table_create_exception}")
        else:
            raise Exception("Database connection not established.")
        

    def execute(self, query, params=None):
        '''Execute a query on the database.'''
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
        '''Close the database connection.'''
        if self.connection is not None:
            self.connection.close()
            self.connection = None

# Example usage:

if __name__ == "__main__":
    # Execute a sample query against native tables.
    result = TimesScaleService().execute(
        "SELECT * FROM pg_catalog.pg_tables"
    )

    # Close the database connection when done.
    TimesScaleService().close()

