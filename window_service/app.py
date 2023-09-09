'''Aggregates the data from the database and calculates one-minute averages.'''
import sys
from pathlib import Path
import time
import pandas as pd

from boto3_type_annotations.sqs import Client

# Setup logging and log timestamp prepend
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s', 
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Determine fixed path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.database import TimeScaleService
from utils.aws import get_client

def fetch_data(is_bid: bool = True, instrument: str = 'EUR_USD', timescale: str = 'M'):
    '''
    Fetch all data from the database and return a DataFrame.

    Parameters:
        is_bid (bool): Whether to fetch bid or ask data
        instrument (str): The instrument to fetch
        timescale (str): The timescale to fetch (M = minutes, H = hours, D = days)
    
    Returns:
        pd.DataFrame: The data from the database
    '''
    try:
        # Fetch all data from the database based on the parameters
        ask_or_bid = 'bid' if is_bid else 'ask'
        truncation = 'minute' if timescale == 'M' else 'hour' if timescale == 'H' else 'day'
        tick_data = TimeScaleService().execute(
            query=f"""
                SELECT TO_CHAR(date_trunc('{truncation}', time), 'YYYY-MM-DD HH24:MI:SS') as time,
                {ask_or_bid} AS price
                FROM forex_data
                WHERE instrument = '{instrument}'
                GROUP BY time, {ask_or_bid}
                ORDER BY time ASC
            """
        )

        df = pd.DataFrame(tick_data)

        return df.groupby('time')['price'].mean().reset_index() # Calculate the average price for each minute
    except Exception as fetch_exception:
        logger.error(f"Error: {fetch_exception}")

if __name__ == "__main__":
    # Execute SQL queries here
    TimeScaleService().create_table(
        query="""CREATE TABLE IF NOT EXISTS subscription_feeds (
        queue_url VARCHAR(255) NOT NULL,
        instrument VARCHAR(10) NOT NULL,
        timescale VARCHAR(10) NOT NULL,
        PRIMARY KEY (queue_url, instrument, timescale)              
    )""")

    # Insert Dummy Queue for Logging if exists replace
    TimeScaleService().execute("""DELETE FROM subscription_feeds WHERE queue_url = 'dummy'""")
    TimeScaleService().execute(
        query="""INSERT INTO subscription_feeds (queue_url, instrument, timescale)
        VALUES (%s, %s, %s)""",
        params=('dummy', 'EUR_USD', 'M')
    )

    sqsClient: Client = get_client('sqs')

    while True:
        try:
            subscriptions = TimeScaleService().execute(
                query="""SELECT queue_url, instrument, timescale FROM subscription_feeds"""
            )

            # Calculate averages for each subscription
            for subscription in subscriptions:
                data = fetch_data(
                    is_bid=True,
                    instrument=subscription['instrument'],
                    timescale=subscription['timescale']
                )

                if subscription['queue_url'] == 'dummy':
                    logger.info(f"Total number of minutes (grouped) for {subscription['instrument']}: {len(data)}")
                else:
                    logger.info(f'Publishing to {subscription["queue_url"]}')
                    sqsClient.send_message(
                        QueueUrl=subscription['queue_url'],
                        MessageBody=data.to_json(orient='records')
                    )
        except Exception as sending_exception:
            logger.error(f"Error: {sending_exception}")


        # Run at the start of the next minutes
        til_next_minute = round(60 - time.time() % 60, 2)
        logger.info(f"Sleeping for {til_next_minute} seconds")
        time.sleep(til_next_minute)