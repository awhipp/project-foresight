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

def fetch_all_data():
    '''
    Fetch all data from the database.
    '''
    try:
        # Execute the query to fetch all data
        tick_data = TimeScaleService().execute(query="""
            SELECT instrument, time, bid, ask
            FROM forex_data
        """)

        return pd.DataFrame(tick_data)
    except Exception as fetch_exception:
        logger.error(f"Error: {fetch_exception}")

def calculate_averages(df: pd.DataFrame, instrument: str, timescale: str):
    '''
    Calculate averages from the data.
    '''
    if not df.empty:
        # Filter the data by instrument and remove it
        df = df[df['instrument'] == instrument]
        df.drop(columns=['instrument'], inplace=True)

        # Resample the data to calculate determined averages
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        averages = df.resample(timescale).mean()

        # Add instrument column back in
        averages['instrument'] = instrument
        return averages

    return None

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
        params=('dummy', 'EUR_USD', '1T')
    )

    sqsClient: Client = get_client('sqs')

    while True:
        try:
            # Fetch all data from the database
            data = fetch_all_data()

            subscriptions = TimeScaleService().execute(
                query="""SELECT queue_url, instrument, timescale FROM subscription_feeds"""
            )

            # Calculate averages for each subscription
            for subscription in subscriptions:
                sub_average = calculate_averages(
                    df=data,
                    instrument=subscription['instrument'],
                    timescale=subscription['timescale']
                )

                if subscription['queue_url'] == 'dummy':
                    logger.info(f"Total number of minutes for {subscription['instrument']}: {len(sub_average)}")
                else:
                    logger.info(f'Publishing to {subscription["queue_url"]}')
                    sqsClient.send_message(
                        QueueUrl=subscription['queue_url'],
                        MessageBody=sub_average.to_json(orient='records')
                    )
        except Exception as sending_exception:
            logger.error(f"Error: {sending_exception}")


        # Run at the start of the next minutes
        til_next_minute = round(60 - time.time() % 60, 2)
        logger.info(f"Sleeping for {til_next_minute} seconds")
        time.sleep(til_next_minute)