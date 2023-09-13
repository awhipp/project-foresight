'''Indicator Superclass'''
import sys
from pathlib import Path

import io
import time
import json
import datetime

# Determine fixed path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.database import TimeScaleService
from utils.aws import get_client

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

class Indicator():
    '''Indicator Superclass'''
    component_name: str
    queue_url: str
    
    def __init__(self, component_name: str, instrument: str, timescale: str):
        if type(self) == Indicator:
            raise Exception("<Indicator> must be subclassed.")
        self.component_name = component_name
        self.queue_url = self.create_queue()
        self.add_subscription_record(instrument=instrument, timescale=timescale)

    def create_queue(self) -> str:
        '''Create a queue.'''
        sqsClient: Client = get_client('sqs')
        queue_name = f"{self.component_name}_indicator_queue"
        response = sqsClient.create_queue(QueueName=queue_name)
        logger.info(f"Created queue: {self.component_name}_indicator_queue")
        return response['QueueUrl']

    def add_subscription_record(self, instrument: str, timescale: str):
        '''Add a subscription record.'''
        TimeScaleService().execute(f"DELETE FROM subscription_feeds WHERE queue_url = '{self.queue_url}'")
        TimeScaleService().execute(
            query=f"""
                INSERT INTO subscription_feeds (queue_url, instrument, timescale)
                VALUES ('{self.queue_url}', '{instrument}', '{timescale}')
            """
        )
        logger.info(f"Added subscription record for {self.component_name}")

    def pull_from_queue(self) -> pd.DataFrame:
        '''Pulls from Queue and returns a DataFrame.'''
        sqsClient: Client = get_client('sqs')
        logger.info(f"Counting messages in queue: {self.queue_url}")
        response = sqsClient.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        logger.info(f"Messages in queue: {response['Attributes']['ApproximateNumberOfMessages']}")

        logger.info(f"Pulling from queue: {self.queue_url}")
        response = sqsClient.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1
        )
        if 'Messages' in response:
            message =  response['Messages'][0]
            sqsClient.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            df = pd.read_json(io.StringIO(message['Body']))
            return df
        return None
        
    def value(self, data: pd.DataFrame) -> float:
        '''Calculate the value of the indicator.'''
        raise NotImplementedError("Subclasses must implement this method.")
    
    def do_work(self):
        '''Do work.'''
        raise NotImplementedError("Subclasses must implement this method.")
    
    def create_indicator_table(self):
        '''Create a table in the data store.'''
        TimeScaleService().create_table(
            query="""
                CREATE TABLE IF NOT EXISTS indicator_results (
                component_name VARCHAR(255) NOT NULL,
                time TIMESTAMPTZ NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (component_name, time)
            )""",
            hyper_table_name='indicator_results',
            hyper_table_column='time'
        )

    def save_indicator_results(self, value: str):
        '''Save the results of the indicator.'''
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        TimeScaleService().execute(
            query=f"""
                INSERT INTO indicator_results (component_name, time, value)
                VALUES ('{self.component_name}', '{timestamp}', '{value}')
            """
        )
        logger.info(f"Saved indicator results for {self.component_name}")
    
    def schedule_work(self):
        '''Scheduled for every minute.'''
        self.create_indicator_table()
        while True:
            result = self.do_work()

            if isinstance(result, str):
                self.save_indicator_results(value=result)
            elif isinstance(result, dict):
                self.save_indicator_results(value=json.dumps(result))
            elif isinstance(result, pd.DataFrame):
                self.save_indicator_results(
                    value=json.dumps(
                        result.to_json(orient='records')
                    )
                )
            elif isinstance(result, int) or isinstance(result, float):
                self.save_indicator_results(value=str(result))
            elif isinstance(result, list):
                self.save_indicator_results(value=json.dumps(result))
            elif result is None:
                pass
            else:
                raise Exception("Invalid type for indicator result.")
            

            logger.info("Sleeping for 30 seconds")
            time.sleep(30)
        