'''Indicator Superclass'''
import sys
from pathlib import Path

import io
import time

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
        
    def do_work(self):
        '''Do work.'''
        raise NotImplementedError("Subclasses must implement this method.")
    
    def schedule_work(self):
        '''Scheduled for every minute.'''
        while True:
            self.do_work()

            logger.info(f"Sleeping for {30} seconds")
            time.sleep(30)
        