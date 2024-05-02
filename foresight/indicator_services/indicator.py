"""Indicator Superclass"""

import datetime
import json
import time
from typing import Literal

import pandas as pd
from boto3_type_annotations.sqs import Client
from utils.aws import get_client
from utils.database import TimeScaleService
from utils.models.subscription_feed import SubscriptionFeed

from foresight.utils.exceptions import AbstractClassError
from foresight.utils.logger import generate_logger


logger = generate_logger(name=__name__)


class Indicator:
    """Indicator Superclass"""

    component_name: str
    instrument: str
    timescale: str
    queue_url: str
    order_type: str = Literal["bid", "ask", "mid"]
    pricing: dict = {}

    def __init__(
        self,
        component_name: str,
        instrument: str,
        timescale: str,
        order_type: str = "mid",
    ):
        if isinstance(self, Indicator):
            raise AbstractClassError("<Indicator> must be subclassed.")

        self.component_name = component_name
        self.order_type = order_type
        self.instrument = instrument
        self.timescale = timescale

        self.queue_url = self.create_queue()
        self.add_subscription_record()

    def create_queue(self) -> str:
        """Create a queue."""
        sqs_client: Client = get_client("sqs")
        queue_name = f"{self.component_name}_{self.instrument}_indicator_queue"
        response = sqs_client.create_queue(QueueName=queue_name)

        logger.info(f"Created queue: {queue_name}")

        return response["QueueUrl"]

    def add_subscription_record(self):
        """Add a subscription record."""
        SubscriptionFeed(
            queue_url=self.queue_url,
            instrument=self.instrument,
            timescale=self.timescale,
            order_type=self.order_type,
        ).insertOrUpdate()

        logger.info(f"Added subscription record for {self.component_name}")

    def pull_from_queue(self):
        """Pulls from Queue and returns a DataFrame."""
        sqs_client: Client = get_client("sqs")

        logger.info(f"Counting messages in queue: {self.queue_url}")
        response = sqs_client.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        logger.info(
            f"Messages in queue: {response['Attributes']['ApproximateNumberOfMessages']}",
        )

        logger.info(f"Pulling from queue: {self.queue_url}")
        response = sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
        )
        if "Messages" in response:
            message = response["Messages"][0]
            sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )
            self.pricing = json.loads(message["Body"])

    def do_work(self) -> dict:
        """Calculate the value of the indicator."""
        raise NotImplementedError("Subclasses must implement this method.")

    def create_indicator_table(self):
        """Create a table in the data store."""
        # ! TODO - Move to Indicator Results Model
        TimeScaleService().create_table(
            query="""
                CREATE TABLE IF NOT EXISTS indicator_results (
                component_name VARCHAR(255) NOT NULL,
                time TIMESTAMPTZ NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (component_name, time)
            )""",
            hyper_table_name="indicator_results",
            hyper_table_column="time",
        )

    def save_indicator_results(self, value: str):
        """Save the results of the indicator."""
        # ! TODO - Move to Indicator Results Model
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        TimeScaleService().execute(
            query=f"""
                INSERT INTO indicator_results (component_name, time, value)
                VALUES ('{self.component_name}', '{timestamp}', '{value}')
            """,
        )
        logger.info(f"Saved indicator results for {self.component_name}")

    def format_pricing_data(self) -> dict:
        """'Calculate the all price data for the instrument as a list of json objects"""
        # ! TODO - Should be handled in previous stage

        data = pd.DataFrame(self.pricing)

        # Remove nulls
        data = data[data["price"].notnull()]

        # Round price to 6 decimal places max
        data["price"] = data["price"].round(6)

        self.pricing = data.to_dict("records")

    def schedule_work(self):
        """Scheduled for every minute."""
        self.create_indicator_table()
        while True:
            self.pull_from_queue()

            if len(self.pricing) > 0:
                self.format_pricing_data()

                result = self.do_work()

                self.save_indicator_results(value=json.dumps(result))

            time.sleep(5)
