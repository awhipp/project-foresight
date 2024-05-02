"""Indicator Superclass"""

import json
import time
from typing import Literal

import pandas as pd
from boto3_type_annotations.sqs import Client

from foresight.utils.aws import get_client
from foresight.utils.exceptions import AbstractClassError
from foresight.utils.logger import generate_logger
from foresight.utils.models.indicator_result import IndicatorResult
from foresight.utils.models.subscription_feed import SubscriptionFeed


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
        """Initialize the Indicator Class"""
        if isinstance(self, Indicator):
            raise AbstractClassError("<Indicator> must be subclassed.")

        # Instantiate variables
        self.component_name = component_name
        self.order_type = order_type
        self.instrument = instrument
        self.timescale = timescale

        # Create Queue
        sqs_client: Client = get_client("sqs")
        queue_name = f"{self.component_name}_{self.instrument}_indicator_queue"
        response = sqs_client.create_queue(QueueName=queue_name)

        logger.info(f"Created queue: {queue_name}")
        self.queue_url = response["QueueUrl"]

        # Add subscription to feed for window service
        SubscriptionFeed(
            queue_url=self.queue_url,
            instrument=self.instrument,
            timescale=self.timescale,
            order_type=self.order_type,
        ).insert_or_update()
        logger.info(f"Added subscription record for {self.component_name}")

    def poll(self):
        """Pulls from Queue and returns a DataFrame."""
        sqs_client: Client = get_client("sqs")

        logger.info(f"Polling from queue: {self.queue_url}")
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

    def format_pricing_data(self) -> dict:
        """'Calculate the all price data for the instrument as a list of json objects"""
        # ! TODO - Should be handled in previous stage

        data = pd.DataFrame(self.pricing)

        # Remove nulls
        data = data[data["price"].notnull()]

        # Round price to 6 decimal places max
        data["price"] = data["price"].round(6)

        self.pricing = data.to_dict("records")

    def do_work(self) -> dict:
        """Calculate the value of the indicator."""
        raise NotImplementedError("Subclasses must implement this method.")

    def schedule_work(self):
        """Scheduled for every minute."""
        IndicatorResult.create_table()

        while True:
            self.poll()

            if len(self.pricing) > 0:
                self.format_pricing_data()

                result = self.do_work()

                IndicatorResult(
                    component_name=self.component_name,
                    value=json.dumps(result),
                ).insert()

            time.sleep(5)
