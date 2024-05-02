"""Test the indicator super class"""

import pytest
from boto3_type_annotations.sqs import Client

from foresight.indicator_services.indicator import Indicator
from foresight.utils.aws import get_client
from foresight.utils.exceptions import AbstractClassError
from foresight.utils.models.subscription_feed import SubscriptionFeed


def test_abstract_indicator():
    """Test the Indicator Superclass"""
    with pytest.raises(AbstractClassError):
        Indicator(
            component_name="test",
            instrument="test",
            timescale="test",
        )


def test_valid_indicator(setup_subscription_feed_table):
    """Get a class that inherits from Indicator"""

    # Create a test class that inherits from Indicator
    # This is needed because Indicator is an abstract class
    class TestIndicator(Indicator):
        """Test Indicator Class"""

        def do_work(self):
            return True

    # Expected values
    expected_component_name = "test_indicator"
    expected_instrument = "tst_inst"
    expected_timescale = "tst_ts"
    expected_queue_name = (
        f"{expected_component_name}_{expected_instrument}_indicator_queue"
    )

    # Create an instance of the class
    test = TestIndicator(
        component_name=expected_component_name,
        instrument=expected_instrument,
        timescale=expected_timescale,
    )

    # Test the class variables
    assert test.component_name == expected_component_name
    assert test.instrument == expected_instrument
    assert test.timescale == expected_timescale
    assert test.order_type == "mid"
    assert test.queue_url is not None
    assert test.queue_url.rfind(expected_queue_name) != -1

    # Test the do_work method
    assert test.do_work()

    # Test that the queue was created
    sqs_client: Client = get_client("sqs")
    response = sqs_client.get_queue_attributes(
        QueueUrl=test.queue_url,
        AttributeNames=["All"],
    )
    assert response["Attributes"]["QueueArn"] is not None

    # Test that the subscription feed was created
    subscribed_indicators = SubscriptionFeed.fetch(
        table_name=setup_subscription_feed_table,
    )

    # There should be only one indicator
    assert len(subscribed_indicators) == 1

    # Test that the subscription feed is correct
    assert subscribed_indicators[0].queue_url == test.queue_url
    assert subscribed_indicators[0].instrument == expected_instrument
    assert subscribed_indicators[0].timescale == expected_timescale
    assert subscribed_indicators[0].order_type == "mid"
    assert subscribed_indicators[0].queue_url.rfind(expected_queue_name) != -1
