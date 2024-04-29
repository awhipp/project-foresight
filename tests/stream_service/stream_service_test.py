"""The tests for the Stream service application."""

import time
from datetime import datetime

import pytest

from foresight.stream_service.app import open_oanda_stream
from foresight.stream_service.app import open_random_walk_stream
from foresight.stream_service.app import process_stream_data
from foresight.stream_service.models.stream import Stream
from foresight.utils.database import TimeScaleService


def test_open_random_walk_stream(create_forex_data_table):
    """Ensure that the random walk stream is working as expected."""

    # ARRANGE
    table_name = create_forex_data_table
    MAX_WALK = 10

    # ACT
    open_random_walk_stream(max_walk=MAX_WALK, table_name=table_name)
    time.sleep(1)

    # ASSERT
    records = TimeScaleService().execute(
        query=f"SELECT * FROM {table_name}",
    )

    for record in records:
        assert record["instrument"] == "EUR_USD"
        assert record["bid"] > 0
        assert record["ask"] > 0

    assert len(records) == MAX_WALK


def test_process_steam_data(create_forex_data_table):
    """Test the process_stream_data method."""

    # ARRANGE
    table_name = create_forex_data_table
    sample_stream_record = Stream(
        instrument="EUR_USD",
        time=datetime.now().isoformat(),
        bids=[{"price": "1.2000"}],
        asks=[{"price": 1.2001}],
    )
    sample_stream_str = sample_stream_record.model_dump_json().encode("utf-8")

    # ACT
    process_stream_data(
        line=sample_stream_str,
        table_name=table_name,
    )

    # ASSERT
    records = TimeScaleService().execute(
        query=f"SELECT * FROM {table_name}",
    )

    assert len(records) == 1  # 1 record
    record = records[0]
    assert record["instrument"] == sample_stream_record.instrument
    assert record["bid"] == sample_stream_record.bids[0].price
    assert record["ask"] == sample_stream_record.asks[0].price
    # Time remove tzinfo
    assert record["time"].replace(tzinfo=None) == sample_stream_record.time.replace(
        tzinfo=None,
    )


def test_open_oanda_stream(create_forex_data_table):
    """Test the open_oanda_stream method."""
    # ARRANGE
    table_name = create_forex_data_table

    # ACT
    open_oanda_stream(run_forever=False, limit=1)

    # ASSERT
    records = TimeScaleService().execute(
        query=f"SELECT * FROM {table_name}",
    )
    assert records is not None


def test_open_oanda_stream_invalid():
    """Test the open_oanda_stream method with invalid arguments."""

    with pytest.raises(ValueError):
        open_oanda_stream(run_forever=False, limit=None)

    with pytest.raises(ValueError):
        open_oanda_stream(run_forever=True, limit=1)
