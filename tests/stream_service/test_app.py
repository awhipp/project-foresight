"""The tests for the Stream service application."""

import time
from datetime import datetime

from foresight.models.stream import Stream
from foresight.stream_service.app import open_random_walk_stream
from foresight.stream_service.app import process_stream_data
from foresight.utils.database import TimeScaleService


def test_create_table(create_timescale_table):
    """Test the create_table method."""

    # ARRANGE / ACT
    table_name = create_timescale_table

    # ASSERT
    assert table_name == table_name

    hyper_table_details = TimeScaleService().execute(
        query=f"""SELECT * FROM timescaledb_information.hypertables
        WHERE hypertable_name = '{table_name}'""",
    )

    assert len(hyper_table_details) == 1  # Only one record

    hyper_table_details = hyper_table_details[0]

    assert hyper_table_details["hypertable_schema"] == "public"
    assert hyper_table_details["hypertable_name"] == table_name

    table_schema = TimeScaleService().execute(
        query=f"""SELECT
            column_name, data_type, character_maximum_length, column_default, is_nullable
        FROM INFORMATION_SCHEMA.COLUMNS where table_name = '{table_name}'""",
    )

    expected_columns = {
        "instrument": "character varying",
        "time": "timestamp with time zone",
        "bid": "double precision",
        "ask": "double precision",
    }
    assert len(table_schema) == 4  # 4 columns
    for column in table_schema:
        assert column["data_type"] == expected_columns[column["column_name"]]


def test_open_random_walk_stream(create_timescale_table):
    """Ensure that the random walk stream is working as expected."""

    # ARRANGE
    table_name = create_timescale_table

    # ACT
    open_random_walk_stream(max_walk=10, table_name=table_name)
    time.sleep(1)

    # ASSERT
    records = TimeScaleService().execute(
        query=f"SELECT * FROM {table_name}",
    )

    assert len(records) == 10  # 10 records
    for record in records:
        assert record["instrument"] == "EUR_USD"
        assert record["bid"] > 0
        assert record["ask"] > 0


def test_process_steam_data(create_timescale_table):
    """Test the process_stream_data method."""

    # ARRANGE
    table_name = create_timescale_table
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
