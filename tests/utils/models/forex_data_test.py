"""Test forex data model."""

import datetime

import pytest

from foresight.utils.database import TimeScaleService
from foresight.utils.models.forex_data import ForexData


def test_valid_forex_data():
    """Test valid forex data."""

    forex_data = ForexData(
        instrument="EUR_USD",
        time="2021-01-01T00:00:00",
        bid=1.0,
        ask=1.0001,
    )
    assert forex_data.instrument == "EUR_USD"
    assert forex_data.time == datetime.datetime(2021, 1, 1, 0, 0)
    assert forex_data.bid == 1.0
    assert forex_data.ask == 1.0001

    forex_data = ForexData(instrument="EUR_USD", time="2021-01-01T00:00:00", price=1.0)
    assert forex_data.instrument == "EUR_USD"
    assert forex_data.time == datetime.datetime(2021, 1, 1, 0, 0)
    assert forex_data.price == 1.0


@pytest.mark.parametrize(
    "invalid_options",
    [
        {
            "instrument": "EUR_USD",
            "time": "2021-01-01T00:00:00",
        },
        {
            "instrument": "EUR_USD",
            "time": "2021-01-01T00:00:00",
            "bid": 1.0,
            "ask": 1.0001,
            "price": 1.0001,
        },
        {
            "instrument": "EUR_USD",
            "time": "2021-01-01T00:00:00",
            "ask": 1.0001,
        },
        {
            "instrument": "EUR_USD",
            "time": "2021-01-01T00:00:00",
            "bid": 1.0,
        },
        {
            "instrument": "EUR_USD",
            "price": 1.0001,
        },
        {
            "time": "2021-01-01T00:00:00",
            "price": 1.0001,
        },
    ],
)
def test_invalid_forex_data(invalid_options):
    """Test invalid forex data."""

    with pytest.raises(ValueError):
        ForexData(**invalid_options)


def test_create_table():
    """Test the create_table method."""

    # ARRANGE / ACT
    table_name = ForexData.create_table()

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


@pytest.mark.usefixtures("setup_forex_data_table")
def test_insert_and_fetch():
    """Insert and fetch forex data."""
    RECORD_COUNT = 1000

    # ARRANGE
    dt = datetime.datetime.now()
    # Zero out the seconds and microseconds
    dt = dt.replace(second=0, microsecond=0)
    dt = dt - datetime.timedelta(seconds=RECORD_COUNT)

    # Create random data for the last 1000 seconds
    data_array = []
    for i in range(RECORD_COUNT):
        data_array.append(
            ForexData(
                instrument="EUR_USD",
                time=dt,
                bid=1.0 + i,
                ask=2.0 + i,
            ),
        )
        dt = dt + datetime.timedelta(seconds=1)

    # Double the array to test the insert_multiple and the time_bucket
    data_array = data_array + data_array

    # ACT
    ForexData.insert_multiple(data=data_array)

    data = ForexData.fetch()

    # ASSERT
    assert len(data) == 1000

    # Ensure that the data is in ascending order of time
    # Ensure that the bid and ask are in ascending order

    for i in range(1, len(data)):
        assert data[i].time > data[i - 1].time
        assert data[i].bid > data[i - 1].bid
        assert data[i].ask > data[i - 1].ask


def test_convert_to_price():
    """Test the convert_to_price method."""

    dt = datetime.datetime(2021, 1, 1, 0, 0)
    forex_data = ForexData(
        instrument="EUR_USD",
        time=dt,
        bid=1.0,
        ask=2.0,
    )

    dt = str(dt)

    data = forex_data.convert_to_price(order_type="bid")
    assert data.price == 1.0

    data = forex_data.convert_to_price(order_type="ask")
    assert data.price == 2.0

    data = forex_data.convert_to_price(order_type="mid")
    assert data.price == 1.5

    with pytest.raises(ValueError):
        forex_data.convert_to_price(order_type="invalid")
