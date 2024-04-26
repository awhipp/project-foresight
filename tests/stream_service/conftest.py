"""Test configuration for Foresight."""

import pytest

from foresight.stream_service.models.forex_data import ForexData
from foresight.utils.database import TimeScaleService


@pytest.fixture()
def create_forex_data_table():
    """Create a table in the TimescaleDB."""
    table_name = ForexData.create_table()
    yield table_name
    ForexData.drop_table()


@pytest.fixture()
def insert_forex_data():
    """Insert data into the TimescaleDB."""
    # ARRANGE
    table_name = create_forex_data_table
    data = ForexData(
        instrument="EUR_USD",
        time="2021-01-01T00:00:00",
        bid=1.0,
        ask=1.0001,
    )

    # ACT
    data.insert_forex_data()

    # ASSERT
    records = TimeScaleService().execute(
        query=f"SELECT * FROM {table_name}",
    )

    assert len(records) == 1  # 1 record
    assert records[0]["instrument"] == "EUR_USD"
    assert records[0]["bid"] == 1.0
    assert records[0]["ask"] == 1.0001
    assert records[0]["time"] == "2021-01-01 00:00:00+00"
