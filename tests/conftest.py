"""Test configuration for Foresight."""

import uuid

import pytest

from foresight.stream_service.app import create_table
from foresight.utils.database import TimeScaleService


@pytest.fixture()
def create_timescale_table():
    """Create a table in the TimescaleDB."""
    table_name = create_table(table_name=f"forex_data_{str(uuid.uuid4())[0:4]}")
    yield table_name
    TimeScaleService().execute(query=f"DROP TABLE {table_name}")
