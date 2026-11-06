"""Unit tests for the Parquet serialization infrastructure."""

from io import BytesIO
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

from nexora_data_engineering.infrastructure.serialization.parquet_serializer import (
    ParquetSerializer,
)


def test_serialize_returns_valid_parquet_bytes() -> None:
    # Arrange
    serializer = ParquetSerializer()

    records = [
        {"OrderId": 1, "CustomerName": "John"},
        {"OrderId": 2, "CustomerName": "Jane"},
    ]

    # Act
    content = serializer.serialize(records)

    arrow_table = pq.read_table(BytesIO(content))

    # Assert
    assert len(content) > 0
    assert arrow_table.num_rows == 2
    assert arrow_table.schema.names == ["OrderId", "CustomerName"]


def test_serialize_converts_uuid_values_to_strings() -> None:
    serializer = ParquetSerializer()
    identifier = uuid4()

    content = serializer.serialize([{"Id": identifier}])

    arrow_table = pq.read_table(BytesIO(content))
    result = arrow_table.to_pylist()

    assert result[0]["Id"] == str(identifier)


def test_serialize_rejects_empty_records() -> None:
    serializer = ParquetSerializer()

    with pytest.raises(ValueError, match="empty batch"):
        serializer.serialize([])
