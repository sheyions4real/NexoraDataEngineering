"""Unit tests for Bronze Parquet validation."""

from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nexora_data_engineering.application.services.bronze_validation_service import (
    BronzeValidationService,
)


@pytest.fixture
def validator() -> BronzeValidationService:
    return BronzeValidationService()


def create_parquet(records: list[dict]) -> bytes:
    """Create Parquet bytes for validation tests."""

    output = BytesIO()
    table = pa.Table.from_pylist(records)
    pq.write_table(table, output)

    return output.getvalue()


def test_validate_accepts_valid_parquet(validator) -> None:
    records = [
        {"OrderId": 1, "CustomerName": "John"},
        {"OrderId": 2, "CustomerName": "Jane"},
    ]

    result = validator.validate(
        content=create_parquet(records),
        expected_columns=("OrderId", "CustomerName"),
        expected_row_count=2,
        non_nullable_columns=("OrderId",),
        primary_key_columns=("OrderId",),
    )

    assert result.is_valid is True
    assert result.actual_row_count == 2
    assert result.errors == ()


def test_validate_detects_missing_columns(validator) -> None:
    content = create_parquet([{"OrderId": 1}])

    result = validator.validate(
        content=content,
        expected_columns=("OrderId", "CustomerName"),
        expected_row_count=1,
    )

    assert result.is_valid is False
    assert any("Missing expected columns" in error for error in result.errors)


def test_validate_detects_unexpected_columns(validator) -> None:
    content = create_parquet([{"OrderId": 1, "CustomerName": "John", "Unexpected": "value"}])

    result = validator.validate(
        content=content,
        expected_columns=("OrderId", "CustomerName"),
        expected_row_count=1,
    )

    assert result.is_valid is False
    assert any("Unexpected columns" in error for error in result.errors)


def test_validate_detects_row_count_mismatch(validator) -> None:
    content = create_parquet([{"OrderId": 1}])

    result = validator.validate(
        content=content,
        expected_columns=("OrderId",),
        expected_row_count=5,
    )

    assert result.is_valid is False
    assert any("Record count mismatch" in error for error in result.errors)


def test_validate_detects_null_values(validator) -> None:
    content = create_parquet([{"OrderId": None}])

    result = validator.validate(
        content=content,
        expected_columns=("OrderId",),
        expected_row_count=1,
        non_nullable_columns=("OrderId",),
    )

    assert result.is_valid is False
    assert any("null values" in error for error in result.errors)


def test_validate_detects_duplicate_primary_keys(validator) -> None:
    content = create_parquet(
        [
            {"OrderId": 100},
            {"OrderId": 100},
            {"OrderId": 200},
        ]
    )

    result = validator.validate(
        content=content,
        expected_columns=("OrderId",),
        expected_row_count=3,
        primary_key_columns=("OrderId",),
    )

    assert result.is_valid is False
    assert any("duplicate primary key" in error for error in result.errors)
