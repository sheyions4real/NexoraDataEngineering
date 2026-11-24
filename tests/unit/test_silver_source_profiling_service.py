"""
MODULE: Silver Source Profiling Service Tests

PURPOSE:
Tests statistical profiling of Arrow datasets.
"""

from decimal import Decimal

import pyarrow as pa

from nexora_data_engineering.application.services.silver_source_profiling_service import (
    SilverSourceProfilingService,
)


def test_profiles_dataset_dimensions() -> None:
    """Verify dataset row and column counts."""

    dataset = pa.table(
        {
            "OrderNumber": ["SO-001", "SO-002", "SO-003"],
            "Amount": [100.00, 200.00, 300.00],
        }
    )

    service = SilverSourceProfilingService()

    profile = service.profile(dataset)

    assert profile.total_rows == 3
    assert profile.total_columns == 2


def test_profiles_null_and_distinct_counts() -> None:
    """Verify null and distinct value calculations."""

    dataset = pa.table(
        {
            "OrderStatus": ["Draft", "Completed", "Draft", None],
        }
    )

    service = SilverSourceProfilingService()

    profile = service.profile(dataset)

    column = profile.columns[0]

    assert column.name == "OrderStatus"
    assert column.total_count == 4
    assert column.null_count == 1
    assert column.distinct_count == 2


def test_profiles_minimum_and_maximum_values() -> None:
    """Verify numeric minimum and maximum values."""

    dataset = pa.table(
        {
            "Amount": pa.array(
                [
                    Decimal("100.00"),
                    Decimal("250.00"),
                    Decimal("175.00"),
                ],
                type=pa.decimal128(18, 2),
            )
        }
    )

    service = SilverSourceProfilingService()

    profile = service.profile(dataset)

    column = profile.columns[0]

    assert column.minimum == "100.00"
    assert column.maximum == "250.00"


def test_handles_empty_dataset() -> None:
    """Verify profiling an empty Arrow dataset."""

    dataset = pa.table(
        {
            "OrderNumber": pa.array([], type=pa.string()),
        }
    )

    service = SilverSourceProfilingService()

    profile = service.profile(dataset)

    assert profile.total_rows == 0
    assert profile.total_columns == 1
    assert profile.columns[0].null_count == 0
    assert profile.columns[0].distinct_count == 0
    assert profile.columns[0].minimum is None
    assert profile.columns[0].maximum is None
