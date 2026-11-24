"""
MODULE: Bronze Dataset Reader Service Tests

PURPOSE:
Tests the loading and assembly of committed Bronze datasets.

TEST COVERAGE:
- Reading a single Parquet file.
- Combining multiple Parquet files.
- Handling tables without committed runs.
- Rejecting schema mismatches.
- Rejecting row-count mismatches.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from nexora_data_engineering.application.services.bronze_dataset_reader_service import (
    BronzeDatasetReaderService,
)


def create_service(
    committed_runs: list,
    parquet_tables: dict[str, pa.Table],
) -> BronzeDatasetReaderService:
    """Create a reader service with mocked infrastructure dependencies."""

    discovery_service = MagicMock()
    discovery_service.discover_committed_runs.return_value = committed_runs

    storage = MagicMock()
    storage.download_file.side_effect = lambda file_path: file_path.encode("utf-8")

    parquet_reader = MagicMock()
    parquet_reader.read.side_effect = lambda content: parquet_tables[content.decode("utf-8")]

    return BronzeDatasetReaderService(
        discovery_service=discovery_service,
        storage=storage,
        parquet_reader=parquet_reader,
    )


def create_committed_run(
    files: list[str],
    source_row_count: int,
) -> SimpleNamespace:
    """Create representative committed Bronze run metadata."""

    return SimpleNamespace(
        run_id="test-run-001",
        bronze_directory="bronze/Sales/SalesOrders/run_id=test-run-001",
        files=files,
        source_row_count=source_row_count,
    )


def create_table(order_ids: list[int]) -> pa.Table:
    """Create a representative Arrow table."""

    return pa.table(
        {
            "OrderId": order_ids,
            "OrderNumber": [f"SO-{order_id}" for order_id in order_ids],
        }
    )


def test_reads_single_parquet_file() -> None:
    """Verify that one Parquet file produces one Arrow dataset."""

    run = create_committed_run(
        files=["part-00001.parquet"],
        source_row_count=2,
    )

    expected_table = create_table([1, 2])

    service = create_service(
        committed_runs=[run],
        parquet_tables={
            "bronze/Sales/SalesOrders/run_id=test-run-001/part-00001.parquet": expected_table,
        },
    )

    datasets = service.read_committed_datasets(MagicMock())

    assert len(datasets) == 1
    assert datasets[0][0].run_id == "test-run-001"
    assert datasets[0][1].num_rows == 2
    assert datasets[0][1].equals(expected_table)


def test_combines_multiple_parquet_files() -> None:
    """Verify that files from the same run are combined."""

    run = create_committed_run(
        files=[
            "part-00001.parquet",
            "part-00002.parquet",
        ],
        source_row_count=4,
    )

    first_table = create_table([1, 2])
    second_table = create_table([3, 4])

    base_path = "bronze/Sales/SalesOrders/run_id=test-run-001/"

    service = create_service(
        committed_runs=[run],
        parquet_tables={
            f"{base_path}part-00001.parquet": first_table,
            f"{base_path}part-00002.parquet": second_table,
        },
    )

    datasets = service.read_committed_datasets(MagicMock())

    result = datasets[0][1]

    assert result.num_rows == 4
    assert result.column("OrderId").to_pylist() == [1, 2, 3, 4]


def test_returns_empty_list_when_no_committed_runs() -> None:
    """Verify that no committed runs produce an empty result."""

    service = create_service(
        committed_runs=[],
        parquet_tables={},
    )

    datasets = service.read_committed_datasets(MagicMock())

    assert datasets == []


def test_rejects_parquet_schema_mismatch() -> None:
    """Verify that inconsistent schemas within a run are rejected."""

    run = create_committed_run(
        files=[
            "part-00001.parquet",
            "part-00002.parquet",
        ],
        source_row_count=2,
    )

    first_table = pa.table({"OrderId": [1]})
    second_table = pa.table({"OrderNumber": ["SO-002"]})

    base_path = "bronze/Sales/SalesOrders/run_id=test-run-001/"

    service = create_service(
        committed_runs=[run],
        parquet_tables={
            f"{base_path}part-00001.parquet": first_table,
            f"{base_path}part-00002.parquet": second_table,
        },
    )

    with pytest.raises(ValueError, match="schema mismatch"):
        service.read_committed_datasets(MagicMock())


def test_rejects_row_count_mismatch() -> None:
    """Verify that actual rows must match committed source count."""

    run = create_committed_run(
        files=["part-00001.parquet"],
        source_row_count=5,
    )

    actual_table = create_table([1, 2, 3])

    service = create_service(
        committed_runs=[run],
        parquet_tables={
            "bronze/Sales/SalesOrders/run_id=test-run-001/part-00001.parquet": actual_table,
        },
    )

    with pytest.raises(ValueError, match="row count does not match"):
        service.read_committed_datasets(MagicMock())
