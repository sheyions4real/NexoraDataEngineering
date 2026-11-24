"""
MODULE: Silver Data Quality Validation Runner Tests

PURPOSE:
Tests the coordination of Bronze dataset reading,
Silver profiling and data quality validation.
"""

from types import SimpleNamespace

import pyarrow as pa

from nexora_data_engineering.application.runners.silver_data_quality_validation_runner import (
    SilverDataQualityValidationRunner,
)
from nexora_data_engineering.application.services.silver_data_quality_validation_service import (
    SilverDataQualityValidationService,
)
from nexora_data_engineering.application.services.silver_source_profiling_service import (
    SilverSourceProfilingService,
)
from nexora_data_engineering.domain.models.table_metadata import (
    ColumnMetadata,
    TableMetadata,
)


class FakeBronzeDatasetReader:
    """Provides committed datasets for runner tests."""

    def __init__(
        self,
        datasets: list[tuple[object, pa.Table]],
    ) -> None:
        self.datasets = datasets

    def read_committed_datasets(
        self,
        table: TableMetadata,
    ) -> list[tuple[object, pa.Table]]:
        return self.datasets


def create_sales_orders_metadata() -> TableMetadata:
    """Create representative SalesOrders source metadata."""

    return TableMetadata(
        schema="Sales",
        name="SalesOrders",
        columns=(
            ColumnMetadata(
                name="Id",
                data_type="UNIQUEIDENTIFIER",
                nullable=False,
            ),
            ColumnMetadata(
                name="TotalNetAmount",
                data_type="DECIMAL(18,2)",
                nullable=False,
            ),
        ),
        primary_key=("Id",),
        foreign_keys=(),
    )


def test_runner_profiles_and_validates_committed_dataset() -> None:
    dataset = pa.table(
        {
            "Id": ["ORDER-001", "ORDER-002"],
            "TotalNetAmount": [100.00, 250.00],
        }
    )

    committed_run = SimpleNamespace(run_id="test-run-001")

    reader = FakeBronzeDatasetReader([(committed_run, dataset)])

    runner = SilverDataQualityValidationRunner(
        dataset_reader=reader,
        profiler=SilverSourceProfilingService(),
        validator=SilverDataQualityValidationService(),
    )

    results = runner.run(
        table=create_sales_orders_metadata(),
        non_negative_columns=("TotalNetAmount",),
    )

    assert len(results) == 1

    run, profile, validation = results[0]

    assert run.run_id == "test-run-001"
    assert profile.total_rows == 2
    assert profile.total_columns == 2
    assert validation.passed is True
    assert validation.issues == ()


def test_runner_detects_invalid_sales_order_amounts() -> None:
    dataset = pa.table(
        {
            "Id": ["ORDER-001", "ORDER-002"],
            "TotalNetAmount": [100.00, -25.00],
        }
    )

    committed_run = SimpleNamespace(run_id="test-run-002")

    reader = FakeBronzeDatasetReader([(committed_run, dataset)])

    runner = SilverDataQualityValidationRunner(
        dataset_reader=reader,
        profiler=SilverSourceProfilingService(),
        validator=SilverDataQualityValidationService(),
    )

    results = runner.run(
        table=create_sales_orders_metadata(),
        non_negative_columns=("TotalNetAmount",),
    )

    validation = results[0][2]

    assert validation.passed is False
    assert any(
        issue.rule == "NON_NEGATIVE"
        and issue.column == "TotalNetAmount"
        and issue.affected_rows == 1
        for issue in validation.issues
    )
