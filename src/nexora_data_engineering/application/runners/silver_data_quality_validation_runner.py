"""
MODULE: Silver Data Quality Validation Runner

ARCHITECTURAL LAYER:
Application / Runners

PURPOSE:
Coordinates profiling and data quality validation
of committed Bronze datasets before Silver ingestion.

RESPONSIBILITIES:
- Read committed Bronze datasets.
- Profile each dataset.
- Derive structural validation rules from table metadata.
- Apply configured business validation rules.
- Return validation results for each committed run.
"""

from nexora_data_engineering.application.services.bronze_dataset_reader_service import (
    BronzeDatasetReaderService,
)
from nexora_data_engineering.application.services.silver_data_quality_validation_service import (
    SilverDataQualityValidationService,
)
from nexora_data_engineering.application.services.silver_source_profiling_service import (
    SilverSourceProfilingService,
)
from nexora_data_engineering.domain.models.bronze_committed_run import (
    BronzeCommittedRun,
)
from nexora_data_engineering.domain.models.silver_data_quality_result import (
    SilverDataQualityResult,
)
from nexora_data_engineering.domain.models.silver_dataset_profile import (
    SilverDatasetProfile,
)
from nexora_data_engineering.domain.models.table_metadata import TableMetadata


class SilverDataQualityValidationRunner:
    """Coordinates Silver profiling and quality validation."""

    def __init__(
        self,
        dataset_reader: BronzeDatasetReaderService,
        profiler: SilverSourceProfilingService,
        validator: SilverDataQualityValidationService,
    ) -> None:
        self._dataset_reader = dataset_reader
        self._profiler = profiler
        self._validator = validator

    def run(
        self,
        table: TableMetadata,
        non_negative_columns: tuple[str, ...] = (),
    ) -> list[
        tuple[
            BronzeCommittedRun,
            SilverDatasetProfile,
            SilverDataQualityResult,
        ]
    ]:
        """Profile and validate every committed dataset for a table."""

        datasets = self._dataset_reader.read_committed_datasets(table)

        results = []

        required_columns = tuple(column.name for column in table.columns)

        non_nullable_columns = tuple(column.name for column in table.columns if not column.nullable)

        for committed_run, dataset in datasets:
            profile = self._profiler.profile(dataset)

            validation = self._validator.validate(
                dataset=dataset,
                required_columns=required_columns,
                non_nullable_columns=non_nullable_columns,
                unique_columns=table.primary_key,
                non_negative_columns=non_negative_columns,
            )

            results.append(
                (
                    committed_run,
                    profile,
                    validation,
                )
            )

        return results
