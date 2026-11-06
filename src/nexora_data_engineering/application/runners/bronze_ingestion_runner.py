"""
MODULE: Bronze Ingestion Runner

ARCHITECTURAL LAYER:
Application Layer / Runners

PURPOSE:
Coordinates Bronze ingestion across multiple source database tables.

RESPONSIBILITIES:
- Accept a collection of discovered source table definitions.
- Execute the Bronze ingestion pipeline for each table.
- Process tables sequentially.
- Capture successful ingestion results.
- Capture individual table failures.
- Continue processing after an individual table fails.
- Return an overall execution summary.

DESIGN CONSIDERATIONS:
- Depends on the BronzeIngestionPipeline abstraction.
- Does not implement extraction, serialization, validation,
  reconciliation, or audit management.
- Does not manage infrastructure resources.
- Uses sequential execution initially.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from nexora_data_engineering.application.pipelines.bronze_ingestion_pipeline import (
    BronzeIngestionPipeline,
)
from nexora_data_engineering.domain.models.bronze_ingestion_result import (
    BronzeIngestionResult,
)
from nexora_data_engineering.domain.models.table_metadata import TableMetadata

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BronzeTableFailure:
    """Describes a failed Bronze ingestion for one source table."""

    source_schema: str
    source_table: str
    error_message: str


@dataclass(frozen=True, slots=True)
class BronzeIngestionRunSummary:
    """Summarizes the outcome of a multi-table Bronze ingestion run."""

    status: str
    total_tables: int
    successful_tables: int
    failed_tables: int
    results: tuple[BronzeIngestionResult, ...]
    failures: tuple[BronzeTableFailure, ...]


class BronzeIngestionRunner:
    """Coordinates sequential Bronze ingestion across multiple tables."""

    def __init__(self, pipeline: BronzeIngestionPipeline) -> None:
        """Initialize the runner with the reusable ingestion pipeline."""

        self._pipeline = pipeline

    def run(
        self,
        tables: Sequence[TableMetadata],
    ) -> BronzeIngestionRunSummary:
        """Execute Bronze ingestion for each supplied table."""

        if not tables:
            raise ValueError("At least one source table must be provided.")

        table_references = [(table.schema.casefold(), table.name.casefold()) for table in tables]

        if len(set(table_references)) != len(table_references):
            raise ValueError("Duplicate source tables were provided to the runner.")

        successful_results: list[BronzeIngestionResult] = []
        failures: list[BronzeTableFailure] = []

        logger.info(
            "Starting multi-table Bronze ingestion | Tables: %d",
            len(tables),
        )

        for index, table in enumerate(tables, start=1):
            logger.info(
                "Processing table %d of %d | %s.%s",
                index,
                len(tables),
                table.schema,
                table.name,
            )

            try:
                result = self._pipeline.run(table)
                successful_results.append(result)

                logger.info(
                    "Table ingestion succeeded | %s.%s | Rows: %d",
                    table.schema,
                    table.name,
                    result.validated_row_count,
                )

            except Exception as exc:
                failure = BronzeTableFailure(
                    source_schema=table.schema,
                    source_table=table.name,
                    error_message=str(exc),
                )

                failures.append(failure)

                logger.exception(
                    "Table ingestion failed | %s.%s | Continuing with the next table.",
                    table.schema,
                    table.name,
                )

        successful_count = len(successful_results)
        failed_count = len(failures)

        if failed_count == 0:
            status = "SUCCEEDED"
        elif successful_count > 0:
            status = "PARTIAL_SUCCESS"
        else:
            status = "FAILED"

        summary = BronzeIngestionRunSummary(
            status=status,
            total_tables=len(tables),
            successful_tables=successful_count,
            failed_tables=failed_count,
            results=tuple(successful_results),
            failures=tuple(failures),
        )

        logger.info(
            "Multi-table Bronze ingestion completed | "
            "Status: %s | Total: %d | Successful: %d | Failed: %d",
            summary.status,
            summary.total_tables,
            summary.successful_tables,
            summary.failed_tables,
        )

        return summary
