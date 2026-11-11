"""
MODULE: Failing Table Extractor

ARCHITECTURAL LAYER:
Application Test Support

PURPOSE:
Wraps a real TableExtractor and introduces controlled failures
for testing Bronze ingestion failure handling and recovery.

RESPONSIBILITIES:
- Delegate normal row counts to the underlying extractor.
- Delegate normal batch extraction.
- Simulate a failure after a configurable number of batches.
- Preserve the real extraction behaviour before the failure.
"""

from collections.abc import Iterator
from typing import Any

from nexora_data_engineering.application.ports.table_extractor import (
    TableExtractor,
)
from nexora_data_engineering.domain.models.table_metadata import (
    TableMetadata,
)


class FailingTableExtractor:
    """Inject controlled extraction failures for selected tables."""

    def __init__(
        self,
        extractor: TableExtractor,
        failure_schema: str,
        failure_table: str,
        fail_after_batches: int = 1,
    ) -> None:
        if fail_after_batches < 1:
            raise ValueError("fail_after_batches must be at least 1.")

        self._extractor = extractor
        self._failure_schema = failure_schema.casefold()
        self._failure_table = failure_table.casefold()
        self._fail_after_batches = fail_after_batches

    def _should_fail(self, table: TableMetadata) -> bool:
        return (
            table.schema.casefold() == self._failure_schema
            and table.name.casefold() == self._failure_table
        )

    def count_rows(self, table: TableMetadata) -> int:
        """Delegate source row counting without injecting a failure."""
        return self._extractor.count_rows(table)

    def extract_batches(
        self,
        table: TableMetadata,
        batch_size: int,
    ) -> Iterator[list[dict[str, Any]]]:
        """Extract batches and fail after the configured batch count."""

        batches = self._extractor.extract_batches(table, batch_size)

        if not self._should_fail(table):
            yield from batches
            return

        for batch_number, batch in enumerate(batches, start=1):
            yield batch

            if batch_number >= self._fail_after_batches:
                raise RuntimeError(
                    f"Controlled extraction failure for "
                    f"{table.schema}.{table.name} "
                    f"after {batch_number} uploaded batch(es)."
                )
