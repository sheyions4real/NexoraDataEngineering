"""
MODULE: Table Extraction Application Service

ARCHITECTURAL LAYER:
Application Layer / Services

PURPOSE:
Coordinates the extraction of records from source database tables.

RESPONSIBILITIES:
- Accept a discovered table definition.
- Validate the extraction batch size.
- Delegate extraction to the injected TableExtractor.
- Return extracted record batches to the caller.

DATA ENGINEERING ROLE:
Provides the application-level entry point for reading
source records before serialization and Bronze ingestion.

DESIGN CONSIDERATIONS:
- Depends on the TableExtractor abstraction.
- Does not know which database technology is being used.
- Does not construct SQL queries.
- Does not write files or communicate with Azure Storage.
- Keeps extraction orchestration separate from infrastructure.
"""

from collections.abc import Iterator
from typing import Any

from nexora_data_engineering.application.ports.table_extractor import (
    TableExtractor,
)
from nexora_data_engineering.domain.models.table_metadata import TableMetadata


class TableExtractionService:
    """Coordinates source table record extraction."""

    def __init__(self, extractor: TableExtractor) -> None:
        self._extractor = extractor

    def extract_table(
        self,
        table: TableMetadata,
        batch_size: int = 10_000,
    ) -> Iterator[list[dict[str, Any]]]:
        """Extract records from a table using the configured batch size."""

        if batch_size <= 0:
            raise ValueError("Batch size must be greater than zero.")

        return self._extractor.extract_batches(table, batch_size)
