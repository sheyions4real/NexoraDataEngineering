"""
MODULE: Table Extractor Contract

ARCHITECTURAL LAYER:
Application Layer / Ports

PURPOSE:
Defines the contract for extracting records from source
database tables.

RESPONSIBILITIES:
- Define the batch extraction operation.
- Specify the expected table metadata and batch size.
- Return extracted records in manageable batches.

DATA ENGINEERING ROLE:
Provides a common abstraction for extracting source data
without coupling application services to a particular
database technology.

DESIGN CONSIDERATIONS:
- Uses Python structural typing through Protocol.
- Does not execute SQL queries.
- Does not manage database connections.
- Does not serialize data into Parquet.
- Supports different database extraction implementations.
"""

from collections.abc import Iterator
from typing import Any, Protocol

from nexora_data_engineering.domain.models.table_metadata import TableMetadata


class TableExtractor(Protocol):
    """Contract for extracting records from a source table."""

    def extract_batches(
        self,
        table: TableMetadata,
        batch_size: int,
    ) -> Iterator[list[dict[str, Any]]]:
        """Extract table records in batches."""
        ...

    def count_rows(self, table: TableMetadata) -> int:
        """Return the number of records in the source table."""
        ...
