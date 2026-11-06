"""
MODULE: Schema Inspector Contract

ARCHITECTURAL LAYER:
Application Layer / Ports

PURPOSE:
Defines the contract that source database schema inspectors
must satisfy.

RESPONSIBILITIES:
- Define the discover_tables operation.
- Specify the expected input and output types.
- Abstract database-specific metadata discovery from
  application services.

DATA ENGINEERING ROLE:
Provides a standard interface through which the ingestion
platform can discover source database structures.

DESIGN CONSIDERATIONS:
- Uses Python Protocol for structural typing.
- Does not implement database inspection.
- Does not import database drivers or infrastructure services.
- Allows different source database implementations to satisfy
  the same contract.

IMPLEMENTATIONS MAY INCLUDE:
- SQLServerSchemaInspector
- PostgreSQLSchemaInspector
- OracleSchemaInspector

This abstraction allows the application layer to remain
independent of the underlying database technology.
"""

# this is an interface contract
from typing import Protocol

from nexora_data_engineering.domain.models.table_metadata import TableMetadata


class SchemaInspector(Protocol):
    """Contract for discovering database schema metadata."""

    def discover_tables(self, schema: str) -> list[TableMetadata]:
        """Discover all tables and their metadata in a schema."""
        ...
