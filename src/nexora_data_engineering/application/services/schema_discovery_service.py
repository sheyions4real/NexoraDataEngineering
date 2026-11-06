"""
MODULE: Schema Discovery Application Service

ARCHITECTURAL LAYER:
Application Layer / Services

PURPOSE:
Coordinates source database schema discovery by using
the SchemaInspector contract.

RESPONSIBILITIES:
- Accept a schema name from the caller.
- Validate the requested schema name.
- Delegate metadata discovery to the injected inspector.
- Return discovered table metadata to the caller.

DATA ENGINEERING ROLE:
Acts as the application-level entry point for discovering
the source database structures that will eventually be
processed by ingestion pipelines.

DESIGN CONSIDERATIONS:
- Depends on the SchemaInspector abstraction rather than
  a specific database implementation.
- Does not create database connections.
- Does not execute database-specific SQL.
- Does not extract actual table records.
- Keeps application orchestration separate from infrastructure.

This service can work with any implementation that satisfies
the SchemaInspector contract.
"""

from nexora_data_engineering.application.ports.schema_inspector import (
    SchemaInspector,
)
from nexora_data_engineering.domain.models.table_metadata import TableMetadata


class SchemaDiscoveryService:
    """Application service for source schema discovery."""

    def __init__(self, inspector: SchemaInspector) -> None:
        self._inspector = inspector

    def discover_schema(self, schema: str) -> list[TableMetadata]:
        """Discover all tables in the requested schema."""
        if not schema or not schema.strip():
            raise ValueError("Schema name cannot be empty.")

        return self._inspector.discover_tables(schema.strip())
