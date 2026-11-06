"""
MODULE: SQL Server Schema Inspector

ARCHITECTURAL LAYER:
Infrastructure Layer / Database Adapter

PURPOSE:
Provides the SQL Server-specific implementation of the
SchemaInspector contract.

RESPONSIBILITIES:
- Inspect SQL Server database schemas using SQLAlchemy.
- Discover tables within a requested schema.
- Retrieve column definitions and data types.
- Discover primary key constraints.
- Discover foreign key relationships.
- Convert SQLAlchemy inspection results into domain metadata models.

DATA ENGINEERING ROLE:
Reads the structural metadata of the production-like
NexoraEnterprise source database.

This metadata will later help the ingestion framework
determine which tables and columns to extract and how to
interpret the source database structure.

DESIGN CONSIDERATIONS:
- Contains SQL Server infrastructure-specific logic.
- Implements the application layer's SchemaInspector contract
  through Python structural typing.
- Returns domain models instead of SQLAlchemy-specific objects.
- Does not perform actual row extraction.
- Does not write data into Azure Data Lake Storage.
"""

from sqlalchemy import Engine, inspect

from nexora_data_engineering.domain.models.table_metadata import (
    ColumnMetadata,
    ForeignKeyMetadata,
    TableMetadata,
)


class SQLServerSchemaInspector:
    """Discovers database metadata using SQLAlchemy inspection."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def discover_tables(self, schema: str) -> list[TableMetadata]:
        """Discover all tables and metadata within a given schema."""
        inspector = inspect(self._engine)

        table_names = inspector.get_table_names(schema=schema)

        discovered_tables: list[TableMetadata] = []

        for table_name in sorted(table_names):
            columns = inspector.get_columns(table_name, schema=schema)
            primary_key = inspector.get_pk_constraint(
                table_name,
                schema=schema,
            )
            foreign_keys = inspector.get_foreign_keys(
                table_name,
                schema=schema,
            )

            column_metadata = tuple(
                ColumnMetadata(
                    name=column["name"],
                    data_type=str(column["type"]),
                    nullable=column["nullable"],
                    default=column.get("default"),
                    identity=bool(column.get("identity")),
                )
                for column in columns
            )

            foreign_key_metadata = tuple(
                ForeignKeyMetadata(
                    columns=tuple(fk.get("constrained_columns") or ()),
                    referred_schema=fk.get("referred_schema"),
                    referred_table=fk.get("referred_table") or "",
                    referred_columns=tuple(fk.get("referred_columns") or ()),
                )
                for fk in foreign_keys
            )

            discovered_tables.append(
                TableMetadata(
                    schema=schema,
                    name=table_name,
                    columns=column_metadata,
                    primary_key=tuple(primary_key.get("constrained_columns") or ()),
                    foreign_keys=foreign_key_metadata,
                )
            )

        return discovered_tables
