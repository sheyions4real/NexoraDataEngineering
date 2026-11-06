"""
MODULE: Source Table Metadata Models

ARCHITECTURAL LAYER:
Domain Layer

PURPOSE:
Defines the core metadata structures used to represent the
schema of source database tables.

RESPONSIBILITIES:
- Represent individual database columns.
- Represent primary key definitions.
- Represent foreign key relationships.
- Represent table-level metadata.
- Provide consistent metadata structures across the application.

DATA ENGINEERING ROLE:
These models describe the structure of source data before
the ingestion process begins.

The ingestion engine can use this metadata to understand
column names, data types, nullability, keys and relationships.

DESIGN CONSIDERATIONS:
- These are pure Python domain models.
- They must not depend on SQLAlchemy or database drivers.
- They must not contain database connection logic.
- They should remain reusable regardless of the source
  database technology.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ColumnMetadata:
    """Describes a source database column."""

    name: str
    data_type: str
    nullable: bool
    default: str | None = None
    identity: bool = False


@dataclass(frozen=True, slots=True)
class ForeignKeyMetadata:
    """Describes a foreign key relationship."""

    columns: tuple[str, ...]
    referred_schema: str | None
    referred_table: str
    referred_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TableMetadata:
    """Describes a source database table."""

    schema: str
    name: str
    columns: tuple[ColumnMetadata, ...]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKeyMetadata, ...]
