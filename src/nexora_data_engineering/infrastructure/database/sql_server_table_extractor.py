"""
MODULE: SQL Server Table Extractor

ARCHITECTURAL LAYER:
Infrastructure Layer / Database Adapter

PURPOSE:
Implements the TableExtractor contract for SQL Server.

RESPONSIBILITIES:
- Reflect the selected source table using SQLAlchemy.
- Construct a SQLAlchemy SELECT statement.
- Execute the source query.
- Retrieve records in batches.
- Convert database rows into standard Python dictionaries.

DATA ENGINEERING ROLE:
Reads actual source records from NexoraEnterprise.

The extracted batches will eventually be passed to the
serialization and storage components for writing Parquet
files into the Bronze layer of Azure Data Lake Storage Gen2.

DESIGN CONSIDERATIONS:
- Uses the SQLAlchemy Engine provided by the application.
- Uses discovered table metadata to identify the source table.
- Does not perform business transformations.
- Does not write to the data lake.
- Keeps database-specific extraction logic in infrastructure.
"""

from collections.abc import Iterator
from typing import Any

from sqlalchemy import MetaData, Table, func, select
from sqlalchemy.engine import Engine

from nexora_data_engineering.domain.models.table_metadata import TableMetadata


class SQLServerTableExtractor:
    """Extracts SQL Server table records in manageable batches."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def extract_batches(
        self,
        table: TableMetadata,
        batch_size: int,
    ) -> Iterator[list[dict[str, Any]]]:
        """Extract records from a source table in batches."""

        if batch_size <= 0:
            raise ValueError("Batch size must be greater than zero.")

        metadata = MetaData()

        source_table = Table(
            table.name,
            metadata,
            schema=table.schema,
            autoload_with=self._engine,
        )

        statement = select(source_table)

        with self._engine.connect() as connection:
            result = connection.execute(statement)

            for partition in result.mappings().partitions(batch_size):
                yield [dict(row) for row in partition]

    def count_rows(self, table: TableMetadata) -> int:
        """Return the current source table record count."""

        source_table = Table(
            table.name,
            MetaData(),
            schema=table.schema,
            autoload_with=self._engine,
        )

        statement = select(func.count()).select_from(source_table)

        with self._engine.connect() as connection:
            result = connection.scalar(statement)

        return int(result or 0)
