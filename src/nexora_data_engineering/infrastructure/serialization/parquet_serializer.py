"""
MODULE: Parquet Serializer

ARCHITECTURAL LAYER:
Infrastructure Layer / Serialization Adapter

PURPOSE:
Implements the DataSerializer contract using Apache Arrow
and the Parquet columnar file format.

RESPONSIBILITIES:
- Convert extracted Python records into an Arrow table.
- Normalize values that Arrow cannot represent directly.
- Write the Arrow table into Parquet format.
- Return the generated Parquet file contents as bytes.

DATA ENGINEERING ROLE:
Converts extracted SQL Server batches into a compressed,
columnar format suitable for storage and analytical processing
in the Bronze layer of Azure Data Lake Storage Gen2.

DESIGN CONSIDERATIONS:
- Uses PyArrow for serialization.
- Does not connect to SQL Server.
- Does not upload files to Azure.
- Preserves supported source values and column names.
- Converts Python UUID values to strings because UUID objects
  are not directly supported by PyArrow's standard inference.
"""

from collections.abc import Mapping, Sequence
from io import BytesIO
from typing import Any
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq


class ParquetSerializer:
    """Serializes batches of records into Parquet bytes."""

    def serialize(
        self,
        records: Sequence[Mapping[str, Any]],
    ) -> bytes:
        """Convert a batch of records into Parquet file contents."""

        if not records:
            raise ValueError("Cannot serialize an empty batch.")

        normalized_records = [self._normalize_record(record) for record in records]

        arrow_table = pa.Table.from_pylist(normalized_records)

        output = BytesIO()

        pq.write_table(
            arrow_table,
            output,
            compression="snappy",
            use_dictionary=True,
        )

        return output.getvalue()

    @staticmethod
    def _normalize_record(
        record: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Normalize values that need explicit serialization handling."""

        return {
            column: str(value) if isinstance(value, UUID) else value
            for column, value in record.items()
        }
