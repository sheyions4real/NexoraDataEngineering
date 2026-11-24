"""
MODULE: PyArrow Parquet Reader

ARCHITECTURAL LAYER:
Infrastructure Layer / Serialization Adapter

PURPOSE:
Implements the ParquetReader contract using Apache Arrow.

RESPONSIBILITIES:
- Accept Parquet file contents as bytes.
- Read Parquet data using PyArrow.
- Return an Arrow table for downstream processing.

DATA ENGINEERING ROLE:
Allows the Silver layer to consume Parquet datasets
stored in Bronze without converting them prematurely
into ordinary Python records.
"""

from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq


class PyArrowParquetReader:
    """Reads Parquet file contents into an Arrow table."""

    def read(self, content: bytes) -> pa.Table:
        """Deserialize Parquet bytes into an Arrow table."""

        if not content:
            raise ValueError("Cannot read an empty Parquet file.")

        parquet_buffer = BytesIO(content)

        return pq.read_table(parquet_buffer)
