"""
MODULE: Parquet Reader Contract

ARCHITECTURAL LAYER:
Application Layer / Ports

PURPOSE:
Defines the contract for reading Parquet data into an
in-memory columnar representation.

RESPONSIBILITIES:
- Accept serialized Parquet file contents.
- Return a PyArrow table for downstream processing.

DATA ENGINEERING ROLE:
Provides Silver processing with a consistent interface
for reading Bronze Parquet datasets.

DESIGN CONSIDERATIONS:
- Does not connect to Azure or SQL Server.
- Does not perform data cleansing or transformation.
- Allows the underlying Parquet implementation to be replaced.
"""

from typing import Protocol

import pyarrow as pa


class ParquetReader(Protocol):
    """Contract for reading Parquet file contents."""

    def read(self, content: bytes) -> pa.Table:
        """Read Parquet bytes and return an Arrow table."""
        ...
