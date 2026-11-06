"""
MODULE: Data Serializer Contract

ARCHITECTURAL LAYER:
Application Layer / Ports

PURPOSE:
Defines the contract for converting extracted records into
a serialized data format.

DATA ENGINEERING ROLE:
Separates data serialization from source extraction and
destination storage. The initial implementation will use
Apache Parquet for Bronze-layer files.

DESIGN CONSIDERATIONS:
- Defines the serialization operation without specifying
  a particular file format.
- Does not connect to databases or cloud storage.
- Allows alternative serializers to be introduced later.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Protocol


class DataSerializer(Protocol):
    """Contract for serializing batches of extracted records."""

    def serialize(
        self,
        records: Sequence[Mapping[str, Any]],
    ) -> bytes:
        """Serialize records and return the resulting file contents."""
        ...
