"""
MODULE: Data Lake Storage Contract

ARCHITECTURAL LAYER:
Application Layer / Ports

PURPOSE:
Defines the contract for storing serialized files in a
data lake destination.

RESPONSIBILITIES:
- Define the file upload operation.
- Accept a destination path and file contents.
- Allow implementations to control overwrite behaviour.

DATA ENGINEERING ROLE:
Provides an abstraction for writing serialized Bronze,
Silver and Gold data into a storage destination.

DESIGN CONSIDERATIONS:
- Does not depend on Azure SDKs.
- Does not know how Parquet files are generated.
- Allows alternative storage implementations in the future.
"""

from typing import Protocol


class DataLakeStorage(Protocol):
    """Contract for uploading files into data lake storage."""

    def upload_file(
        self,
        file_path: str,
        content: bytes,
        overwrite: bool = False,
    ) -> None:
        """Upload file contents to the specified data lake path."""
        ...

    def download_file(
        self,
        file_path: str,
    ) -> bytes:
        """Download file contents from the data lake."""
        ...
