"""
MODULE: Azure Data Lake Storage Adapter

ARCHITECTURAL LAYER:
Infrastructure Layer / Storage Adapter

PURPOSE:
Implements the DataLakeStorage contract using Azure Data
Lake Storage Gen2.

RESPONSIBILITIES:
- Authenticate using Microsoft Entra ID credentials.
- Connect to the configured Azure Storage account.
- Access the configured filesystem.
- Create missing directory paths.
- Upload serialized files to the data lake.
- Respect file overwrite settings.

DATA ENGINEERING ROLE:
Provides the destination storage capability for the Nexora
data ingestion platform.

Initially, it writes extracted source data into the Bronze
layer of the nexora filesystem.

DESIGN CONSIDERATIONS:
- Uses DefaultAzureCredential for Entra ID authentication.
- Uses the ADLS Gen2 DataLakeServiceClient.
- Does not perform extraction or serialization.
- Does not contain business transformation logic.
- Requires appropriate Azure Storage data-plane permissions.
"""

import logging

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)


class AzureDataLakeStorage:
    """Uploads files into Azure Data Lake Storage Gen2."""

    def __init__(
        self,
        account_url: str,
        filesystem_name: str,
    ) -> None:
        self._credential = DefaultAzureCredential()

        self._service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=self._credential,
        )

        self._filesystem_client = self._service_client.get_file_system_client(filesystem_name)

    def upload_file(
        self,
        file_path: str,
        content: bytes,
        overwrite: bool = False,
    ) -> None:
        """Upload file contents to Azure Data Lake Storage Gen2.

        The upload follows the ADLS Gen2 file lifecycle:
        1. Create missing parent directories.
        2. Handle an existing destination file.
        3. Create the destination file.
        4. Append the serialized content.
        5. Flush the content to commit the final file length.
        """

        normalized_path = file_path.strip("/")

        if not normalized_path or "\\" in normalized_path or ".." in normalized_path.split("/"):
            raise ValueError("A valid relative data lake file path is required.")

        if not content:
            raise ValueError("Cannot upload an empty file.")

        # ---------------------------------------------------------
        # 1. Create missing parent directories
        # ---------------------------------------------------------
        path_parts = normalized_path.split("/")
        directory_parts = path_parts[:-1]

        current_directory = ""

        for part in directory_parts:
            current_directory = f"{current_directory}/{part}" if current_directory else part

            directory_client = self._filesystem_client.get_directory_client(current_directory)

            try:
                directory_client.create_directory()
            except ResourceExistsError:
                logger.debug(
                    "ADLS directory already exists: %s",
                    current_directory,
                )

        # ---------------------------------------------------------
        # 2. Prepare destination file
        # ---------------------------------------------------------
        file_client = self._filesystem_client.get_file_client(normalized_path)

        if overwrite:
            try:
                logger.info(
                    "ADLS: Deleting existing file before overwrite: %s",
                    normalized_path,
                )
                file_client.delete_file()
            except ResourceNotFoundError:
                logger.debug(
                    "ADLS: No existing file found to overwrite: %s",
                    normalized_path,
                )

        # ---------------------------------------------------------
        # 3. Create destination file
        # ---------------------------------------------------------
        logger.info(
            "ADLS: Creating destination file: %s",
            normalized_path,
        )

        file_client.create_file()

        logger.info("ADLS: File created successfully.")

        # ---------------------------------------------------------
        # 4. Append serialized content
        # ---------------------------------------------------------
        logger.info(
            "ADLS: Appending %s bytes to destination file.",
            len(content),
        )

        file_client.append_data(
            data=content,
            offset=0,
            length=len(content),
        )

        logger.info("ADLS: Content appended successfully.")

        # ---------------------------------------------------------
        # 5. Commit final file length
        # ---------------------------------------------------------
        logger.info(
            "ADLS: Flushing destination file at position %s.",
            len(content),
        )

        file_client.flush_data(len(content))

        logger.info(
            "ADLS: Upload completed successfully: %s",
            normalized_path,
        )

    def download_file(
        self,
        file_path: str,
    ) -> bytes:
        """Download a file from Azure Data Lake Storage Gen2.

        Args:
            file_path: Relative path of the file inside the filesystem.

        Returns:
            The downloaded file contents as bytes.

        Raises:
            ValueError: If the supplied path is invalid.
            ResourceNotFoundError: If the destination file does not exist.
        """

        normalized_path = file_path.strip("/")

        if not normalized_path or "\\" in normalized_path or ".." in normalized_path.split("/"):
            raise ValueError("A valid relative data lake file path is required.")

        logger.info(
            "ADLS: Downloading file: %s",
            normalized_path,
        )

        file_client = self._filesystem_client.get_file_client(normalized_path)

        download_stream = file_client.download_file()

        content = download_stream.readall()

        logger.info(
            "ADLS: Download completed. Retrieved %s bytes.",
            len(content),
        )

        return content

    def close(self) -> None:
        """Close Azure SDK clients and release credentials."""

        self._service_client.close()
        self._credential.close()
