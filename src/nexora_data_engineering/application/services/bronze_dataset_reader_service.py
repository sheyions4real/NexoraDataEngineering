"""
MODULE: Bronze Dataset Reader Service

ARCHITECTURAL LAYER:
Application / Services

PURPOSE:
Reads validated, committed Bronze datasets from Azure
Data Lake Storage Gen2 into Apache Arrow tables.

RESPONSIBILITIES:
- Discover committed Bronze runs.
- Download Parquet files belonging to each run.
- Deserialize Parquet files using the ParquetReader contract.
- Combine files belonging to the same committed run.
- Validate the resulting dataset row count.
- Preserve individual Bronze snapshots.

DATA ENGINEERING ROLE:
Provides trusted Bronze datasets as input for subsequent
Silver transformation services.
"""

import posixpath

import pyarrow as pa

from nexora_data_engineering.application.ports.data_lake_storage import (
    DataLakeStorage,
)
from nexora_data_engineering.application.ports.parquet_reader import ParquetReader
from nexora_data_engineering.application.services.bronze_committed_run_discovery_service import (
    BronzeCommittedRunDiscoveryService,
)
from nexora_data_engineering.domain.models.bronze_committed_run import (
    BronzeCommittedRun,
)
from nexora_data_engineering.domain.models.table_metadata import TableMetadata


class BronzeDatasetReaderService:
    """Reads committed Bronze snapshots into Arrow datasets."""

    def __init__(
        self,
        discovery_service: BronzeCommittedRunDiscoveryService,
        storage: DataLakeStorage,
        parquet_reader: ParquetReader,
    ) -> None:
        self._discovery_service = discovery_service
        self._storage = storage
        self._parquet_reader = parquet_reader

    def read_committed_datasets(
        self,
        table: TableMetadata,
    ) -> list[tuple[BronzeCommittedRun, pa.Table]]:
        """
        Discover and read all committed snapshots for a source table.

        Returns each committed run alongside its corresponding
        Arrow dataset, preserving snapshot boundaries.
        """

        committed_runs = self._discovery_service.discover_committed_runs(table)

        datasets: list[tuple[BronzeCommittedRun, pa.Table]] = []

        for committed_run in committed_runs:
            parquet_tables: list[pa.Table] = []

            for file_name in committed_run.files:
                file_path = posixpath.join(
                    committed_run.bronze_directory,
                    file_name,
                )

                content = self._storage.download_file(file_path)

                parquet_table = self._parquet_reader.read(content)

                parquet_tables.append(parquet_table)

            if not parquet_tables:
                raise ValueError(
                    f"Committed Bronze run contains no Parquet files: {committed_run.run_id}"
                )

            expected_schema = parquet_tables[0].schema

            for parquet_table in parquet_tables[1:]:
                if not parquet_table.schema.equals(
                    expected_schema,
                    check_metadata=False,
                ):
                    raise ValueError(
                        "Parquet schema mismatch within committed Bronze run: "
                        f"{committed_run.run_id}"
                    )

            dataset = pa.concat_tables(parquet_tables)

            if dataset.num_rows != committed_run.source_row_count:
                raise ValueError(
                    "Bronze dataset row count does not match committed "
                    f"source count for run {committed_run.run_id}. "
                    f"Expected: {committed_run.source_row_count}, "
                    f"Actual: {dataset.num_rows}"
                )

            datasets.append((committed_run, dataset))

        return datasets
