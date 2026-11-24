"""
MODULE: Silver Referential Integrity Validation Runner

ARCHITECTURAL LAYER:
Application Layer / Runner

PURPOSE:
Coordinates referential integrity validation across
committed Bronze snapshots.

RESPONSIBILITIES:
- Read committed source table snapshots.
- Discover referenced tables from foreign-key metadata.
- Read and cache referenced table snapshots.
- Select temporally eligible referenced snapshots.
- Execute referential integrity validation.
- Return structured results for each source run and relationship.

DATA ENGINEERING ROLE:
Ensures that source datasets maintain valid relationships
with their referenced datasets before Silver promotion.
"""

import pyarrow as pa

from nexora_data_engineering.application.services import (
    silver_referential_integrity_validation_service,
)
from nexora_data_engineering.application.services.bronze_dataset_reader_service import (
    BronzeDatasetReaderService,
)
from nexora_data_engineering.domain.models.bronze_committed_run import (
    BronzeCommittedRun,
)
from nexora_data_engineering.domain.models.silver_referential_integrity_result import (
    SilverReferentialIntegrityResult,
)
from nexora_data_engineering.domain.models.table_metadata import (
    ForeignKeyMetadata,
    TableMetadata,
)

SilverReferentialIntegrityValidationService = (
    silver_referential_integrity_validation_service.SilverReferentialIntegrityValidationService
)


class SilverReferentialIntegrityValidationRunner:
    """Coordinates referential integrity validation across Bronze snapshots."""

    def __init__(
        self,
        dataset_reader: BronzeDatasetReaderService,
        validation_service: SilverReferentialIntegrityValidationService,
    ) -> None:
        self._dataset_reader = dataset_reader
        self._validation_service = validation_service

    def run(
        self,
        table: TableMetadata,
        available_tables: tuple[TableMetadata, ...],
    ) -> list[
        tuple[
            BronzeCommittedRun,
            ForeignKeyMetadata,
            SilverReferentialIntegrityResult,
        ]
    ]:
        """
        Validate every foreign-key relationship for each source snapshot.

        Referenced snapshots are selected using the latest committed
        completion timestamp that does not exceed the source run's
        completion timestamp.
        """

        source_datasets = self._dataset_reader.read_committed_datasets(table)

        if not source_datasets or not table.foreign_keys:
            return []

        table_metadata = {(item.schema, item.name): item for item in available_tables}

        referenced_datasets: dict[
            tuple[str, str],
            list[tuple[BronzeCommittedRun, pa.Table]],
        ] = {}

        source_table_key = (table.schema, table.name)

        results: list[
            tuple[
                BronzeCommittedRun,
                ForeignKeyMetadata,
                SilverReferentialIntegrityResult,
            ]
        ] = []

        for foreign_key in table.foreign_keys:
            referenced_schema = foreign_key.referred_schema or table.schema
            referenced_table_key = (
                referenced_schema,
                foreign_key.referred_table,
            )

            if referenced_table_key == source_table_key:
                referenced_datasets[referenced_table_key] = source_datasets
                continue

            if referenced_table_key not in table_metadata:
                raise ValueError(
                    "Referenced table metadata was not supplied: "
                    f"{referenced_schema}.{foreign_key.referred_table}"
                )

            if referenced_table_key not in referenced_datasets:
                referenced_table = table_metadata[referenced_table_key]

                referenced_datasets[referenced_table_key] = (
                    self._dataset_reader.read_committed_datasets(referenced_table)
                )

            if not referenced_datasets[referenced_table_key]:
                raise ValueError(
                    "No committed Bronze snapshots are available for "
                    f"{referenced_schema}.{foreign_key.referred_table}."
                )

        for source_run, source_dataset in source_datasets:
            for foreign_key in table.foreign_keys:
                referenced_schema = foreign_key.referred_schema or table.schema
                referenced_table_key = (
                    referenced_schema,
                    foreign_key.referred_table,
                )

                eligible_snapshots = [
                    (referenced_run, referenced_dataset)
                    for referenced_run, referenced_dataset in referenced_datasets[
                        referenced_table_key
                    ]
                    if referenced_run.completed_at_utc <= source_run.completed_at_utc
                ]

                if not eligible_snapshots:
                    raise ValueError(
                        "No temporally eligible referenced snapshot exists "
                        f"for {referenced_schema}.{foreign_key.referred_table} "
                        f"at source run {source_run.run_id}."
                    )

                referenced_run, referenced_dataset = max(
                    eligible_snapshots,
                    key=lambda item: item[0].completed_at_utc,
                )

                result = self._validation_service.validate(
                    source_dataset=source_dataset,
                    referenced_dataset=referenced_dataset,
                    foreign_key=foreign_key,
                )

                results.append((source_run, foreign_key, result))

        return results
