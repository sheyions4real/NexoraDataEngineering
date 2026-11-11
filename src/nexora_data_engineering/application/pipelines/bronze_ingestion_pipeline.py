"""
MODULE: Bronze Ingestion Pipeline

ARCHITECTURAL LAYER:
Application Layer / Pipelines

PURPOSE:
Orchestrates the ingestion of one source database table
into the Bronze layer of Azure Data Lake Storage Gen2.

RESPONSIBILITIES:
- Establish ingestion run metadata and audit tracking.
- Count source records and calculate expected batches.
- Extract source records in configurable batches.
- Add technical ingestion metadata to each record.
- Serialize batches into Parquet files.
- Upload Parquet files to Bronze storage.
- Download uploaded files and verify byte integrity.
- Validate Parquet structure and basic data quality.
- Reconcile source, extracted, validated, and uploaded metrics.
- Create a success manifest after successful reconciliation.
- Persist ingestion progress and final audit status.
- Record failure stage and batch-level progress.

DATA ENGINEERING ROLE:
Implements the reusable single-table Bronze ingestion workflow.

DESIGN CONSIDERATIONS:
- Depends on application ports rather than infrastructure classes.
- Processes one batch at a time to limit memory consumption.
- Does not discover source schemas or select tables.
- Does not manage database or storage client lifecycles.
- Preserves uploaded files when an ingestion run fails.
- Creates a manifest only after successful reconciliation.
- Re-raises failures after attempting to persist the failed audit.
- Leaves multi-table coordination to the ingestion runner.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from nexora_data_engineering.application.ports.data_lake_storage import DataLakeStorage
from nexora_data_engineering.application.ports.data_serializer import DataSerializer
from nexora_data_engineering.application.services.bronze_reconciliation_service import (
    BronzeReconciliationService,
)
from nexora_data_engineering.application.services.bronze_validation_service import (
    BronzeValidationService,
)
from nexora_data_engineering.application.services.table_extraction_service import (
    TableExtractionService,
)
from nexora_data_engineering.domain.models.bronze_ingestion_result import (
    BronzeIngestionResult,
)
from nexora_data_engineering.domain.models.table_metadata import TableMetadata

logger = logging.getLogger(__name__)


class BronzeIngestionPipeline:
    """Orchestrates Bronze ingestion for one source database table."""

    PIPELINE_NAME = "sqlserver_to_bronze"

    INGESTION_METADATA_COLUMNS = (
        "_ingested_at_utc",
        "_source_system",
        "_source_database",
        "_source_schema",
        "_source_table",
        "_batch_id",
    )

    def __init__(
        self,
        extraction_service: TableExtractionService,
        serializer: DataSerializer,
        storage: DataLakeStorage,
        validator: BronzeValidationService,
        reconciler: BronzeReconciliationService,
        source_database: str,
        batch_size: int,
        bronze_root_path: str = "bronze",
        audit_root_path: str = "audit",
        source_system: str = "sqlserver",
    ) -> None:
        """Initialize the pipeline with its required dependencies."""

        if not source_database.strip():
            raise ValueError("Source database must be provided.")

        if batch_size <= 0:
            raise ValueError("Batch size must be greater than zero.")

        self._extraction_service = extraction_service
        self._serializer = serializer
        self._storage = storage
        self._validator = validator
        self._reconciler = reconciler

        self._source_database = source_database
        self._batch_size = batch_size
        self._bronze_root_path = bronze_root_path.strip("/")
        self._audit_root_path = audit_root_path.strip("/")
        self._source_system = source_system

    def run(self, table: TableMetadata) -> BronzeIngestionResult:
        """Ingest one source table into Bronze and return its run metrics."""

        run_id = str(uuid4())
        started_at = datetime.now(UTC)
        load_date = started_at.strftime("%Y-%m-%d")

        source_schema = table.schema
        source_table = table.name

        bronze_directory = (
            f"{self._bronze_root_path}/"
            f"source_system={self._source_system}/"
            f"database={self._source_database}/"
            f"schema={source_schema}/"
            f"table={source_table}/"
            f"load_date={load_date}/"
            f"run_id={run_id}"
        )

        audit_path = (
            f"{self._audit_root_path}/"
            f"source_system={self._source_system}/"
            f"database={self._source_database}/"
            f"schema={source_schema}/"
            f"table={source_table}/"
            f"load_date={load_date}/"
            f"run_id={run_id}/"
            "audit.json"
        )

        manifest_path = f"{bronze_directory}/manifest.json"

        # ---------------------------------------------------------
        # Initialize ingestion metrics and execution tracking
        # ---------------------------------------------------------

        source_row_count = 0
        extracted_rows = 0
        validated_rows = 0

        expected_batch_count: int | None = None
        batch_count = 0
        uploaded_files = 0
        verified_files = 0

        uploaded_bytes = 0
        verified_bytes = 0

        current_stage = "INITIALIZING"
        active_batch_number: int | None = None
        last_successful_batch_number = 0

        validation_failed = False
        reconciliation_result = None
        manifest_created = False

        # ---------------------------------------------------------
        # Initialize the audit record before source extraction.
        # This allows failures during counting or extraction to
        # be recorded against this ingestion run.
        # ---------------------------------------------------------

        audit_record: dict[str, Any] = {
            "run_id": run_id,
            "pipeline_name": self.PIPELINE_NAME,
            "source_system": self._source_system,
            "source_database": self._source_database,
            "source_schema": source_schema,
            "source_table": source_table,
            "status": "RUNNING",
            "validation_status": "PENDING",
            "started_at_utc": started_at.isoformat(),
            "completed_at_utc": None,
            "total_rows": 0,
            "total_batches": 0,
            "total_files": 0,
            "total_bytes": 0,
            "error_message": None,
            "source_row_count": None,
            "extracted_row_count": 0,
            "validated_row_count": 0,
            "expected_batch_count": None,
            "verified_files": 0,
            "verified_bytes": 0,
            "reconciliation_status": "PENDING",
            "reconciliation_errors": [],
            "failed_stage": None,
            "failed_batch_number": None,
            "last_successful_batch_number": 0,
            "manifest_path": manifest_path,
            "manifest_status": "PENDING",
        }

        try:
            # -----------------------------------------------------
            # 1. Persist initial audit status
            # -----------------------------------------------------

            current_stage = "INITIALIZING"
            self._persist_audit(audit_path, audit_record)

            logger.info(
                "Starting Bronze ingestion | Table: %s.%s | Run ID: %s",
                source_schema,
                source_table,
                run_id,
            )

            # -----------------------------------------------------
            # 2. Count source records and calculate expected batches
            # -----------------------------------------------------

            current_stage = "SOURCE_COUNT"
            source_row_count = self._extraction_service.count_rows(table)

            if source_row_count < 0:
                raise RuntimeError("Source row count cannot be negative.")

            expected_batch_count = (source_row_count + self._batch_size - 1) // self._batch_size

            audit_record.update(
                {
                    "source_row_count": source_row_count,
                    "expected_batch_count": expected_batch_count,
                }
            )

            self._persist_audit(audit_path, audit_record)

            logger.info(
                "Source count | Table: %s.%s | Rows: %d | Expected batches: %d",
                source_schema,
                source_table,
                source_row_count,
                expected_batch_count,
            )

            # -----------------------------------------------------
            # 3. Define the expected Bronze schema
            # -----------------------------------------------------

            expected_bronze_columns = (
                tuple(column.name for column in table.columns) + self.INGESTION_METADATA_COLUMNS
            )

            non_nullable_columns = (
                tuple(column.name for column in table.columns if not column.nullable)
                + self.INGESTION_METADATA_COLUMNS
            )

            # -----------------------------------------------------
            # 4. Extract, serialize, upload, verify and validate
            # -----------------------------------------------------

            ingestion_timestamp = datetime.now(UTC)
            current_stage = "EXTRACTING"

            for batch in self._extraction_service.extract_table(
                table,
                batch_size=self._batch_size,
            ):
                # Empty batches do not produce Parquet files.
                if not batch:
                    continue

                batch_count += 1
                active_batch_number = batch_count
                extracted_rows += len(batch)

                # Add technical metadata to each source record.
                bronze_records = [
                    {
                        **record,
                        "_ingested_at_utc": ingestion_timestamp,
                        "_source_system": self._source_system,
                        "_source_database": self._source_database,
                        "_source_schema": source_schema,
                        "_source_table": source_table,
                        "_batch_id": run_id,
                    }
                    for record in batch
                ]

                # -------------------------------------------------
                # 4.1 Serialize the batch into Parquet bytes
                # -------------------------------------------------

                current_stage = "SERIALIZING"
                parquet_content = self._serializer.serialize(bronze_records)

                if not parquet_content:
                    raise RuntimeError(
                        f"Serializer returned empty content for batch {batch_count}."
                    )

                file_name = f"part-{batch_count:05d}.parquet"
                destination_path = f"{bronze_directory}/{file_name}"

                # -------------------------------------------------
                # 4.2 Upload the Parquet file
                # -------------------------------------------------

                current_stage = "UPLOADING"

                self._storage.upload_file(
                    file_path=destination_path,
                    content=parquet_content,
                )

                uploaded_files += 1
                uploaded_bytes += len(parquet_content)

                logger.info(
                    "Uploaded Bronze batch | Batch: %d | Rows: %d | Bytes: %d",
                    batch_count,
                    len(batch),
                    len(parquet_content),
                )

                # -------------------------------------------------
                # 4.3 Verify uploaded file integrity
                # -------------------------------------------------

                current_stage = "VERIFYING"
                downloaded_content = self._storage.download_file(destination_path)

                if downloaded_content != parquet_content:
                    raise RuntimeError(
                        f"Bronze file integrity check failed for batch {batch_count}: "
                        "Downloaded content differs from uploaded content."
                    )

                verified_files += 1
                verified_bytes += len(downloaded_content)

                # -------------------------------------------------
                # 4.4 Validate the downloaded Parquet file
                # -------------------------------------------------

                current_stage = "VALIDATING"
                validation_failed = True

                validation_result = self._validator.validate(
                    content=downloaded_content,
                    expected_columns=expected_bronze_columns,
                    expected_row_count=len(batch),
                    non_nullable_columns=non_nullable_columns,
                    primary_key_columns=table.primary_key,
                )

                if not validation_result.is_valid:
                    error_details = "; ".join(validation_result.errors)

                    raise RuntimeError(
                        f"Bronze validation failed for batch {batch_count}: {error_details}"
                    )

                validation_failed = False
                validated_rows += validation_result.actual_row_count

                logger.info(
                    "Bronze validation passed | Batch: %d | Rows: %d | Columns: %d",
                    batch_count,
                    validation_result.actual_row_count,
                    len(validation_result.actual_columns),
                )

                # -------------------------------------------------
                # 4.5 Persist ingestion progress
                # -------------------------------------------------

                current_stage = "PERSISTING_PROGRESS"

                audit_record.update(
                    {
                        "validation_status": "IN_PROGRESS",
                        "extracted_row_count": extracted_rows,
                        "validated_row_count": validated_rows,
                        "total_rows": validated_rows,
                        "total_batches": batch_count,
                        "total_files": uploaded_files,
                        "total_bytes": uploaded_bytes,
                        "verified_files": verified_files,
                        "verified_bytes": verified_bytes,
                        "last_successful_batch_number": batch_count,
                    }
                )

                self._persist_audit(audit_path, audit_record)

                # A batch is considered successfully completed only
                # after upload, verification, validation and audit
                # progress persistence have all succeeded.
                last_successful_batch_number = batch_count
                active_batch_number = None
                current_stage = "EXTRACTING"

            # -----------------------------------------------------
            # 5. Mark complete extraction and validation
            # -----------------------------------------------------

            current_stage = "VALIDATION_COMPLETED"

            audit_record.update(
                {
                    "validation_status": "PASSED",
                    "extracted_row_count": extracted_rows,
                    "validated_row_count": validated_rows,
                    "total_rows": validated_rows,
                    "total_batches": batch_count,
                    "total_files": uploaded_files,
                    "total_bytes": uploaded_bytes,
                    "verified_files": verified_files,
                    "verified_bytes": verified_bytes,
                    "last_successful_batch_number": last_successful_batch_number,
                }
            )

            self._persist_audit(audit_path, audit_record)

            # -----------------------------------------------------
            # 6. Reconcile the complete ingestion run
            # -----------------------------------------------------

            current_stage = "RECONCILING"

            logger.info(
                "Starting Bronze reconciliation | Table: %s.%s",
                source_schema,
                source_table,
            )

            reconciliation_result = self._reconciler.reconcile(
                source_row_count=source_row_count,
                extracted_row_count=extracted_rows,
                validated_row_count=validated_rows,
                batch_size=self._batch_size,
                actual_batch_count=batch_count,
                uploaded_file_count=uploaded_files,
                verified_file_count=verified_files,
                uploaded_bytes=uploaded_bytes,
                verified_bytes=verified_bytes,
            )

            if not reconciliation_result.is_reconciled:
                error_details = "; ".join(reconciliation_result.errors)

                audit_record.update(
                    {
                        "reconciliation_status": "FAILED",
                        "reconciliation_errors": list(reconciliation_result.errors),
                    }
                )

                raise RuntimeError(f"Bronze ingestion reconciliation failed: {error_details}")

            # -----------------------------------------------------
            # 7. Create the Bronze success manifest
            #
            # The manifest is the commit marker for downstream
            # consumers. It is created only after reconciliation.
            # -----------------------------------------------------

            current_stage = "WRITING_MANIFEST"

            completed_at = datetime.now(UTC)

            manifest_record = {
                "manifest_version": 1,
                "status": "COMMITTED",
                "pipeline_name": self.PIPELINE_NAME,
                "run_id": run_id,
                "source_system": self._source_system,
                "source_database": self._source_database,
                "source_schema": source_schema,
                "source_table": source_table,
                "started_at_utc": started_at.isoformat(),
                "completed_at_utc": completed_at.isoformat(),
                "source_row_count": source_row_count,
                "extracted_row_count": extracted_rows,
                "validated_row_count": validated_rows,
                "expected_batch_count": reconciliation_result.expected_batch_count,
                "actual_batch_count": batch_count,
                "uploaded_file_count": uploaded_files,
                "verified_file_count": verified_files,
                "uploaded_bytes": uploaded_bytes,
                "verified_bytes": verified_bytes,
                "bronze_directory": bronze_directory,
                "audit_path": audit_path,
                "files": [
                    f"part-{batch_number:05d}.parquet"
                    for batch_number in range(1, uploaded_files + 1)
                ],
            }

            self._storage.upload_file(
                file_path=manifest_path,
                content=json.dumps(
                    manifest_record,
                    indent=4,
                ).encode("utf-8"),
                overwrite=False,
            )

            manifest_created = True

            logger.info(
                "Bronze success manifest created | Run ID: %s | Path: %s",
                run_id,
                manifest_path,
            )

            # -----------------------------------------------------
            # 8. Finalize the successful audit record
            # -----------------------------------------------------

            completed_at = datetime.now(UTC)

            audit_record.update(
                {
                    "status": "SUCCEEDED",
                    "validation_status": "PASSED",
                    "reconciliation_status": "PASSED",
                    "completed_at_utc": completed_at.isoformat(),
                    "source_row_count": source_row_count,
                    "extracted_row_count": extracted_rows,
                    "validated_row_count": validated_rows,
                    "total_rows": validated_rows,
                    "expected_batch_count": reconciliation_result.expected_batch_count,
                    "total_batches": batch_count,
                    "total_files": uploaded_files,
                    "verified_files": verified_files,
                    "total_bytes": uploaded_bytes,
                    "verified_bytes": verified_bytes,
                    "reconciliation_errors": [],
                    "error_message": None,
                    "manifest_path": manifest_path,
                    "manifest_status": "CREATED",
                    "failed_stage": None,
                    "failed_batch_number": None,
                    "last_successful_batch_number": last_successful_batch_number,
                }
            )

            current_stage = "FINALIZING_AUDIT"
            self._persist_audit(audit_path, audit_record)

            logger.info(
                "Bronze ingestion completed | Table: %s.%s | Rows: %d | Batches: %d | Files: %d",
                source_schema,
                source_table,
                validated_rows,
                batch_count,
                uploaded_files,
            )

            return BronzeIngestionResult(
                run_id=run_id,
                source_system=self._source_system,
                source_database=self._source_database,
                source_schema=source_schema,
                source_table=source_table,
                status="SUCCEEDED",
                validation_status="PASSED",
                reconciliation_status="PASSED",
                started_at_utc=started_at,
                completed_at_utc=completed_at,
                source_row_count=source_row_count,
                extracted_row_count=extracted_rows,
                validated_row_count=validated_rows,
                expected_batch_count=reconciliation_result.expected_batch_count,
                actual_batch_count=batch_count,
                uploaded_file_count=uploaded_files,
                verified_file_count=verified_files,
                uploaded_bytes=uploaded_bytes,
                verified_bytes=verified_bytes,
                bronze_directory=bronze_directory,
                audit_path=audit_path,
            )

        except Exception as exc:
            logger.exception(
                "Bronze ingestion failed | Table: %s.%s | Run ID: %s",
                source_schema,
                source_table,
                run_id,
            )

            # -----------------------------------------------------
            # 9. Determine the failed batch
            # -----------------------------------------------------

            failed_batch_number = active_batch_number

            if (
                failed_batch_number is None
                and current_stage == "EXTRACTING"
                and expected_batch_count is not None
                and batch_count < expected_batch_count
            ):
                failed_batch_number = batch_count + 1

            # -----------------------------------------------------
            # 10. Attempt to persist the failed audit record
            # -----------------------------------------------------

            audit_record.update(
                {
                    "status": "FAILED",
                    "completed_at_utc": datetime.now(UTC).isoformat(),
                    "source_row_count": source_row_count,
                    "extracted_row_count": extracted_rows,
                    "validated_row_count": validated_rows,
                    "total_rows": validated_rows,
                    "total_batches": batch_count,
                    "total_files": uploaded_files,
                    "total_bytes": uploaded_bytes,
                    "verified_files": verified_files,
                    "verified_bytes": verified_bytes,
                    "error_message": str(exc),
                    "failed_stage": current_stage,
                    "failed_batch_number": failed_batch_number,
                    "last_successful_batch_number": last_successful_batch_number,
                    "manifest_path": manifest_path,
                    "manifest_status": ("CREATED" if manifest_created else "NOT_CREATED"),
                }
            )

            if validation_failed:
                audit_record["validation_status"] = "FAILED"

            if reconciliation_result is not None:
                if not reconciliation_result.is_reconciled:
                    audit_record["reconciliation_status"] = "FAILED"
                    audit_record["reconciliation_errors"] = list(reconciliation_result.errors)

            try:
                self._persist_audit(audit_path, audit_record)

            except Exception:
                logger.exception(
                    "Unable to persist failed ingestion audit | Path: %s",
                    audit_path,
                )

            # Preserve the original exception for the caller.
            raise

    def _persist_audit(
        self,
        audit_path: str,
        audit_record: dict[str, Any],
    ) -> None:
        """Serialize and upload the current ingestion audit record."""

        audit_content = json.dumps(
            audit_record,
            indent=4,
        ).encode("utf-8")

        self._storage.upload_file(
            file_path=audit_path,
            content=audit_content,
            overwrite=True,
        )
