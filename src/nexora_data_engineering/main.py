"""
MODULE: Application Entry Point

ARCHITECTURAL LAYER:
Application Entry Point / Composition Root

PURPOSE:
Starts the Nexora Data Engineering application and
coordinates the initial application execution.

RESPONSIBILITIES:
- Load application configuration.
- Initialize application logging.
- Establish source database connectivity.
- Instantiate infrastructure implementations.
- Inject infrastructure dependencies into application services.
- Execute source schema discovery.
- Log discovered database metadata.
- Handle application-level execution failures.

DATA ENGINEERING ROLE:
Acts as the starting point for the data engineering CLI.

Currently, it verifies the source database connection and
discovers the tables and metadata within the Sales schema.

FUTURE RESPONSIBILITIES:
As the platform evolves, this entry point will coordinate
the execution of configured ingestion pipelines.

DESIGN CONSIDERATIONS:
- Acts as the composition root for dependency injection.
- Wires application services to infrastructure implementations.
- Should not contain database-specific extraction logic.
- Should not contain data transformation logic.
- Business and application logic belong in their respective
  application and domain modules.
"""

import json
import logging

# Serialization and upload to azure datalake storage
from datetime import UTC, datetime
from uuid import uuid4

from nexora_data_engineering.application.services.bronze_validation_service import (
    BronzeValidationService,
)

# schema discovery implementations
from nexora_data_engineering.application.services.schema_discovery_service import (
    SchemaDiscoveryService,
)

# table extractor implementation
from nexora_data_engineering.application.services.table_extraction_service import (
    TableExtractionService,
)
from nexora_data_engineering.config.settings import get_settings
from nexora_data_engineering.infrastructure.database.schema_inspector import (
    SQLServerSchemaInspector,
)
from nexora_data_engineering.infrastructure.database.sql_server import (
    get_sql_server_engine,
    test_database_connection,
)
from nexora_data_engineering.infrastructure.database.sql_server_table_extractor import (
    SQLServerTableExtractor,
)
from nexora_data_engineering.infrastructure.serialization.parquet_serializer import (
    ParquetSerializer,
)
from nexora_data_engineering.infrastructure.storage.azure_data_lake import (
    AzureDataLakeStorage,
)


def main() -> None:
    """Application entry point."""
    settings = get_settings()

    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    logger = logging.getLogger(settings.app_name)

    logger.info("Starting %s", settings.app_name)
    logger.info("Environment: %s", settings.app_environment)

    storage_client = None

    # Audit tracking
    audit_record = None
    audit_path = None

    total_rows = 0
    batch_count = 0
    total_bytes = 0
    total_files = 0

    try:
        database_info = test_database_connection()

        logger.info("SQL Server connection successful")
        logger.info("Server: %s", database_info["server_name"])
        logger.info("Database: %s", database_info["database_name"])
        logger.info("Login: %s", database_info["login_name"])

        # Discover Sales schema
        logger.info("Starting source schema discovery: Sales")

        inspector = SQLServerSchemaInspector(get_sql_server_engine())

        # Dependency Injection SQLServerSchemaInspector represents
        # SchemaInspector because of the contract
        discovery_service = SchemaDiscoveryService(inspector)

        # ---------------------------------------------------------
        # 1. Discover configured source schemas
        # ---------------------------------------------------------

        sales_orders_table = None

        for schema in settings.configured_source_schemas:
            logger.info("Starting source schema discovery: %s", schema)

            tables = discovery_service.discover_schema(schema)

            logger.info(
                "Discovered %d tables in %s schema",
                len(tables),
                schema,
            )

            for table in tables:
                logger.info(
                    "Table: %s.%s | Columns: %d | Primary Key: %s | Foreign Keys: %d",
                    table.schema,
                    table.name,
                    len(table.columns),
                    ", ".join(table.primary_key) or "None",
                    len(table.foreign_keys),
                )

                for column in table.columns:
                    logger.info(
                        "    Column: %-30s | Type: %-20s | Nullable: %-5s",
                        column.name,
                        column.data_type,
                        column.nullable,
                    )

                for foreign_key in table.foreign_keys:
                    logger.info(
                        "    Foreign Key: %s -> %s.%s (%s)",
                        ", ".join(foreign_key.columns),
                        foreign_key.referred_schema or "default schema",
                        foreign_key.referred_table,
                        ", ".join(foreign_key.referred_columns),
                    )

                # Select only the SalesOrders table for our first extraction.
                if table.schema.casefold() == "sales" and table.name.casefold() == "salesorders":
                    sales_orders_table = table

        # ---------------------------------------------------------
        # 2. Initialize serialization and storage components
        # ---------------------------------------------------------

        if sales_orders_table is None:
            raise RuntimeError("Sales.SalesOrders was not found in the configured source schemas.")

        logger.info("Initializing Parquet serializer")

        serializer = ParquetSerializer()

        logger.info("Connecting to Azure Data Lake Storage Gen2")

        storage_client = AzureDataLakeStorage(
            account_url=settings.azure_storage_account_url,
            filesystem_name=settings.azure_storage_filesystem,
        )

        # ---------------------------------------------------------
        # 3. Initialize extraction service
        # ---------------------------------------------------------

        extractor = SQLServerTableExtractor(get_sql_server_engine())

        extraction_service = TableExtractionService(extractor)

        logger.info("Initializing Bronze validation service")
        bronze_validator = BronzeValidationService()

        # ---------------------------------------------------------
        # 4. Create ingestion run metadata
        # ---------------------------------------------------------

        run_id = str(uuid4())

        ingestion_timestamp = datetime.now(UTC)

        load_date = ingestion_timestamp.strftime("%Y-%m-%d")

        source_system = "sqlserver"

        source_database = settings.db_database

        source_schema = sales_orders_table.schema

        source_table = sales_orders_table.name

        # ---------------------------------------------------------
        # 4.1 Define expected Bronze schema
        # ---------------------------------------------------------

        ingestion_metadata_columns = (
            "_ingested_at_utc",
            "_source_system",
            "_source_database",
            "_source_schema",
            "_source_table",
            "_batch_id",
        )

        expected_bronze_columns = (
            tuple(column.name for column in sales_orders_table.columns) + ingestion_metadata_columns
        )

        logger.info(
            "Expected Bronze columns: %d",
            len(expected_bronze_columns),
        )

        bronze_directory = (
            f"{settings.bronze_root_path}/"
            f"source_system={source_system}/"
            f"database={source_database}/"
            f"schema={source_schema}/"
            f"table={source_table}/"
            f"load_date={load_date}/"
            f"run_id={run_id}"
        )

        # ---------------------------------------------------------
        # 4.2 Initialize ingestion audit record
        # ---------------------------------------------------------

        audit_record = {
            "run_id": run_id,
            "pipeline_name": "sqlserver_to_bronze",
            "source_system": source_system,
            "source_database": source_database,
            "source_schema": source_schema,
            "source_table": source_table,
            "status": "RUNNING",
            "validation_status": "PENDING",
            "started_at_utc": ingestion_timestamp.isoformat(),
            "completed_at_utc": None,
            "total_rows": 0,
            "total_batches": 0,
            "total_files": 0,
            "total_bytes": 0,
            "error_message": None,
        }

        audit_path = (
            f"audit/"
            f"source_system={source_system}/"
            f"database={source_database}/"
            f"schema={source_schema}/"
            f"table={source_table}/"
            f"load_date={load_date}/"
            f"run_id={run_id}/"
            "audit.json"
        )

        # Persist initial RUNNING status.
        storage_client.upload_file(
            file_path=audit_path,
            content=json.dumps(audit_record, indent=4).encode("utf-8"),
            overwrite=True,
        )

        logger.info("Initial ingestion audit record created: %s", audit_path)

        # ---------------------------------------------------------
        # 5. Extract, serialize and upload each batch
        # ---------------------------------------------------------

        logger.info(
            "Starting Bronze ingestion: %s.%s",
            source_schema,
            source_table,
        )

        for batch in extraction_service.extract_table(
            sales_orders_table,
            batch_size=settings.extraction_batch_size,
        ):
            batch_count += 1

            # total_rows += len(batch)

            # Add technical ingestion metadata to each source record.
            bronze_records = [
                {
                    **record,
                    "_ingested_at_utc": ingestion_timestamp,
                    "_source_system": source_system,
                    "_source_database": source_database,
                    "_source_schema": source_schema,
                    "_source_table": source_table,
                    "_batch_id": run_id,
                }
                for record in batch
            ]

            # Serialize the extracted batch into Parquet bytes.
            parquet_content = serializer.serialize(bronze_records)

            file_name = f"part-{batch_count:05d}.parquet"

            destination_path = f"{bronze_directory}/{file_name}"

            logger.info("Final Bronze destination path: %s", destination_path)
            logger.info("Parquet content size: %s bytes", len(parquet_content))

            if destination_path.endswith("/"):
                raise ValueError(
                    "Bronze destination path must include a filename, not just a directory."
                )

            if not destination_path.endswith(".parquet"):
                raise ValueError("Bronze destination must be a Parquet file.")

            # Upload the Parquet file to ADLS Gen2.
            storage_client.upload_file(
                file_path=destination_path,
                content=parquet_content,
            )

            total_files += 1
            batch_bytes = len(parquet_content)
            total_bytes += batch_bytes

            # ---------------------------------------------------------
            # Bronze: Download and validate uploaded Parquet file
            # ---------------------------------------------------------

            logger.info(
                "Starting Bronze validation for batch %d",
                batch_count,
            )

            # Retrieve the uploaded file from Azure.
            downloaded_content = storage_client.download_file(destination_path)

            # Check that the downloaded bytes match the uploaded bytes.
            if downloaded_content != parquet_content:
                raise RuntimeError(
                    f"Bronze file integrity check failed for batch {batch_count}: "
                    "Downloaded content differs from uploaded content."
                )

            logger.info(
                "Bronze file integrity check passed | Batch: %d | Bytes: %d",
                batch_count,
                len(downloaded_content),
            )

            # Validate Parquet readability, expected columns and record count.
            validation_result = bronze_validator.validate(
                content=downloaded_content,
                expected_columns=expected_bronze_columns,
                expected_row_count=len(batch),
                non_nullable_columns=tuple(
                    column.name for column in sales_orders_table.columns if not column.nullable
                )
                + ingestion_metadata_columns,
                primary_key_columns=sales_orders_table.primary_key,
            )

            if not validation_result.is_valid:
                error_details = "; ".join(validation_result.errors)

                logger.error(
                    "Bronze validation failed | Batch: %d | Errors: %s",
                    batch_count,
                    error_details,
                )

                raise RuntimeError(
                    f"Bronze validation failed for batch {batch_count}: {error_details}"
                )

            total_rows += len(batch)

            logger.info(
                "Bronze validation passed | Batch: %d | Expected rows: %d | "
                "Actual rows: %d | Columns: %d",
                batch_count,
                validation_result.expected_row_count,
                validation_result.actual_row_count,
                len(validation_result.actual_columns),
            )

            # ============================ VALIDATION ENDS ======================

            # ---------------------------------------------------------
            # Update ingestion audit progress
            # ---------------------------------------------------------

            audit_record.update(
                {
                    "validation_status": "IN_PROGRESS",
                    "total_rows": total_rows,
                    "total_batches": batch_count,
                    "total_files": total_files,
                    "total_bytes": total_bytes,
                }
            )

            storage_client.upload_file(
                file_path=audit_path,
                content=json.dumps(audit_record, indent=4).encode("utf-8"),
                overwrite=True,
            )

            logger.info(
                "Uploaded batch %d | Rows: %d | Size: %d bytes | Path: %s",
                batch_count,
                len(batch),
                batch_bytes,
                destination_path,
            )

        # ---------------------------------------------------------
        # 6. Report ingestion results
        # ---------------------------------------------------------

        logger.info("Bronze ingestion completed successfully")

        logger.info("Source: %s.%s", source_schema, source_table)

        logger.info("Ingestion Run ID: %s", run_id)

        logger.info("Total batches: %d", batch_count)

        logger.info("Total rows: %d", total_rows)

        logger.info("Total Parquet size: %d bytes", total_bytes)

        logger.info(
            "ADLS Gen2 destination: %s/%s",
            settings.azure_storage_filesystem,
            bronze_directory,
        )

        logger.info("Validated batches: %d", batch_count)

        # ---------------------------------------------------------
        # Finalize successful ingestion audit
        # ---------------------------------------------------------

        audit_record.update(
            {
                "status": "SUCCEEDED",
                "validation_status": "PASSED",
                "completed_at_utc": datetime.now(UTC).isoformat(),
                "total_rows": total_rows,
                "total_batches": batch_count,
                "total_files": total_files,
                "total_bytes": total_bytes,
            }
        )

        storage_client.upload_file(
            file_path=audit_path,
            content=json.dumps(audit_record, indent=4).encode("utf-8"),
            overwrite=True,
        )

        logger.info("Ingestion audit completed successfully: %s", audit_path)

    except Exception as exc:
        logger.exception("Application execution failed")

        # ---------------------------------------------------------
        # Record failed ingestion audit
        # ---------------------------------------------------------

        if audit_record is not None and audit_path is not None:
            audit_record.update(
                {
                    "status": "FAILED",
                    "validation_status": "FAILED",
                    "completed_at_utc": datetime.now(UTC).isoformat(),
                    "total_rows": total_rows,
                    "total_batches": batch_count,
                    "total_files": total_files,
                    "total_bytes": total_bytes,
                    "error_message": str(exc),
                }
            )

            try:
                storage_client.upload_file(
                    file_path=audit_path,
                    content=json.dumps(audit_record, indent=4).encode("utf-8"),
                    overwrite=True,
                )

                logger.info("Failed ingestion audit recorded: %s", audit_path)

            except Exception:
                logger.exception("Unable to persist failed ingestion audit")

        raise

    finally:
        if storage_client is not None:
            storage_client.close()
        get_sql_server_engine().dispose()


if __name__ == "__main__":
    main()
