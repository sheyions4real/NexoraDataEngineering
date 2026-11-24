"""
MODULE: Application Entry Point

ARCHITECTURAL LAYER:
Application Entry Point / Composition Root

PURPOSE:
Initializes and executes the Nexora multi-table
SQL Server-to-Bronze ingestion application.

RESPONSIBILITIES:
- Load application configuration.
- Initialize application logging.
- Establish source database connectivity.
- Discover configured source schemas.
- Resolve configured Bronze table selections.
- Instantiate infrastructure implementations.
- Inject dependencies into application services and pipelines.
- Execute the multi-table Bronze ingestion runner.
- Log the overall ingestion summary.
- Release infrastructure resources.

DESIGN CONSIDERATIONS:
- Acts as the composition root for dependency injection.
- Does not implement ingestion workflow logic.
- Delegates single-table ingestion to BronzeIngestionPipeline.
- Delegates multi-table coordination to BronzeIngestionRunner.
"""

import argparse
import logging

# FOR TEMP SILVER PHASE
import pyarrow as pa
import pyarrow.parquet as pq

# for Bronze Ingestion
from nexora_data_engineering.application.pipelines.bronze_ingestion_pipeline import (
    BronzeIngestionPipeline,
)
from nexora_data_engineering.application.runners.bronze_ingestion_runner import (
    BronzeIngestionRunner,
)

# ================== SLIVER PHASE IMPORTS ====================
from nexora_data_engineering.application.runners.silver_data_quality_validation_runner import (
    SilverDataQualityValidationRunner,
)
from nexora_data_engineering.application.services.bronze_committed_run_discovery_service import (
    BronzeCommittedRunDiscoveryService,
)
from nexora_data_engineering.application.services.bronze_dataset_reader_service import (
    BronzeDatasetReaderService,
)
from nexora_data_engineering.application.services.bronze_reconciliation_service import (
    BronzeReconciliationService,
)
from nexora_data_engineering.application.services.bronze_validation_service import (
    BronzeValidationService,
)
from nexora_data_engineering.application.services.schema_discovery_service import (
    SchemaDiscoveryService,
)
from nexora_data_engineering.application.services.silver_data_quality_validation_service import (
    SilverDataQualityValidationService,
)
from nexora_data_engineering.application.services.silver_source_profiling_service import (
    SilverSourceProfilingService,
)
from nexora_data_engineering.application.services.table_extraction_service import (
    TableExtractionService,
)

# this was used to simulate a failure in tableExtractor service
# from nexora_data_engineering.application.tests.support.failing_table_extractor import (
#     FailingTableExtractor,
# )
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
from nexora_data_engineering.infrastructure.serialization.parquet_reader import (
    PyArrowParquetReader,
)
from nexora_data_engineering.infrastructure.serialization.parquet_serializer import (
    ParquetSerializer,
)
from nexora_data_engineering.infrastructure.storage.azure_data_lake import (
    AzureDataLakeStorage,
)


def main() -> None:
    """Initialize dependencies and execute multi-table Bronze ingestion."""

    parser = argparse.ArgumentParser(description="Nexora Data Engineering application.")

    parser.add_argument(
        "--discover-bronze",
        action="store_true",
        help="Discover committed Bronze runs without executing ingestion.",
    )
    parser.add_argument(
        "--validate-silver",
        action="store_true",
        help="Profile and validate committed Bronze datasets for Silver.",
    )

    args = parser.parse_args()

    settings = get_settings()

    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    logger = logging.getLogger(settings.app_name)

    logger.info("Starting %s", settings.app_name)
    logger.info("Environment: %s", settings.app_environment)

    engine = None
    storage_client = None

    try:
        # ---------------------------------------------------------
        # 1. Initialize source database connectivity
        # ---------------------------------------------------------

        engine = get_sql_server_engine()
        database_info = test_database_connection()

        logger.info("SQL Server connection successful")
        logger.info("Server: %s", database_info["server_name"])
        logger.info("Database: %s", database_info["database_name"])
        logger.info("Login: %s", database_info["login_name"])

        # ---------------------------------------------------------
        # 2. Discover configured source schemas
        # ---------------------------------------------------------

        inspector = SQLServerSchemaInspector(engine)
        discovery_service = SchemaDiscoveryService(inspector)

        discovered_tables = []

        for schema in settings.configured_source_schemas:
            logger.info("Discovering source schema: %s", schema)

            schema_tables = discovery_service.discover_schema(schema)
            discovered_tables.extend(schema_tables)

            logger.info(
                "Discovered %d tables in schema %s",
                len(schema_tables),
                schema,
            )

        # ---------------------------------------------------------
        # 3. Resolve configured Bronze table selections
        # ---------------------------------------------------------

        discovered_table_lookup = {
            (table.schema.casefold(), table.name.casefold()): table for table in discovered_tables
        }

        selected_tables = []
        missing_tables = []

        for schema, table_name in settings.configured_bronze_tables:
            table_reference = (schema.casefold(), table_name.casefold())
            table_metadata = discovered_table_lookup.get(table_reference)

            if table_metadata is None:
                missing_tables.append(f"{schema}.{table_name}")
                continue

            selected_tables.append(table_metadata)

        if missing_tables:
            raise RuntimeError(
                "Configured Bronze tables were not discovered: " + ", ".join(missing_tables)
            )

        logger.info(
            "Selected %d Bronze tables for ingestion",
            len(selected_tables),
        )

        for table in selected_tables:
            if table.schema != "Sales" or table.name != "SalesOrders":
                continue

            logger.info("========== SILVER SOURCE INSPECTION ==========")

            logger.info("Table: %s.%s", table.schema, table.name)
            logger.info("Primary Key: %s", table.primary_key)

            logger.info("Columns:")

            for column in table.columns:
                logger.info(
                    "Column: %s | SQL Type: %s | Nullable: %s | Identity: %s | Default: %s",
                    column.name,
                    column.data_type,
                    column.nullable,
                    column.identity,
                    column.default,
                )

            logger.info("Foreign Keys:")

            for foreign_key in table.foreign_keys:
                logger.info(
                    "Columns: %s -> %s.%s (%s)",
                    foreign_key.columns,
                    foreign_key.referred_schema,
                    foreign_key.referred_table,
                    foreign_key.referred_columns,
                )

            logger.info("==============================================")

        # for table in selected_tables:
        #     logger.info("Selected table: %s.%s", table.schema, table.name)

        # ---------------------------------------------------------
        # 4. Initialize infrastructure implementations
        # ---------------------------------------------------------

        extractor = SQLServerTableExtractor(engine)

        serializer = ParquetSerializer()

        storage_client = AzureDataLakeStorage(
            account_url=settings.azure_storage_account_url,
            filesystem_name=settings.azure_storage_filesystem,
        )

        # ---------------------------------------------------------
        # 5.1 Execute Silver data quality validation when requested
        # ---------------------------------------------------------

        if args.validate_silver:
            logger.info("Starting Silver data quality validation.")

            sales_orders = next(
                (
                    table
                    for table in selected_tables
                    if table.schema.casefold() == "sales" and table.name.casefold() == "salesorders"
                ),
                None,
            )

            if sales_orders is None:
                raise RuntimeError(
                    "Sales.SalesOrders is not present in the configured Bronze table selections."
                )

            bronze_discovery_service = BronzeCommittedRunDiscoveryService(
                storage=storage_client,
                source_database=settings.db_database,
                bronze_root_path=settings.bronze_root_path,
                audit_root_path="audit",
            )

            dataset_reader = BronzeDatasetReaderService(
                discovery_service=bronze_discovery_service,
                storage=storage_client,
                parquet_reader=PyArrowParquetReader(),
            )

            silver_runner = SilverDataQualityValidationRunner(
                dataset_reader=dataset_reader,
                profiler=SilverSourceProfilingService(),
                validator=SilverDataQualityValidationService(),
            )

            validation_results = silver_runner.run(
                table=sales_orders,
                non_negative_columns=(
                    "TotalGrossAmount",
                    "TotalTaxAmount",
                    "TotalNetAmount",
                ),
            )

            if not validation_results:
                logger.warning("No committed Bronze snapshots found for Sales.SalesOrders.")
                return

            failed_snapshots = 0

            logger.info("================================================")
            logger.info("SILVER DATA QUALITY VALIDATION SUMMARY")
            logger.info("================================================")

            for committed_run, profile, validation in validation_results:
                status = "PASSED" if validation.passed else "FAILED"

                logger.info(
                    "%s | Sales.SalesOrders | Run ID: %s | Rows: %d | Columns: %d",
                    status,
                    committed_run.run_id,
                    profile.total_rows,
                    profile.total_columns,
                )

                logger.info("Column profiling:")

                for column in profile.columns:
                    logger.info(
                        "Column: %s | Type: %s | Rows: %d | Nulls: %d "
                        "| Distinct: %d | Min: %s | Max: %s",
                        column.name,
                        column.data_type,
                        column.total_count,
                        column.null_count,
                        column.distinct_count,
                        column.minimum,
                        column.maximum,
                    )

                if not validation.passed:
                    failed_snapshots += 1

                    for issue in validation.issues:
                        logger.error(
                            "QUALITY ISSUE | Rule: %s | Column: %s "
                            "| Affected rows: %d | Message: %s",
                            issue.rule,
                            issue.column,
                            issue.affected_rows,
                            issue.message,
                        )

            logger.info("Total snapshots: %d", len(validation_results))
            logger.info(
                "Successful snapshots: %d",
                len(validation_results) - failed_snapshots,
            )
            logger.info("Failed snapshots: %d", failed_snapshots)
            logger.info("================================================")

            if failed_snapshots:
                raise RuntimeError(
                    f"Silver validation failed for {failed_snapshots} Bronze snapshot(s)."
                )

            logger.info("Silver data quality validation completed successfully.")
            return

        # ---------------------------------------------------------
        # 5. Execute Bronze discovery when requested
        # ---------------------------------------------------------

        if args.discover_bronze:
            logger.info("Starting Bronze committed-run discovery.")

            discovery_service = BronzeCommittedRunDiscoveryService(
                storage=storage_client,
                source_database=settings.db_database,
                bronze_root_path=settings.bronze_root_path,
                audit_root_path="audit",
            )

            for table in selected_tables:
                logger.info(
                    "Discovering committed runs for %s.%s",
                    table.schema,
                    table.name,
                )

                committed_runs = discovery_service.discover_committed_runs(table)

                if table.schema == "Sales" and table.name == "SalesOrders":
                    if not committed_runs:
                        logger.warning("No Bronze dataset available for inspection.")
                    else:
                        latest_run = committed_runs[-1]

                        logger.info("========== BRONZE PARQUET INSPECTION ==========")
                        logger.info("Run ID: %s", latest_run.run_id)
                        logger.info("Bronze directory: %s", latest_run.bronze_directory)

                        for file_name in latest_run.files:
                            file_path = f"{latest_run.bronze_directory}/{file_name}"

                            parquet_bytes = storage_client.download_file(file_path)

                            parquet_file = pa.BufferReader(parquet_bytes)

                            parquet_schema = pq.read_schema(parquet_file)

                            logger.info("Parquet file: %s", file_name)
                            logger.info("Parquet schema:\n%s", parquet_schema)

                            parquet_file.seek(0)

                            parquet_data = pq.read_table(parquet_file)

                            logger.info("Parquet rows: %d", parquet_data.num_rows)
                            logger.info(
                                "First five records:\n%s",
                                parquet_data.slice(0, 5).to_pylist(),
                            )

                        logger.info("==============================================")

                if not committed_runs:
                    logger.warning(
                        "No committed Bronze runs found for %s.%s",
                        table.schema,
                        table.name,
                    )
                    continue

                for run in committed_runs:
                    logger.info(
                        "COMMITTED | %s.%s | Load date: %s | Rows: %d | Files: %d | Run ID: %s",
                        run.source_schema,
                        run.source_table,
                        run.load_date,
                        run.source_row_count,
                        len(run.files),
                        run.run_id,
                    )

            logger.info("Bronze committed-run discovery completed.")
            return

        # ---------------------------------------------------------
        # 5.2 Initialize application services
        # ---------------------------------------------------------

        extraction_service = TableExtractionService(extractor)

        validation_service = BronzeValidationService()
        reconciliation_service = BronzeReconciliationService()

        # ---------------------------------------------------------
        # 6. Construct the reusable single-table pipeline
        # ---------------------------------------------------------

        bronze_pipeline = BronzeIngestionPipeline(
            extraction_service=extraction_service,
            serializer=serializer,
            storage=storage_client,
            validator=validation_service,
            reconciler=reconciliation_service,
            source_database=settings.db_database,
            batch_size=settings.extraction_batch_size,
            bronze_root_path=settings.bronze_root_path,
        )

        # ---------------------------------------------------------
        # 7. Construct and execute the multi-table runner
        # ---------------------------------------------------------

        bronze_runner = BronzeIngestionRunner(bronze_pipeline)
        summary = bronze_runner.run(selected_tables)

        # ---------------------------------------------------------
        # 8. Log the overall execution summary
        # ---------------------------------------------------------

        logger.info("================================================")
        logger.info("MULTI-TABLE BRONZE INGESTION SUMMARY")
        logger.info("================================================")
        logger.info("Overall status: %s", summary.status)
        logger.info("Total tables: %d", summary.total_tables)
        logger.info("Successful tables: %d", summary.successful_tables)
        logger.info("Failed tables: %d", summary.failed_tables)

        for result in summary.results:
            logger.info(
                "SUCCEEDED | %s.%s | Rows: %d | Files: %d | Run ID: %s",
                result.source_schema,
                result.source_table,
                result.validated_row_count,
                result.uploaded_file_count,
                result.run_id,
            )

        for failure in summary.failures:
            logger.error(
                "FAILED | %s.%s | Error: %s",
                failure.source_schema,
                failure.source_table,
                failure.error_message,
            )

        logger.info("================================================")

        if summary.status != "SUCCEEDED":
            raise RuntimeError(
                f"Multi-table Bronze ingestion completed with status: {summary.status}"
            )

    except Exception:
        logger.exception("Application execution failed")
        raise

    finally:
        # Infrastructure lifecycle remains in the composition root.
        if storage_client is not None:
            storage_client.close()

        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()
