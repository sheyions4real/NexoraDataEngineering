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

from nexora_data_engineering.application.pipelines.bronze_ingestion_pipeline import (
    BronzeIngestionPipeline,
)
from nexora_data_engineering.application.runners.bronze_ingestion_runner import (
    BronzeIngestionRunner,
)
from nexora_data_engineering.application.services.bronze_committed_run_discovery_service import (
    BronzeCommittedRunDiscoveryService,
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
            logger.info("Selected table: %s.%s", table.schema, table.name)

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
