# Bronze Layer – Data Ingestion Pipeline

## 1. Overview

The Bronze layer is the first layer of the Nexora Enterprise Medallion Architecture.

Its responsibility is to extract data from the source operational database (`NexoraEnterprise`) and persist it in Azure Data Lake Storage Gen2 (ADLS Gen2) in its original, raw form.

The Bronze layer acts as the historical landing zone for source data. It preserves the extracted records and captures technical metadata about each ingestion run.

**Source:** SQL Server – `NexoraEnterprise`  
**Target:** Azure Data Lake Storage Gen2 – `nexora` filesystem  
**Storage format:** Apache Parquet  
**Pipeline:** SQL Server → Bronze

## 2. Objectives

The Bronze ingestion pipeline is responsible for:

- Discovering source database schemas and table metadata.
- Extracting records from configured SQL Server tables.
- Supporting batch-based extraction.
- Serializing extracted data into Parquet files.
- Uploading files to ADLS Gen2.
- Verifying uploaded files by downloading and comparing their contents.
- Validating extracted data and its structure.
- Reconciling source, extracted, validated and uploaded record counts.
- Maintaining ingestion audit records.
- Publishing a manifest only after a successful and reconciled ingestion run.

The Bronze layer does not perform business-level transformations or dimensional modelling. Those responsibilities belong to Silver and Gold.

## 3. High-Level Data Flow

```text
SQL Server
NexoraEnterprise
       |
       v
Schema Discovery
       |
       v
Configured Table Selection
       |
       v
Table Extraction
       |
       v
Batch Processing
       |
       v
Parquet Serialization
       |
       v
Upload to ADLS Gen2
       |
       v
Upload Verification
       |
       v
Bronze Data Validation
       |
       v
Reconciliation
       |
       v
Commit Manifest
       |
       v
Successful Ingestion Audit
```

A successful ingestion run produces a committed Bronze dataset and a corresponding successful audit record.

## 4. Project Architecture

The implementation follows a layered architecture that separates business responsibilities from infrastructure concerns.

### Domain Layer

**Location:** `src/nexora_data_engineering/domain/models/`

Contains the data structures used throughout the application.

Key models:

- `TableMetadata` – represents source table metadata, including columns, primary keys and foreign keys.
- `ColumnMetadata` – represents individual source column information, including SQL data type, nullability, default and identity properties.
- `ForeignKeyMetadata` – represents source table relationships.
- `BronzeIngestionResult` – represents the outcome and metrics of an ingestion run.
- `BronzeCommittedRun` – represents a successfully committed Bronze ingestion run.

### Application Layer

**Location:** `src/nexora_data_engineering/application/`

Coordinates ingestion workflows and contains the application-level business logic.

Key components:

| Component | Responsibility |
|---|---|
| `BronzeIngestionPipeline` | Orchestrates the complete Bronze ingestion workflow. |
| `BronzeIngestionRunner` | Executes ingestion for the selected tables and coordinates their results. |
| `SchemaDiscoveryService` | Discovers source database schemas and table metadata. |
| `TableExtractionService` | Coordinates source table extraction. |
| `BronzeValidationService` | Validates extracted data and its structure. |
| `BronzeReconciliationService` | Compares expected and actual ingestion metrics. |
| `BronzeCommittedRunDiscoveryService` | Discovers and verifies previously committed Bronze runs. |

The application layer depends on abstractions defined in the ports directory rather than directly depending on infrastructure implementations.

### Application Ports

**Location:** `src/nexora_data_engineering/application/ports/`

Define the contracts required by the application.

| Port | Responsibility |
|---|---|
| `SchemaInspector` | Defines how source database metadata is inspected. |
| `TableExtractor` | Defines how source table records are extracted. |
| `DataSerializer` | Defines how extracted records are serialized. |
| `DataLakeStorage` | Defines how files are uploaded, downloaded and listed in the data lake. |

### Infrastructure Layer

**Location:** `src/nexora_data_engineering/infrastructure/`

Contains the concrete implementations of the application ports.

| Component | Responsibility |
|---|---|
| `SQLServer` | Establishes connectivity to the source SQL Server database. |
| `SchemaInspector` | Retrieves SQL Server schema and table metadata. |
| `SQLServerTableExtractor` | Extracts source table records in batches. |
| `ParquetSerializer` | Converts extracted records into Parquet format. |
| `AzureDataLakeStorage` | Handles file operations against ADLS Gen2. |

### Configuration and Entry Point

- `config/settings.py` – loads and validates application configuration and environment variables.
- `main.py` – acts as the composition root, initializing infrastructure implementations, injecting dependencies and starting the application.

## 5. Bronze Storage Structure

Data is stored using a partition-style directory structure.

```text
bronze/
└── source_system=sqlserver/
    └── database=NexoraEnterprise/
        └── schema=Sales/
            └── table=SalesOrders/
                └── load_date=YYYY-MM-DD/
                    └── run_id=<unique-run-id>/
                        ├── part-00001.parquet
                        └── manifest.json
```

The structure identifies:

- Source system
- Source database
- Source schema
- Source table
- Load date
- Unique ingestion run

Each table ingestion receives its own unique run ID. This prevents separate runs from overwriting one another and preserves historical ingestion output.

The audit records are maintained separately:

```text
audit/
└── source_system=sqlserver/
    └── database=NexoraEnterprise/
        └── schema=Sales/
            └── table=SalesOrders/
                └── load_date=YYYY-MM-DD/
                    └── run_id=<unique-run-id>/
                        └── audit.json
```

## 6. Ingestion Process

### Step 1 – Schema Discovery

The application connects to SQL Server and discovers configured source schemas.

The schema inspector retrieves table metadata, including:

- Column names and SQL data types.
- Nullable properties.
- Default and identity properties.
- Primary keys.
- Foreign key relationships.

The discovered metadata is represented using the domain models.

### Step 2 – Table Selection

The application uses configuration to determine which tables should be ingested.

Example:

```env
SOURCE_SCHEMAS=Sales,Core,Finance

BRONZE_TABLES=Sales.SalesOrders,Sales.Products,Core.Employees,Core.Customers
```

This separates source discovery from ingestion selection.

### Step 3 – Table Extraction

The table extraction service coordinates the extraction of source records.

Extraction is performed in batches to avoid unnecessarily loading an entire table into application memory.

The configured extraction batch size controls the number of records retrieved per batch.

### Step 4 – Technical Metadata Enrichment

Each extracted dataset receives technical metadata columns to support traceability and downstream processing.

Examples include:

- `_ingested_at_utc`
- `_source_system`
- `_source_database`
- `_source_schema`
- `_source_table`
- `_batch_id`

These columns identify where the records originated and when and under which ingestion run they were extracted.

### Step 5 – Parquet Serialization

The Parquet serializer converts extracted records into Apache Parquet files.

Parquet is a columnar storage format suitable for analytical workloads and efficient downstream processing.

The serialized files are uploaded to the corresponding Bronze run directory.

### Step 6 – Upload Verification

After uploading each file, the pipeline downloads the stored file and compares its bytes with the original serialized content.

This verifies that the uploaded object matches the file produced by the serializer.

The pipeline also tracks uploaded and verified file counts and byte totals.

### Step 7 – Bronze Validation

The Bronze validation service checks the extracted data against the discovered source metadata.

Current validation responsibilities include:

- Schema validation.
- Required/non-nullable column validation.
- Primary-key duplicate detection within each batch.
- Record count validation.
- Parquet file validation.

Bronze validation focuses on structural and technical data quality rather than business transformations.

### Step 8 – Reconciliation

The reconciliation service compares the expected ingestion results with the actual results.

Metrics include:

- Source row count.
- Extracted row count.
- Validated row count.
- Expected and actual batch counts.
- Uploaded and verified file counts.
- Uploaded and verified byte totals.

A run must satisfy the reconciliation requirements before it can be committed.

### Step 9 – Manifest Creation

Once validation and reconciliation succeed, the pipeline writes a manifest describing the committed dataset.

The manifest contains information such as:

- Manifest version and status.
- Pipeline name.
- Run ID.
- Source system, database, schema and table.
- Start and completion timestamps.
- Source, extracted and validated row counts.
- Expected and actual batch counts.
- Uploaded and verified file counts.
- Uploaded and verified byte totals.
- Bronze directory.
- Audit path.
- Data file names.

The manifest status is set to `COMMITTED` only after the ingestion has passed its required checks.

### Step 10 – Audit Completion

The audit service maintains the lifecycle of an ingestion run.

It records execution status, progress, validation and reconciliation information, errors and relevant file metrics.

A successful run must have:

- A committed manifest.
- A successful audit record.

Failed runs retain their audit information and any partially uploaded files for investigation. They are not treated as committed datasets.

## 7. Audit and Reliability

The pipeline maintains an audit record for each table ingestion run.

The audit lifecycle includes execution states such as:

- `RUNNING`
- `FAILED`
- `SUCCEEDED`

It also records validation, reconciliation and manifest statuses.

Examples include:

- `validation_status`
- `reconciliation_status`
- `manifest_status`
- `failed_stage`
- `failed_batch_number`
- `last_successful_batch_number`
- `error_message`

This provides operational traceability and helps identify where an ingestion run failed.

The committed-run discovery service uses the manifest and audit records to identify valid Bronze datasets for downstream processing.

A dataset is eligible for downstream consumption only when its manifest and corresponding audit record satisfy the committed-run requirements.

## 8. Committed Bronze Run Discovery

The `BronzeCommittedRunDiscoveryService` is responsible for discovering previously ingested datasets.

It:

1. Lists available files under the relevant Bronze table directory.
2. Locates and reads manifest files.
3. Confirms that the manifest status is `COMMITTED`.
4. Checks the source table and run identity.
5. Locates the corresponding audit record.
6. Confirms that the audit status is `SUCCEEDED`.
7. Verifies that the expected Parquet files exist.
8. Builds a collection of committed run metadata for downstream processing.

This provides Silver with a controlled way to identify which Bronze datasets are eligible for transformation.

## 9. Current Implementation Scope

The current Bronze implementation supports:

- SQL Server source connectivity.
- Configurable schema and table discovery.
- Batch-based table extraction.
- Parquet serialization.
- ADLS Gen2 file upload and download.
- Upload verification.
- Structural data validation.
- Primary-key duplicate checks within individual batches.
- Source-to-target reconciliation.
- Run-specific audit records.
- Commit manifests.
- Committed-run discovery.

The current implementation uses full table extraction for configured tables.

The following capabilities are intentionally deferred:

- Incremental ingestion using watermarks.
- SQL Server Change Data Capture (CDC).
- Change tracking and deletion detection.
- Cross-run incremental checkpoints.
- Advanced retry and recovery orchestration.
- Cross-batch primary-key duplicate detection.

These will be considered in a later reliability and incremental-ingestion enhancement phase.

## 10. Bronze Layer Outcome

The Bronze layer establishes a reliable raw-data landing zone between the Nexora operational database and the analytical data platform.

Its output is a collection of Parquet datasets accompanied by ingestion manifests and audit records.

The next stage, Silver, will consume committed Bronze datasets and apply data cleansing, standardization, deduplication and business-level data quality rules.

**Bronze responsibility:** Capture and preserve source data reliably.

**Silver responsibility:** Transform raw data into clean, consistent and trustworthy datasets.

**Gold responsibility:** Organize trusted data into business-oriented analytical models for reporting, BI and advanced analytics.
