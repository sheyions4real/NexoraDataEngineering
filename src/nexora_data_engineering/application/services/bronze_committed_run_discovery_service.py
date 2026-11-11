"""
MODULE: Bronze Committed Run Discovery Service

ARCHITECTURAL LAYER:
Application Layer / Services

PURPOSE:
Discovers eligible Bronze ingestion runs for a source table.

RESPONSIBILITIES:
- Locate Bronze manifests for a source table.
- Require a COMMITTED manifest.
- Require a matching SUCCEEDED audit record.
- Verify that manifest data files exist.
- Exclude incomplete or inconsistent runs.
- Select the latest successful run per load date.

DATA ENGINEERING ROLE:
Provides Silver ingestion with reliable Bronze snapshots
without exposing storage-provider implementation details.

DESIGN CONSIDERATIONS:
- Uses the DataLakeStorage application port.
- Does not modify Bronze files or audit records.
- Preserves historical load dates.
- Does not select failed or incomplete ingestion attempts.
"""

import json
import logging
from datetime import date, datetime
from typing import Any

from nexora_data_engineering.application.ports.data_lake_storage import (
    DataLakeStorage,
)
from nexora_data_engineering.domain.models.bronze_committed_run import (
    BronzeCommittedRun,
)
from nexora_data_engineering.domain.models.table_metadata import TableMetadata

logger = logging.getLogger(__name__)


class BronzeCommittedRunDiscoveryService:
    """Discovers committed Bronze snapshots for a source table."""

    def __init__(
        self,
        storage: DataLakeStorage,
        source_database: str,
        bronze_root_path: str = "bronze",
        audit_root_path: str = "audit",
        source_system: str = "sqlserver",
    ) -> None:
        self._storage = storage
        self._source_database = source_database
        self._bronze_root_path = bronze_root_path.strip("/")
        self._audit_root_path = audit_root_path.strip("/")
        self._source_system = source_system

    def discover_committed_runs(
        self,
        table: TableMetadata,
    ) -> list[BronzeCommittedRun]:
        """Discover the latest committed run for each load date.

        Runs are returned in ascending load-date order so downstream
        processing can consume historical snapshots chronologically.
        """

        table_path = (
            f"{self._bronze_root_path}/"
            f"source_system={self._source_system}/"
            f"database={self._source_database}/"
            f"schema={table.schema}/"
            f"table={table.name}"
        )

        audit_table_path = (
            f"{self._audit_root_path}/"
            f"source_system={self._source_system}/"
            f"database={self._source_database}/"
            f"schema={table.schema}/"
            f"table={table.name}"
        )

        logger.info(
            "Discovering committed Bronze runs for %s.%s.",
            table.schema,
            table.name,
        )

        available_files = set(self._storage.list_files(table_path))
        available_audit_files = set(self._storage.list_files(audit_table_path))

        manifest_paths = sorted(path for path in available_files if path.endswith("/manifest.json"))

        committed_runs: list[BronzeCommittedRun] = []

        for manifest_path in manifest_paths:
            committed_run = self._evaluate_manifest(
                manifest_path=manifest_path,
                available_files=available_files,
                available_audit_files=available_audit_files,
                table=table,
            )

            if committed_run is not None:
                committed_runs.append(committed_run)

        # Keep only the latest successful run for each load date.
        latest_by_load_date: dict[date, BronzeCommittedRun] = {}

        for run in committed_runs:
            existing = latest_by_load_date.get(run.load_date)

            if existing is None or (
                run.completed_at_utc,
                run.run_id,
            ) > (
                existing.completed_at_utc,
                existing.run_id,
            ):
                latest_by_load_date[run.load_date] = run

        result = sorted(
            latest_by_load_date.values(),
            key=lambda run: run.load_date,
        )

        logger.info(
            "Discovered %s eligible Bronze snapshot(s) for %s.%s.",
            len(result),
            table.schema,
            table.name,
        )

        return result

    def _evaluate_manifest(
        self,
        manifest_path: str,
        available_files: set[str],
        available_audit_files: set[str],
        table: TableMetadata,
    ) -> BronzeCommittedRun | None:
        """Validate a manifest and its corresponding audit record."""

        bronze_directory = manifest_path.rsplit("/", 1)[0]

        manifest = self._read_json(manifest_path)

        if manifest is None:
            return None

        if manifest.get("status") != "COMMITTED":
            logger.debug(
                "Ignoring non-committed Bronze manifest: %s",
                manifest_path,
            )
            return None

        run_id = manifest.get("run_id")

        if not isinstance(run_id, str) or not run_id:
            logger.warning("Manifest has no valid run_id: %s", manifest_path)
            return None

        expected_identity = {
            "source_system": self._source_system,
            "source_database": self._source_database,
            "source_schema": table.schema,
            "source_table": table.name,
        }

        for field, expected_value in expected_identity.items():
            if manifest.get(field) != expected_value:
                logger.warning(
                    "Manifest identity mismatch for %s: %s",
                    manifest_path,
                    field,
                )
                return None

        if manifest.get("bronze_directory") != bronze_directory:
            logger.warning(
                "Manifest Bronze directory does not match its location: %s",
                manifest_path,
            )
            return None

        path_segments = bronze_directory.split("/")

        load_date_segment = next(
            (part for part in path_segments if part.startswith("load_date=")),
            None,
        )
        run_id_segment = next(
            (part for part in path_segments if part.startswith("run_id=")),
            None,
        )

        if load_date_segment is None or run_id_segment != f"run_id={run_id}":
            logger.warning(
                "Manifest path does not match its run metadata: %s",
                manifest_path,
            )
            return None

        try:
            load_date = date.fromisoformat(load_date_segment.removeprefix("load_date="))
            completed_at_utc = datetime.fromisoformat(manifest["completed_at_utc"])
        except (KeyError, TypeError, ValueError):
            logger.warning(
                "Manifest contains invalid date metadata: %s",
                manifest_path,
            )
            return None

        if completed_at_utc.tzinfo is None or completed_at_utc.utcoffset() is None:
            logger.warning(
                "Manifest completion timestamp is not timezone-aware: %s",
                manifest_path,
            )
            return None

        # Derive the expected audit path from the Bronze run directory.
        bronze_prefix = f"{self._bronze_root_path}/"

        if not bronze_directory.startswith(bronze_prefix):
            logger.warning(
                "Manifest is outside the configured Bronze root: %s",
                manifest_path,
            )
            return None

        relative_run_path = bronze_directory[len(bronze_prefix) :]

        expected_audit_path = f"{self._audit_root_path}/{relative_run_path}/audit.json"

        if manifest.get("audit_path") != expected_audit_path:
            logger.warning(
                "Manifest audit path does not match the expected run path: %s",
                manifest_path,
            )
            return None

        if expected_audit_path not in available_audit_files:
            logger.warning(
                "Committed manifest has no corresponding audit file: %s",
                manifest_path,
            )
            return None

        audit = self._read_json(expected_audit_path)

        if audit is None:
            return None

        if audit.get("status") != "SUCCEEDED":
            logger.debug(
                "Ignoring run without a successful audit: %s",
                expected_audit_path,
            )
            return None

        if audit.get("run_id") != run_id:
            logger.warning(
                "Manifest and audit run IDs do not match: %s",
                manifest_path,
            )
            return None

        for field, expected_value in expected_identity.items():
            if audit.get(field) != expected_value:
                logger.warning(
                    "Audit identity mismatch for %s: %s",
                    expected_audit_path,
                    field,
                )
                return None

        if audit.get("manifest_path") != manifest_path:
            logger.warning(
                "Audit manifest path does not match: %s",
                expected_audit_path,
            )
            return None

        files = manifest.get("files")

        if not isinstance(files, list) or not all(
            isinstance(name, str)
            and name
            and "/" not in name
            and "\\" not in name
            and name not in {".", ".."}
            for name in files
        ):
            logger.warning(
                "Manifest contains an invalid data file list: %s",
                manifest_path,
            )
            return None

        expected_data_paths = {f"{bronze_directory}/{name}" for name in files}

        if not expected_data_paths.issubset(available_files):
            logger.warning(
                "One or more committed Bronze data files are missing: %s",
                manifest_path,
            )
            return None

        uploaded_file_count = manifest.get("uploaded_file_count")
        verified_file_count = manifest.get("verified_file_count")
        source_row_count = manifest.get("source_row_count")

        if (
            not isinstance(source_row_count, int)
            or isinstance(source_row_count, bool)
            or source_row_count < 0
            or uploaded_file_count != len(files)
            or verified_file_count != len(files)
        ):
            logger.warning(
                "Manifest file or row counts are inconsistent: %s",
                manifest_path,
            )
            return None

        return BronzeCommittedRun(
            run_id=run_id,
            source_system=self._source_system,
            source_database=self._source_database,
            source_schema=table.schema,
            source_table=table.name,
            load_date=load_date,
            completed_at_utc=completed_at_utc,
            source_row_count=source_row_count,
            bronze_directory=bronze_directory,
            manifest_path=manifest_path,
            audit_path=expected_audit_path,
            files=tuple(files),
        )

    def _read_json(self, file_path: str) -> dict[str, Any] | None:
        """Read a JSON storage file and return its object contents."""

        content = self._storage.download_file(file_path)

        try:
            document = json.loads(content)

        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.warning(
                "Ignoring invalid JSON file: %s",
                file_path,
            )
            return None

        if not isinstance(document, dict):
            logger.warning(
                "Ignoring JSON file that does not contain an object: %s",
                file_path,
            )
            return None

        return document
