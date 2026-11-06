"""
MODULE: Ingestion Audit Service

ARCHITECTURAL LAYER:
Application Layer / Services

PURPOSE:
Persists ingestion execution metadata in the
Bronze data lake audit directory.

RESPONSIBILITIES:
- Accept an ingestion audit domain model.
- Serialize the audit record into JSON.
- Construct the audit destination path.
- Create or update the audit record in data lake storage.

DATA ENGINEERING ROLE:
Provides execution traceability for ingestion pipelines.

DESIGN CONSIDERATIONS:
- Depends on the DataLakeStorage abstraction.
- Does not connect directly to Azure.
- Does not manage pipeline execution.
- Uses the run ID to maintain one audit file per run.
- Supports updating an existing audit record.
"""

import json
from dataclasses import asdict
from datetime import datetime
from typing import Any

from nexora_data_engineering.application.ports.data_lake_storage import (
    DataLakeStorage,
)
from nexora_data_engineering.domain.models.ingestion_audit import (
    IngestionAudit,
)


class IngestionAuditService:
    """Serializes and persists ingestion audit records."""

    def __init__(
        self,
        storage: DataLakeStorage,
        audit_root_path: str = "audit",
    ) -> None:
        self._storage = storage
        self._audit_root_path = audit_root_path.strip("/")

        if not self._audit_root_path:
            raise ValueError("Audit root path cannot be empty.")

    def write_audit(
        self,
        audit: IngestionAudit,
        load_date: str,
    ) -> str:
        """Create or update an audit JSON file in the data lake.

        Args:
            audit: Ingestion execution metadata.
            load_date: Partition date in YYYY-MM-DD format.

        Returns:
            The relative ADLS file path.

        Raises:
            ValueError: If the load date is invalid.
        """

        # 1. Validate the partition date.
        try:
            datetime.strptime(load_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Load date must use YYYY-MM-DD format.") from exc

        # 2. Construct the audit destination path.
        audit_directory = (
            f"{self._audit_root_path}/"
            f"source_system={audit.source_system}/"
            f"database={audit.source_database}/"
            f"schema={audit.source_schema}/"
            f"table={audit.source_table}/"
            f"load_date={load_date}/"
            f"run_id={audit.run_id}"
        )

        destination_path = f"{audit_directory}/audit.json"

        # 3. Convert the domain model into a JSON-compatible dictionary.
        audit_data: dict[str, Any] = asdict(audit)

        for timestamp_field in (
            "started_at_utc",
            "completed_at_utc",
        ):
            timestamp = audit_data[timestamp_field]

            if timestamp is not None:
                audit_data[timestamp_field] = timestamp.isoformat()

        # 4. Serialize the audit record.
        audit_content = json.dumps(
            audit_data,
            indent=4,
            ensure_ascii=False,
        ).encode("utf-8")

        # 5. Persist the audit record.
        # overwrite=True updates the same run's audit file.
        self._storage.upload_file(
            file_path=destination_path,
            content=audit_content,
            overwrite=True,
        )

        return destination_path
