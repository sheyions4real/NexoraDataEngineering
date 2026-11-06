"""
MODULE: Ingestion Audit Model

ARCHITECTURAL LAYER:
Domain Layer / Models

PURPOSE:
Represents the audit information for a single
data ingestion execution.

RESPONSIBILITIES:
- Identify the ingestion run and source table.
- Track execution timestamps and status.
- Track ingestion and validation metrics.
- Capture failure information.

DESIGN CONSIDERATIONS:
- Contains no database or Azure dependencies.
- Represents the state of one ingestion run.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class IngestionAudit:
    """Represents the audit record for one ingestion run."""

    run_id: str
    pipeline_name: str

    source_system: str
    source_database: str
    source_schema: str
    source_table: str

    started_at_utc: datetime
    completed_at_utc: datetime | None = None

    status: str = "RUNNING"
    validation_status: str = "PENDING"

    total_rows: int = 0
    total_batches: int = 0
    total_files: int = 0
    total_bytes: int = 0

    error_message: str | None = None
