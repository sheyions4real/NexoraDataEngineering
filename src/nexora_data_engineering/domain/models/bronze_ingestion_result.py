"""
MODULE: Bronze Ingestion Result

ARCHITECTURAL LAYER:
Application Layer / Pipelines

PURPOSE:
Defines the result returned after a single-table Bronze
ingestion pipeline completes successfully.

RESPONSIBILITIES:
- Capture the identity of the ingestion run.
- Report source, extraction, validation, and storage metrics.
- Expose the Bronze and audit destinations.
- Report validation and reconciliation outcomes.

DESIGN CONSIDERATIONS:
- Uses an immutable dataclass.
- Contains no infrastructure dependencies.
- Represents an application workflow outcome.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class BronzeIngestionResult:
    """Represents the successful outcome of a Bronze ingestion run."""

    run_id: str
    source_system: str
    source_database: str
    source_schema: str
    source_table: str

    status: str
    validation_status: str
    reconciliation_status: str

    started_at_utc: datetime
    completed_at_utc: datetime

    source_row_count: int
    extracted_row_count: int
    validated_row_count: int

    expected_batch_count: int
    actual_batch_count: int

    uploaded_file_count: int
    verified_file_count: int

    uploaded_bytes: int
    verified_bytes: int

    bronze_directory: str
    audit_path: str
