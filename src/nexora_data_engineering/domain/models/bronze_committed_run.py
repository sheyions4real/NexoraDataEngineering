"""
MODULE: Bronze Committed Run

ARCHITECTURAL LAYER:
Domain Layer / Models

PURPOSE:
Represents a successfully committed Bronze ingestion run
that is eligible for downstream consumption.

RESPONSIBILITIES:
- Identify the source table and ingestion run.
- Record the load date and completion timestamp.
- Expose the Bronze directory and its data files.
- Reference the manifest and audit records.

DESIGN CONSIDERATIONS:
- Immutable domain model.
- Independent of Azure SDKs and storage implementations.
- Contains only committed-run metadata.
"""

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class BronzeCommittedRun:
    """Represents a validated and committed Bronze snapshot."""

    run_id: str
    source_system: str
    source_database: str
    source_schema: str
    source_table: str
    load_date: date
    completed_at_utc: datetime
    source_row_count: int
    bronze_directory: str
    manifest_path: str
    audit_path: str
    files: tuple[str, ...]
