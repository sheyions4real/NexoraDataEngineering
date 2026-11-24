"""
MODULE: Silver Referential Integrity Result Models

ARCHITECTURAL LAYER:
Domain Layer

PURPOSE:
Defines the results produced by Silver referential
integrity validation.

RESPONSIBILITIES:
- Represent individual foreign-key validation issues.
- Record the number of affected source records.
- Represent the overall validation outcome.

DATA ENGINEERING ROLE:
These models capture orphaned references discovered
when validating relationships between Silver datasets.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SilverReferentialIntegrityIssue:
    """Describes an invalid foreign-key relationship."""

    source_columns: tuple[str, ...]
    referred_schema: str | None
    referred_table: str
    referred_columns: tuple[str, ...]
    affected_rows: int
    message: str


@dataclass(frozen=True, slots=True)
class SilverReferentialIntegrityResult:
    """Represents the outcome of referential integrity validation."""

    total_rows: int
    passed: bool
    issues: tuple[SilverReferentialIntegrityIssue, ...]
