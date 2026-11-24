"""
MODULE: Silver Data Quality Result

ARCHITECTURAL LAYER:
Domain / Models

PURPOSE:
Defines the outcome of Silver data quality validation.

RESPONSIBILITIES:
- Represent individual data quality issues.
- Record affected row counts.
- Represent the overall validation outcome.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SilverDataQualityIssue:
    """Represents an individual data quality issue."""

    rule: str
    column: str
    affected_rows: int
    message: str


@dataclass(frozen=True, slots=True)
class SilverDataQualityResult:
    """Represents the complete validation outcome."""

    total_rows: int
    passed: bool
    issues: tuple[SilverDataQualityIssue, ...]
