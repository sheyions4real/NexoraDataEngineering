"""
MODULE: Silver Dataset Profile Models

ARCHITECTURAL LAYER:
Domain / Models

PURPOSE:
Defines the results of profiling a dataset before Silver
data transformation.

RESPONSIBILITIES:
- Represent dataset-level statistics.
- Represent individual column statistics.
- Provide framework-independent profile structures.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ColumnProfile:
    """Describes the profile of an individual dataset column."""

    name: str
    data_type: str
    total_count: int
    null_count: int
    distinct_count: int
    minimum: str | None = None
    maximum: str | None = None


@dataclass(frozen=True, slots=True)
class SilverDatasetProfile:
    """Describes the profile of a complete dataset."""

    total_rows: int
    total_columns: int
    columns: tuple[ColumnProfile, ...]
