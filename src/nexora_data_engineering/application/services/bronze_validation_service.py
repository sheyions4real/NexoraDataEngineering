"""
MODULE: Bronze Validation Service

ARCHITECTURAL LAYER:
Application Layer / Services

PURPOSE:
Validates the structural and basic data quality
of Parquet files written to the Bronze layer.

RESPONSIBILITIES:
- Validate Parquet readability.
- Validate exact column membership.
- Validate expected record counts.
- Validate non-nullable columns.
- Detect duplicate primary keys within a batch.

DESIGN CONSIDERATIONS:
- Does not connect directly to SQL Server or Azure.
- Receives validation expectations from its caller.
- Returns structured validation results.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from io import BytesIO

import pyarrow.parquet as pq


@dataclass(frozen=True)
class BronzeValidationResult:
    """Represents the outcome of Bronze data validation."""

    is_valid: bool
    expected_row_count: int
    actual_row_count: int
    expected_columns: tuple[str, ...]
    actual_columns: tuple[str, ...]
    errors: tuple[str, ...]


class BronzeValidationService:
    """Validates Parquet files stored in the Bronze layer."""

    def validate(
        self,
        content: bytes,
        expected_columns: Sequence[str],
        expected_row_count: int,
        non_nullable_columns: Sequence[str] = (),
        primary_key_columns: Sequence[str] = (),
    ) -> BronzeValidationResult:
        """Validate Parquet structure and basic data quality."""

        if not content:
            raise ValueError("Cannot validate empty Parquet content.")

        if expected_row_count < 0:
            raise ValueError("Expected row count cannot be negative.")

        if not expected_columns:
            raise ValueError("Expected columns must be provided.")

        try:
            arrow_table = pq.read_table(BytesIO(content))
        except Exception as exc:
            raise ValueError(
                "Bronze validation failed: content is not a readable Parquet file."
            ) from exc

        actual_columns = tuple(arrow_table.schema.names)
        actual_row_count = arrow_table.num_rows
        expected_columns_tuple = tuple(expected_columns)

        errors: list[str] = []

        # 1. Exact schema validation
        missing_columns = [
            column for column in expected_columns_tuple if column not in actual_columns
        ]

        unexpected_columns = [
            column for column in actual_columns if column not in expected_columns_tuple
        ]

        if missing_columns:
            errors.append(f"Missing expected columns: {', '.join(missing_columns)}")

        if unexpected_columns:
            errors.append(f"Unexpected columns found: {', '.join(unexpected_columns)}")

        # 2. Record count validation
        if actual_row_count != expected_row_count:
            errors.append(
                f"Record count mismatch: expected {expected_row_count}, "
                f"received {actual_row_count}."
            )

        # 3. Non-nullable column validation
        for column in non_nullable_columns:
            if column not in actual_columns:
                continue

            null_count = arrow_table.column(column).null_count

            if null_count:
                errors.append(
                    f"Column '{column}' contains {null_count} null values "
                    "but is defined as non-nullable."
                )

        # 4. Primary key validation within this batch
        missing_primary_keys = [
            column for column in primary_key_columns if column not in actual_columns
        ]

        if not missing_primary_keys and primary_key_columns:
            key_values = arrow_table.select(list(primary_key_columns)).to_pylist()

            seen_keys: set[tuple] = set()
            duplicate_count = 0

            for record in key_values:
                key = tuple(record[column] for column in primary_key_columns)

                if any(value is None for value in key):
                    continue

                if key in seen_keys:
                    duplicate_count += 1
                else:
                    seen_keys.add(key)

            if duplicate_count:
                errors.append(
                    f"Found {duplicate_count} duplicate primary key occurrences within the batch."
                )

        return BronzeValidationResult(
            is_valid=not errors,
            expected_row_count=expected_row_count,
            actual_row_count=actual_row_count,
            expected_columns=expected_columns_tuple,
            actual_columns=actual_columns,
            errors=tuple(errors),
        )
