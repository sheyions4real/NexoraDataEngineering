"""
MODULE: Silver Referential Integrity Validation Service

ARCHITECTURAL LAYER:
Application Layer / Service

PURPOSE:
Validates foreign-key relationships between datasets.

RESPONSIBILITIES:
- Verify source and referenced columns exist.
- Compare source foreign-key values against referenced keys.
- Identify orphaned references.
- Support single-column and composite foreign keys.
- Return structured validation results.

DATA ENGINEERING ROLE:
Ensures relationships between datasets remain valid
before data is promoted through the Silver layer.
"""

from typing import Any

import pyarrow as pa

from nexora_data_engineering.domain.models.silver_referential_integrity_result import (
    SilverReferentialIntegrityIssue,
    SilverReferentialIntegrityResult,
)
from nexora_data_engineering.domain.models.table_metadata import (
    ForeignKeyMetadata,
)


class SilverReferentialIntegrityValidationService:
    """Validates foreign-key relationships between datasets."""

    def validate(
        self,
        source_dataset: pa.Table,
        referenced_dataset: pa.Table,
        foreign_key: ForeignKeyMetadata,
    ) -> SilverReferentialIntegrityResult:
        """
        Validate source foreign-key values against referenced keys.

        Rows containing NULL in any foreign-key component are
        excluded, following SQL Server foreign-key semantics.
        """

        source_columns = foreign_key.columns
        referenced_columns = foreign_key.referred_columns

        if not source_columns:
            raise ValueError("Foreign-key source columns cannot be empty.")

        if len(source_columns) != len(referenced_columns):
            raise ValueError("Source and referenced foreign-key column counts must match.")

        self._validate_columns_exist(
            dataset=source_dataset,
            columns=source_columns,
            dataset_name="Source",
        )

        self._validate_columns_exist(
            dataset=referenced_dataset,
            columns=referenced_columns,
            dataset_name="Referenced",
        )

        source_keys = self._get_column_values(
            dataset=source_dataset,
            columns=source_columns,
        )

        referenced_keys = self._get_column_values(
            dataset=referenced_dataset,
            columns=referenced_columns,
        )

        valid_referenced_keys = {
            key
            for key in zip(*referenced_keys, strict=True)
            if not any(value is None for value in key)
        }

        orphaned_rows = sum(
            1
            for key in zip(*source_keys, strict=True)
            if not any(value is None for value in key) and key not in valid_referenced_keys
        )

        issues: tuple[SilverReferentialIntegrityIssue, ...] = ()

        if orphaned_rows:
            issue = SilverReferentialIntegrityIssue(
                source_columns=source_columns,
                referred_schema=foreign_key.referred_schema,
                referred_table=foreign_key.referred_table,
                referred_columns=referenced_columns,
                affected_rows=orphaned_rows,
                message=(
                    f"{orphaned_rows} source row(s) contain foreign-key "
                    f"references that do not exist in "
                    f"{foreign_key.referred_schema or 'the referenced schema'}."
                    f"{foreign_key.referred_table}."
                ),
            )

            issues = (issue,)

        return SilverReferentialIntegrityResult(
            total_rows=source_dataset.num_rows,
            passed=not issues,
            issues=issues,
        )

    @staticmethod
    def _validate_columns_exist(
        dataset: pa.Table,
        columns: tuple[str, ...],
        dataset_name: str,
    ) -> None:
        """Ensure all requested columns exist in a dataset."""

        missing_columns = [column for column in columns if column not in dataset.column_names]

        if missing_columns:
            raise ValueError(
                f"{dataset_name} dataset is missing columns: {', '.join(missing_columns)}."
            )

    @staticmethod
    def _get_column_values(
        dataset: pa.Table,
        columns: tuple[str, ...],
    ) -> list[list[Any]]:
        """Extract Python values from the requested Arrow columns."""

        return [dataset.column(column).to_pylist() for column in columns]
