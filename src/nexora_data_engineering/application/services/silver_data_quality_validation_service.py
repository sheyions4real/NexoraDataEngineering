"""
MODULE: Silver Data Quality Validation Service

ARCHITECTURAL LAYER:
Application / Services

PURPOSE:
Validates datasets before they enter the Silver layer.

RESPONSIBILITIES:
- Validate required columns.
- Detect null values in non-nullable columns.
- Detect duplicate primary or unique keys.
- Detect negative values in designated numeric columns.
- Return a structured validation result.
"""

from decimal import Decimal

import pyarrow as pa

from nexora_data_engineering.domain.models.silver_data_quality_result import (
    SilverDataQualityIssue,
    SilverDataQualityResult,
)


class SilverDataQualityValidationService:
    """Validates structural and basic business data quality rules."""

    def validate(
        self,
        dataset: pa.Table,
        required_columns: tuple[str, ...],
        non_nullable_columns: tuple[str, ...],
        unique_columns: tuple[str, ...],
        non_negative_columns: tuple[str, ...],
    ) -> SilverDataQualityResult:
        """Validate a dataset against the supplied quality rules."""

        issues: list[SilverDataQualityIssue] = []

        available_columns = set(dataset.column_names)

        # ---------------------------------------------------------
        # 1. Required column validation
        # ---------------------------------------------------------

        for column_name in required_columns:
            if column_name not in available_columns:
                issues.append(
                    SilverDataQualityIssue(
                        rule="REQUIRED_COLUMN",
                        column=column_name,
                        affected_rows=dataset.num_rows,
                        message=f"Required column '{column_name}' is missing.",
                    )
                )

        # ---------------------------------------------------------
        # 2. Non-nullable column validation
        # ---------------------------------------------------------

        for column_name in non_nullable_columns:
            if column_name not in available_columns:
                continue

            null_count = dataset.column(column_name).null_count

            if null_count > 0:
                issues.append(
                    SilverDataQualityIssue(
                        rule="NOT_NULL",
                        column=column_name,
                        affected_rows=null_count,
                        message=(f"Column '{column_name}' contains {null_count} null value(s)."),
                    )
                )

        # ---------------------------------------------------------
        # 3. Unique key validation
        # ---------------------------------------------------------

        missing_unique_columns = [
            column_name for column_name in unique_columns if column_name not in available_columns
        ]

        if not missing_unique_columns and unique_columns:
            key_values = list(
                zip(
                    *[dataset.column(column_name).to_pylist() for column_name in unique_columns],
                    strict=True,
                )
            )

            duplicate_count = len(key_values) - len(set(key_values))

            if duplicate_count > 0:
                key_name = ", ".join(unique_columns)

                issues.append(
                    SilverDataQualityIssue(
                        rule="UNIQUE_KEY",
                        column=key_name,
                        affected_rows=duplicate_count,
                        message=(
                            f"Unique key '{key_name}' contains {duplicate_count} duplicate row(s)."
                        ),
                    )
                )

        # ---------------------------------------------------------
        # 4. Non-negative numeric validation
        # ---------------------------------------------------------

        for column_name in non_negative_columns:
            if column_name not in available_columns:
                continue

            values = dataset.column(column_name).to_pylist()

            negative_count = sum(
                1 for value in values if isinstance(value, (int, float, Decimal)) and value < 0
            )

            if negative_count > 0:
                issues.append(
                    SilverDataQualityIssue(
                        rule="NON_NEGATIVE",
                        column=column_name,
                        affected_rows=negative_count,
                        message=(
                            f"Column '{column_name}' contains {negative_count} negative value(s)."
                        ),
                    )
                )

        return SilverDataQualityResult(
            total_rows=dataset.num_rows,
            passed=len(issues) == 0,
            issues=tuple(issues),
        )
