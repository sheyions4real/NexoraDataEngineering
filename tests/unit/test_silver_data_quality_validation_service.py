"""
MODULE: Silver Data Quality Validation Tests

PURPOSE:
Tests the Silver data quality validation service.
"""

from decimal import Decimal

import pyarrow as pa

from nexora_data_engineering.application.services.silver_data_quality_validation_service import (
    SilverDataQualityValidationService,
)


def test_valid_dataset_passes_validation() -> None:
    service = SilverDataQualityValidationService()

    dataset = pa.table(
        {
            "Id": ["A", "B", "C"],
            "Amount": [
                Decimal("100.00"),
                Decimal("200.00"),
                Decimal("300.00"),
            ],
        }
    )

    result = service.validate(
        dataset=dataset,
        required_columns=("Id", "Amount"),
        non_nullable_columns=("Id", "Amount"),
        unique_columns=("Id",),
        non_negative_columns=("Amount",),
    )

    assert result.passed is True
    assert result.total_rows == 3
    assert len(result.issues) == 0


def test_missing_required_column_fails_validation() -> None:
    service = SilverDataQualityValidationService()

    dataset = pa.table({"Id": ["A", "B"]})

    result = service.validate(
        dataset=dataset,
        required_columns=("Id", "Amount"),
        non_nullable_columns=(),
        unique_columns=(),
        non_negative_columns=(),
    )

    assert result.passed is False
    assert result.issues[0].rule == "REQUIRED_COLUMN"
    assert result.issues[0].column == "Amount"


def test_null_values_fail_non_nullable_validation() -> None:
    service = SilverDataQualityValidationService()

    dataset = pa.table({"Id": ["A", None, "C"]})

    result = service.validate(
        dataset=dataset,
        required_columns=("Id",),
        non_nullable_columns=("Id",),
        unique_columns=(),
        non_negative_columns=(),
    )

    assert result.passed is False
    assert result.issues[0].rule == "NOT_NULL"
    assert result.issues[0].affected_rows == 1


def test_duplicate_keys_fail_unique_validation() -> None:
    service = SilverDataQualityValidationService()

    dataset = pa.table({"Id": ["A", "B", "A", "C", "B"]})

    result = service.validate(
        dataset=dataset,
        required_columns=("Id",),
        non_nullable_columns=("Id",),
        unique_columns=("Id",),
        non_negative_columns=(),
    )

    assert result.passed is False

    duplicate_issue = next(issue for issue in result.issues if issue.rule == "UNIQUE_KEY")

    assert duplicate_issue.affected_rows == 2


def test_negative_amounts_fail_validation() -> None:
    service = SilverDataQualityValidationService()

    dataset = pa.table(
        {
            "TotalNetAmount": [
                Decimal("100.00"),
                Decimal("-25.00"),
                Decimal("200.00"),
                Decimal("-10.00"),
            ]
        }
    )

    result = service.validate(
        dataset=dataset,
        required_columns=("TotalNetAmount",),
        non_nullable_columns=(),
        unique_columns=(),
        non_negative_columns=("TotalNetAmount",),
    )

    assert result.passed is False
    assert result.issues[0].rule == "NON_NEGATIVE"
    assert result.issues[0].affected_rows == 2
