"""Unit tests for Bronze ingestion reconciliation."""

import pytest

from nexora_data_engineering.application.services.bronze_reconciliation_service import (
    BronzeReconciliationService,
)


@pytest.fixture
def reconciler() -> BronzeReconciliationService:
    return BronzeReconciliationService()


def test_reconcile_accepts_matching_ingestion_metrics(reconciler) -> None:
    result = reconciler.reconcile(
        source_row_count=102,
        extracted_row_count=102,
        validated_row_count=102,
        batch_size=10_000,
        actual_batch_count=1,
        uploaded_file_count=1,
        verified_file_count=1,
        uploaded_bytes=27_264,
        verified_bytes=27_264,
    )

    assert result.is_reconciled is True
    assert result.expected_batch_count == 1
    assert result.errors == ()


def test_reconcile_detects_row_count_mismatch(reconciler) -> None:
    result = reconciler.reconcile(
        source_row_count=102,
        extracted_row_count=100,
        validated_row_count=100,
        batch_size=10_000,
        actual_batch_count=1,
        uploaded_file_count=1,
        verified_file_count=1,
        uploaded_bytes=27_000,
        verified_bytes=27_000,
    )

    assert result.is_reconciled is False
    assert any("Source/extracted row mismatch" in error for error in result.errors)


def test_reconcile_accepts_empty_source_table(reconciler) -> None:
    result = reconciler.reconcile(
        source_row_count=0,
        extracted_row_count=0,
        validated_row_count=0,
        batch_size=10_000,
        actual_batch_count=0,
        uploaded_file_count=0,
        verified_file_count=0,
        uploaded_bytes=0,
        verified_bytes=0,
    )

    assert result.is_reconciled is True
    assert result.expected_batch_count == 0
    assert result.errors == ()


def test_reconcile_detects_missing_verified_file(reconciler) -> None:
    result = reconciler.reconcile(
        source_row_count=102,
        extracted_row_count=102,
        validated_row_count=102,
        batch_size=10_000,
        actual_batch_count=1,
        uploaded_file_count=1,
        verified_file_count=0,
        uploaded_bytes=27_264,
        verified_bytes=0,
    )

    assert result.is_reconciled is False
    assert any("Verified file count mismatch" in error for error in result.errors)
    assert any("Byte count mismatch" in error for error in result.errors)


def test_reconcile_rejects_invalid_batch_size(reconciler) -> None:
    with pytest.raises(ValueError, match="Batch size"):
        reconciler.reconcile(
            source_row_count=10,
            extracted_row_count=10,
            validated_row_count=10,
            batch_size=0,
            actual_batch_count=1,
            uploaded_file_count=1,
            verified_file_count=1,
            uploaded_bytes=100,
            verified_bytes=100,
        )
