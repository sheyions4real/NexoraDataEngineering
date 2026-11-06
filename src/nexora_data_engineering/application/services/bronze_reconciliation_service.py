"""
MODULE: Bronze Reconciliation Service

ARCHITECTURAL LAYER:
Application Layer / Services

PURPOSE:
Reconciles source, extracted, validated and uploaded
record metrics for a Bronze ingestion run.

RESPONSIBILITIES:
- Compare source and extracted record counts.
- Compare extracted and validated record counts.
- Verify expected batch and file counts.
- Verify uploaded and downloaded file counts.
- Compare uploaded and downloaded byte totals.
- Support successful reconciliation of empty tables.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeReconciliationResult:
    """Represents the outcome of a Bronze ingestion reconciliation."""

    is_reconciled: bool
    source_row_count: int
    extracted_row_count: int
    validated_row_count: int
    expected_batch_count: int
    actual_batch_count: int
    uploaded_file_count: int
    verified_file_count: int
    uploaded_bytes: int
    verified_bytes: int
    errors: tuple[str, ...]


class BronzeReconciliationService:
    """Reconciles the metrics collected during a Bronze ingestion run."""

    def reconcile(
        self,
        source_row_count: int,
        extracted_row_count: int,
        validated_row_count: int,
        batch_size: int,
        actual_batch_count: int,
        uploaded_file_count: int,
        verified_file_count: int,
        uploaded_bytes: int,
        verified_bytes: int,
    ) -> BronzeReconciliationResult:
        """Compare ingestion metrics and return reconciliation results."""

        metrics = {
            "source_row_count": source_row_count,
            "extracted_row_count": extracted_row_count,
            "validated_row_count": validated_row_count,
            "actual_batch_count": actual_batch_count,
            "uploaded_file_count": uploaded_file_count,
            "verified_file_count": verified_file_count,
            "uploaded_bytes": uploaded_bytes,
            "verified_bytes": verified_bytes,
        }

        if batch_size <= 0:
            raise ValueError("Batch size must be greater than zero.")

        for name, value in metrics.items():
            if value < 0:
                raise ValueError(f"{name} cannot be negative.")

        expected_batch_count = (source_row_count + batch_size - 1) // batch_size

        errors: list[str] = []

        if source_row_count != extracted_row_count:
            errors.append(
                f"Source/extracted row mismatch: "
                f"source={source_row_count}, extracted={extracted_row_count}."
            )

        if extracted_row_count != validated_row_count:
            errors.append(
                f"Extracted/validated row mismatch: "
                f"extracted={extracted_row_count}, "
                f"validated={validated_row_count}."
            )

        if expected_batch_count != actual_batch_count:
            errors.append(
                f"Batch count mismatch: expected={expected_batch_count}, "
                f"actual={actual_batch_count}."
            )

        if actual_batch_count != uploaded_file_count:
            errors.append(
                f"Uploaded file count mismatch: "
                f"batches={actual_batch_count}, "
                f"uploaded files={uploaded_file_count}."
            )

        if uploaded_file_count != verified_file_count:
            errors.append(
                f"Verified file count mismatch: "
                f"uploaded={uploaded_file_count}, "
                f"verified={verified_file_count}."
            )

        if uploaded_bytes != verified_bytes:
            errors.append(
                f"Byte count mismatch: uploaded={uploaded_bytes}, verified={verified_bytes}."
            )

        return BronzeReconciliationResult(
            is_reconciled=not errors,
            source_row_count=source_row_count,
            extracted_row_count=extracted_row_count,
            validated_row_count=validated_row_count,
            expected_batch_count=expected_batch_count,
            actual_batch_count=actual_batch_count,
            uploaded_file_count=uploaded_file_count,
            verified_file_count=verified_file_count,
            uploaded_bytes=uploaded_bytes,
            verified_bytes=verified_bytes,
            errors=tuple(errors),
        )
