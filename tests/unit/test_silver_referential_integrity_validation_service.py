"""
MODULE: Silver Referential Integrity Validation Service Tests

PURPOSE:
Tests foreign-key relationship validation between datasets.
"""

import pyarrow as pa
import pytest

from nexora_data_engineering.application.services import (
    silver_referential_integrity_validation_service,
)
from nexora_data_engineering.domain.models.table_metadata import (
    ForeignKeyMetadata,
)

SilverReferentialIntegrityValidationService = (
    silver_referential_integrity_validation_service.SilverReferentialIntegrityValidationService
)


@pytest.fixture
def service() -> SilverReferentialIntegrityValidationService:
    return SilverReferentialIntegrityValidationService()


@pytest.fixture
def customer_foreign_key() -> ForeignKeyMetadata:
    return ForeignKeyMetadata(
        columns=("CustomerId",),
        referred_schema="Core",
        referred_table="Customers",
        referred_columns=("Id",),
    )


def test_valid_foreign_key_references_pass(
    service: SilverReferentialIntegrityValidationService,
    customer_foreign_key: ForeignKeyMetadata,
) -> None:
    source = pa.table({"CustomerId": ["C001", "C002", "C003"]})
    referenced = pa.table({"Id": ["C001", "C002", "C003"]})

    result = service.validate(
        source,
        referenced,
        customer_foreign_key,
    )

    assert result.passed is True
    assert result.total_rows == 3
    assert result.issues == ()


def test_orphaned_foreign_key_references_fail(
    service: SilverReferentialIntegrityValidationService,
    customer_foreign_key: ForeignKeyMetadata,
) -> None:
    source = pa.table({"CustomerId": ["C001", "C002", "C004"]})
    referenced = pa.table({"Id": ["C001", "C002", "C003"]})

    result = service.validate(
        source,
        referenced,
        customer_foreign_key,
    )

    assert result.passed is False
    assert result.total_rows == 3
    assert result.issues[0].affected_rows == 1


def test_null_foreign_keys_are_excluded(
    service: SilverReferentialIntegrityValidationService,
    customer_foreign_key: ForeignKeyMetadata,
) -> None:
    source = pa.table({"CustomerId": ["C001", None]})
    referenced = pa.table({"Id": ["C001"]})

    result = service.validate(
        source,
        referenced,
        customer_foreign_key,
    )

    assert result.passed is True
    assert result.issues == ()


def test_missing_source_column_raises_error(
    service: SilverReferentialIntegrityValidationService,
    customer_foreign_key: ForeignKeyMetadata,
) -> None:
    source = pa.table({"OrderId": ["O001"]})
    referenced = pa.table({"Id": ["C001"]})

    with pytest.raises(ValueError, match="Source dataset is missing"):
        service.validate(
            source,
            referenced,
            customer_foreign_key,
        )


def test_composite_foreign_key_validation(
    service: SilverReferentialIntegrityValidationService,
) -> None:
    foreign_key = ForeignKeyMetadata(
        columns=("CountryCode", "RegionCode"),
        referred_schema="Core",
        referred_table="Regions",
        referred_columns=("CountryCode", "RegionCode"),
    )

    source = pa.table(
        {
            "CountryCode": ["CA", "US"],
            "RegionCode": ["MB", "XX"],
        }
    )

    referenced = pa.table(
        {
            "CountryCode": ["CA", "US"],
            "RegionCode": ["MB", "NY"],
        }
    )

    result = service.validate(
        source,
        referenced,
        foreign_key,
    )

    assert result.passed is False
    assert result.issues[0].affected_rows == 1

    def test_duplicate_orphaned_references_count_each_affected_row(
        service: SilverReferentialIntegrityValidationService,
        customer_foreign_key: ForeignKeyMetadata,
    ) -> None:
        source = pa.table({"CustomerId": ["C001", "C004", "C004"]})
        referenced = pa.table({"Id": ["C001", "C002"]})

        result = service.validate(source, referenced, customer_foreign_key)

        assert result.passed is False
        assert result.issues[0].affected_rows == 2

    def test_missing_referenced_column_raises_error(
        service: SilverReferentialIntegrityValidationService,
        customer_foreign_key: ForeignKeyMetadata,
    ) -> None:
        source = pa.table({"CustomerId": ["C001"]})
        referenced = pa.table({"CustomerCode": ["C001"]})

        with pytest.raises(ValueError, match="Referenced dataset is missing"):
            service.validate(source, referenced, customer_foreign_key)

    def test_empty_foreign_key_columns_raise_error(
        service: SilverReferentialIntegrityValidationService,
    ) -> None:
        foreign_key = ForeignKeyMetadata(
            columns=(),
            referred_schema="Core",
            referred_table="Customers",
            referred_columns=(),
        )

        with pytest.raises(ValueError, match="cannot be empty"):
            service.validate(
                pa.table({"CustomerId": ["C001"]}),
                pa.table({"Id": ["C001"]}),
                foreign_key,
            )

    def test_mismatched_composite_foreign_key_columns_raise_error(
        service: SilverReferentialIntegrityValidationService,
    ) -> None:
        foreign_key = ForeignKeyMetadata(
            columns=("CountryCode", "RegionCode"),
            referred_schema="Core",
            referred_table="Regions",
            referred_columns=("Id",),
        )

        with pytest.raises(ValueError, match="column counts must match"):
            service.validate(
                pa.table(
                    {
                        "CountryCode": ["CA"],
                        "RegionCode": ["MB"],
                    }
                ),
                pa.table({"Id": ["R001"]}),
                foreign_key,
            )
