"""
MODULE: Silver Source Profiling Service

ARCHITECTURAL LAYER:
Application / Services

PURPOSE:
Profiles Bronze datasets before Silver transformation.

RESPONSIBILITIES:
- Calculate dataset dimensions.
- Calculate null and distinct counts.
- Identify minimum and maximum values.
- Produce a structured Silver dataset profile.
"""

from typing import Any

import pyarrow as pa

from nexora_data_engineering.domain.models.silver_dataset_profile import (
    ColumnProfile,
    SilverDatasetProfile,
)


class SilverSourceProfilingService:
    """Profiles the structure and contents of an Arrow dataset."""

    def profile(self, dataset: pa.Table) -> SilverDatasetProfile:
        """Generate profiling statistics for every dataset column."""

        column_profiles: list[ColumnProfile] = []

        for column_name in dataset.column_names:
            column = dataset.column(column_name).combine_chunks()

            values = column.to_pylist()

            valid_values = [value for value in values if value is not None]

            null_count = len(values) - len(valid_values)
            distinct_count = len(set(valid_values))

            minimum, maximum = self._get_extreme_values(valid_values)

            column_profiles.append(
                ColumnProfile(
                    name=column_name,
                    data_type=str(column.type),
                    total_count=len(values),
                    null_count=null_count,
                    distinct_count=distinct_count,
                    minimum=minimum,
                    maximum=maximum,
                )
            )

        return SilverDatasetProfile(
            total_rows=dataset.num_rows,
            total_columns=dataset.num_columns,
            columns=tuple(column_profiles),
        )

    @staticmethod
    def _get_extreme_values(
        values: list[Any],
    ) -> tuple[str | None, str | None]:
        """Return the minimum and maximum values where comparable."""

        if not values:
            return None, None

        try:
            minimum = str(min(values))
            maximum = str(max(values))

            return minimum, maximum

        except (TypeError, ValueError):
            return None, None
