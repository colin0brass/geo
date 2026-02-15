"""Measure-to-cache and measure-to-dataframe mapping helpers."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from .schema import DEFAULT_SCHEMA

DEFAULT_MEASURE_TO_CACHE_VAR = {
    'noon_temperature': DEFAULT_SCHEMA.primary_variable,
    'daily_precipitation': 'daily_precip_mm',
    'hourly_precipitation': 'hourly_precip_mm',
    'daily_solar_radiation_energy': 'daily_solar_energy_MJ_m2',
}


DEFAULT_MEASURE_TO_VALUE_COLUMN = {
    'noon_temperature': 'temp_C',
    'daily_precipitation': 'precip_mm',
    'hourly_precipitation': 'precip_mm',
    'daily_solar_radiation_energy': 'solar_energy_MJ_m2',
}


@dataclass(frozen=True)
class MeasureRegistry:
    """Validated mapping registry for logical measures and cache/dataframe keys."""

    cache_var_by_measure: dict[str, str]
    value_column_by_measure: dict[str, str]

    @staticmethod
    def _validate_mapping(field_name: str, configured: object) -> None:
        if not (
            isinstance(configured, dict)
            and configured
            and all(isinstance(key, str) and key for key in configured.keys())
            and all(isinstance(value, str) and value for value in configured.values())
        ):
            raise ValueError(
                f"Current schema field '{field_name}' must be a non-empty mapping of strings"
            )

    @classmethod
    def _load_measure_to_cache_var_mapping(cls, schema=None) -> dict[str, str]:
        """Load logical-measure to cache-variable mapping from schema with validation."""
        schema_obj = DEFAULT_SCHEMA if schema is None else schema
        configured = schema_obj.current.get('measure_cache_vars')
        if configured is None:
            return DEFAULT_MEASURE_TO_CACHE_VAR

        cls._validate_mapping('measure_cache_vars', configured)

        required_measures = set(DEFAULT_MEASURE_TO_CACHE_VAR.keys())
        missing = sorted(required_measures - set(configured.keys()))
        if missing:
            raise ValueError(
                "Current schema field 'measure_cache_vars' is missing required measures: "
                + ", ".join(missing)
            )

        if configured.get('noon_temperature') != schema_obj.primary_variable:
            raise ValueError(
                "Current schema field 'measure_cache_vars.noon_temperature' must match "
                "'primary_variable'"
            )

        return configured

    @classmethod
    def _load_measure_to_value_column_mapping(cls, schema=None) -> dict[str, str]:
        """Load logical-measure to DataFrame value-column mapping from schema with validation."""
        schema_obj = DEFAULT_SCHEMA if schema is None else schema
        configured = schema_obj.current.get('measure_value_columns')
        if configured is None:
            return DEFAULT_MEASURE_TO_VALUE_COLUMN

        cls._validate_mapping('measure_value_columns', configured)

        required_measures = set(DEFAULT_MEASURE_TO_VALUE_COLUMN.keys())
        missing = sorted(required_measures - set(configured.keys()))
        if missing:
            raise ValueError(
                "Current schema field 'measure_value_columns' is missing required measures: "
                + ", ".join(missing)
            )

        return configured

    @classmethod
    def from_schema(cls, schema=None) -> "MeasureRegistry":
        """Construct a validated measure registry from schema metadata."""
        schema_obj = DEFAULT_SCHEMA if schema is None else schema
        return cls(
            cache_var_by_measure=cls._load_measure_to_cache_var_mapping(schema_obj),
            value_column_by_measure=cls._load_measure_to_value_column_mapping(schema_obj),
        )

    def get_cache_var(self, measure: str) -> str:
        """Resolve cache variable key for a logical measure name."""
        try:
            return self.cache_var_by_measure[measure]
        except KeyError as exc:
            allowed = ', '.join(sorted(self.cache_var_by_measure.keys()))
            raise ValueError(f"Unsupported measure '{measure}'. Allowed: {allowed}") from exc

    def get_value_column(self, measure: str) -> str:
        """Resolve DataFrame value column for a logical measure name."""
        try:
            return self.value_column_by_measure[measure]
        except KeyError as exc:
            allowed = ', '.join(sorted(self.value_column_by_measure.keys()))
            raise ValueError(f"Unsupported measure '{measure}'. Allowed: {allowed}") from exc

    def build_variables_metadata(self, measure: str = 'noon_temperature') -> dict:
        """Build variable metadata for v2 cache schema with selected measure included."""
        variables_template = DEFAULT_SCHEMA.variables_template
        metadata = deepcopy(variables_template) if variables_template else {}
        cache_var = self.get_cache_var(measure)

        defaults = {
            'noon_temperature': {
                'units': 'C',
                'source_variable': '2m_temperature',
                'source_dataset': 'reanalysis-era5-single-levels',
                'temporal_definition': 'daily_local_noon',
                'precision': 2,
            },
            'daily_precipitation': {
                'units': 'mm',
                'source_variable': 'total_precipitation',
                'source_dataset': 'reanalysis-era5-single-levels',
                'temporal_definition': 'daily_total_local',
                'precision': 2,
            },
            'hourly_precipitation': {
                'units': 'mm',
                'source_variable': 'total_precipitation',
                'source_dataset': 'reanalysis-era5-single-levels-timeseries',
                'temporal_definition': 'hourly_utc',
                'precision': 3,
            },
            'daily_solar_radiation_energy': {
                'units': 'MJ/m2',
                'source_variable': 'surface_solar_radiation_downwards',
                'source_dataset': 'reanalysis-era5-single-levels',
                'temporal_definition': 'daily_total_local',
                'precision': 2,
            },
        }

        if cache_var not in metadata:
            metadata[cache_var] = defaults[measure]

        return metadata


DEFAULT_MEASURE_REGISTRY = MeasureRegistry.from_schema()
MEASURE_TO_CACHE_VAR = DEFAULT_MEASURE_REGISTRY.cache_var_by_measure
MEASURE_TO_VALUE_COLUMN = DEFAULT_MEASURE_REGISTRY.value_column_by_measure
