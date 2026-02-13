"""Measure-to-cache and measure-to-dataframe mapping helpers."""

from __future__ import annotations

from copy import deepcopy

from .schema import DEFAULT_SCHEMA

DEFAULT_MEASURE_TO_CACHE_VAR = {
    'noon_temperature': DEFAULT_SCHEMA.primary_variable,
    'daily_precipitation': 'daily_precip_mm',
}


DEFAULT_MEASURE_TO_VALUE_COLUMN = {
    'noon_temperature': 'temp_C',
    'daily_precipitation': 'precip_mm',
}


def _load_measure_to_cache_var_mapping() -> dict[str, str]:
    """Load logical-measure to cache-variable mapping from schema with validation."""
    configured = DEFAULT_SCHEMA.current.get('measure_cache_vars')
    if configured is None:
        return DEFAULT_MEASURE_TO_CACHE_VAR

    if not (
        isinstance(configured, dict)
        and configured
        and all(isinstance(key, str) and key for key in configured.keys())
        and all(isinstance(value, str) and value for value in configured.values())
    ):
        raise ValueError(
            "Current schema field 'measure_cache_vars' must be a non-empty mapping of strings"
        )

    required_measures = set(DEFAULT_MEASURE_TO_CACHE_VAR.keys())
    missing = sorted(required_measures - set(configured.keys()))
    if missing:
        raise ValueError(
            "Current schema field 'measure_cache_vars' is missing required measures: "
            + ", ".join(missing)
        )

    if configured.get('noon_temperature') != DEFAULT_SCHEMA.primary_variable:
        raise ValueError(
            "Current schema field 'measure_cache_vars.noon_temperature' must match "
            "'primary_variable'"
        )

    return configured


MEASURE_TO_CACHE_VAR = _load_measure_to_cache_var_mapping()


def _load_measure_to_value_column_mapping() -> dict[str, str]:
    """Load logical-measure to DataFrame value-column mapping from schema with validation."""
    configured = DEFAULT_SCHEMA.current.get('measure_value_columns')
    if configured is None:
        return DEFAULT_MEASURE_TO_VALUE_COLUMN

    if not (
        isinstance(configured, dict)
        and configured
        and all(isinstance(key, str) and key for key in configured.keys())
        and all(isinstance(value, str) and value for value in configured.values())
    ):
        raise ValueError(
            "Current schema field 'measure_value_columns' must be a non-empty mapping of strings"
        )

    required_measures = set(DEFAULT_MEASURE_TO_VALUE_COLUMN.keys())
    missing = sorted(required_measures - set(configured.keys()))
    if missing:
        raise ValueError(
            "Current schema field 'measure_value_columns' is missing required measures: "
            + ", ".join(missing)
        )

    return configured


MEASURE_TO_VALUE_COLUMN = _load_measure_to_value_column_mapping()


def _get_measure_cache_var(measure: str) -> str:
    """Resolve cache variable key for a logical measure name."""
    try:
        return MEASURE_TO_CACHE_VAR[measure]
    except KeyError as exc:
        allowed = ', '.join(sorted(MEASURE_TO_CACHE_VAR.keys()))
        raise ValueError(f"Unsupported measure '{measure}'. Allowed: {allowed}") from exc


def _get_measure_value_column(measure: str) -> str:
    """Resolve DataFrame value column for a logical measure name."""
    try:
        return MEASURE_TO_VALUE_COLUMN[measure]
    except KeyError as exc:
        allowed = ', '.join(sorted(MEASURE_TO_VALUE_COLUMN.keys()))
        raise ValueError(f"Unsupported measure '{measure}'. Allowed: {allowed}") from exc


def _build_variables_metadata(measure: str = 'noon_temperature') -> dict:
    """Build variable metadata for v2 cache schema with selected measure included."""
    variables_template = DEFAULT_SCHEMA.variables_template
    metadata = deepcopy(variables_template) if variables_template else {}
    cache_var = _get_measure_cache_var(measure)

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
    }

    if cache_var not in metadata:
        metadata[cache_var] = defaults[measure]

    return metadata
