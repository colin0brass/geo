"""Schema registry, measure metadata, and migration helpers for cache data."""

from __future__ import annotations

import logging
from copy import deepcopy
from pathlib import Path

import yaml

logger = logging.getLogger("geo")

SCHEMA_REGISTRY_FILE = Path(__file__).resolve().parent / 'schema.yaml'


def _load_cache_schema_registry(schema_file: Path = SCHEMA_REGISTRY_FILE) -> dict:
    """Load cache schema registry from YAML file."""
    with open(schema_file, 'r') as f:
        raw = yaml.safe_load(f) or {}

    current_version = raw.get('current_version')
    versions = raw.get('versions', {})
    if current_version is None or not isinstance(versions, dict) or not versions:
        raise ValueError(f"Invalid cache schema registry: {schema_file}")

    normalized_versions = {str(k): v for k, v in versions.items()}
    current_key = str(current_version)
    if current_key not in normalized_versions:
        raise ValueError(
            f"Cache schema version {current_version} not found in registry: {schema_file}"
        )

    for version_key, schema_def in normalized_versions.items():
        if not isinstance(schema_def, dict):
            raise ValueError(f"Schema version {version_key} definition must be a mapping")

        for list_field in ('required', 'required_paths'):
            field_value = schema_def.get(list_field)
            if field_value is not None and not (
                isinstance(field_value, list)
                and all(isinstance(item, str) and item for item in field_value)
            ):
                raise ValueError(
                    f"Schema version {version_key} field '{list_field}' must be a non-empty string list"
                )

        any_of_groups = schema_def.get('required_any_of')
        if any_of_groups is None:
            any_of_groups = schema_def.get('required_any_of_paths')
        if any_of_groups is not None:
            if not isinstance(any_of_groups, list):
                raise ValueError(
                    f"Schema version {version_key} field 'required_any_of' must be a list of path groups"
                )
            for group in any_of_groups:
                if not (
                    isinstance(group, list)
                    and group
                    and all(isinstance(item, str) and item for item in group)
                ):
                    raise ValueError(
                        f"Schema version {version_key} field 'required_any_of' must contain non-empty string lists"
                    )

        if version_key == current_key:
            for required_field in ('data_key', 'variables_key', 'primary_variable'):
                required_value = schema_def.get(required_field)
                if not isinstance(required_value, str) or not required_value:
                    raise ValueError(
                        f"Current schema version {version_key} must define '{required_field}'"
                    )

    return {
        'current_version': int(current_version),
        'versions': normalized_versions,
    }


_SCHEMA_REGISTRY = _load_cache_schema_registry()
SCHEMA_VERSION = _SCHEMA_REGISTRY['current_version']
CURRENT_SCHEMA = _SCHEMA_REGISTRY['versions'][str(SCHEMA_VERSION)]

DATA_KEY = CURRENT_SCHEMA['data_key']
VARIABLES_KEY = CURRENT_SCHEMA['variables_key']
NOON_TEMP_VAR = CURRENT_SCHEMA['primary_variable']
VARIABLES_METADATA_TEMPLATE = CURRENT_SCHEMA.get('variables', {})

MEASURE_TO_CACHE_VAR = {
    'noon_temperature': NOON_TEMP_VAR,
    'daily_precipitation': 'daily_precip_mm',
}

MEASURE_TO_VALUE_COLUMN = {
    'noon_temperature': 'temp_C',
    'daily_precipitation': 'precip_mm',
}


class CacheSchemaRegistry:
    """Schema and migration façade over module-level schema utilities."""

    @property
    def schema_version(self) -> int:
        return SCHEMA_VERSION

    @property
    def data_key(self) -> str:
        return DATA_KEY

    @property
    def variables_key(self) -> str:
        return VARIABLES_KEY

    @property
    def primary_variable(self) -> str:
        return NOON_TEMP_VAR

    def get_measure_cache_var(self, measure: str) -> str:
        return _get_measure_cache_var(measure)

    def get_measure_value_column(self, measure: str) -> str:
        return _get_measure_value_column(measure)

    def load_registry(self, schema_file: Path = SCHEMA_REGISTRY_FILE) -> dict:
        return _load_cache_schema_registry(schema_file)

    def is_v2_schema(self, data: dict) -> bool:
        return _is_v2_schema(data)

    def detect_schema_version(self, data: dict) -> int | None:
        return _detect_schema_version(data)

    def build_variables_metadata(self, measure: str = 'noon_temperature') -> dict:
        return _build_variables_metadata(measure)

    def load_cache_data(self, yaml_file: Path, auto_migrate: bool = True) -> dict:
        return _load_cache_data_v2(yaml_file, auto_migrate)

    def migrate_cache_file(self, yaml_file: Path) -> bool:
        return migrate_cache_file_to_v2(yaml_file)


DEFAULT_SCHEMA_REGISTRY = CacheSchemaRegistry()


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


def _schema_legacy_data_paths(schema_def: dict) -> list[str]:
    """Collect candidate legacy root-level data paths from a schema definition."""
    candidates: list[str] = []

    primary = schema_def.get('primary_data_path')
    if primary is None:
        primary = schema_def.get('temperature_key')
    if isinstance(primary, str) and primary and primary not in candidates:
        candidates.append(primary)

    for list_field in ('legacy_data_paths', 'legacy_temperature_keys'):
        for key in schema_def.get(list_field, []):
            if isinstance(key, str) and key and key not in candidates:
                candidates.append(key)

    return candidates


def _get_by_path(data: dict, path: str):
    """Get a nested value from dict using dot-separated path."""
    node = data
    for part in path.split('.'):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _has_path(data: dict, path: str) -> bool:
    """Check whether a dot-separated key path exists in a mapping."""
    node = data
    for part in path.split('.'):
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


def _extract_temp_map_from_schema_mapping(data: dict) -> dict:
    """Extract temperature map using versioned migration field mapping if present."""
    schema_version = data.get('schema_version')
    if schema_version is None:
        return {}

    target_path = f"{DATA_KEY}.{NOON_TEMP_VAR}"

    mapping = None

    migration = CURRENT_SCHEMA.get('migration', {})
    if isinstance(migration, dict):
        from_version = migration.get('from_version')
        if from_version is not None and str(from_version) == str(schema_version):
            field_mappings = migration.get('field_mappings', {})
            if isinstance(field_mappings, dict):
                mapping = field_mappings.get(target_path)

    if mapping is None:
        current_migrations = CURRENT_SCHEMA.get('migration_from_previous', {})
        if isinstance(current_migrations, dict):
            prev_mapping = current_migrations.get(str(schema_version))
            if prev_mapping is None:
                prev_mapping = current_migrations.get(schema_version)
            if prev_mapping is None:
                try:
                    prev_mapping = current_migrations.get(int(schema_version))
                except (TypeError, ValueError):
                    prev_mapping = None
            if isinstance(prev_mapping, dict):
                field_mappings = prev_mapping.get('field_mappings', {})
                if isinstance(field_mappings, dict):
                    mapping = field_mappings.get(target_path)

    if mapping is None:
        schema_def = _SCHEMA_REGISTRY['versions'].get(str(schema_version), {})
        migration = schema_def.get('migration_to_next', {})
        if isinstance(migration, dict):
            field_mappings = migration.get('field_mappings', {})
            if isinstance(field_mappings, dict):
                mapping = field_mappings.get(target_path)

    if isinstance(mapping, str):
        value = _get_by_path(data, mapping)
        return value if isinstance(value, dict) else {}

    if isinstance(mapping, dict):
        source_path = mapping.get('source_path')
        if isinstance(source_path, str):
            value = _get_by_path(data, source_path)
            if isinstance(value, dict):
                return value

        for candidate in mapping.get('source_candidates', []):
            if not isinstance(candidate, str):
                continue
            value = _get_by_path(data, candidate)
            if isinstance(value, dict):
                return value

    return {}


def _build_variables_metadata(measure: str = 'noon_temperature') -> dict:
    """Build variable metadata for v2 cache schema with selected measure included."""
    metadata = deepcopy(VARIABLES_METADATA_TEMPLATE) if VARIABLES_METADATA_TEMPLATE else {}
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


def _extract_legacy_noon_temps(data: dict) -> dict:
    """Extract legacy temperature map if present."""
    mapped = _extract_temp_map_from_schema_mapping(data)
    if mapped:
        return mapped

    schema_version = data.get('schema_version')
    if schema_version is None:
        return {}

    schema_def = _SCHEMA_REGISTRY['versions'].get(str(schema_version), {})
    for legacy_key in _schema_legacy_data_paths(schema_def):
        if legacy_key in data:
            return data[legacy_key]
    return {}


def _validate_required_schema_fields(data: dict, schema_def: dict, yaml_file: Path) -> None:
    """Validate required key-path contracts declared in schema metadata."""
    required_paths = schema_def.get('required')
    if required_paths is None:
        required_paths = schema_def.get('required_paths', [])

    for path in required_paths:
        if not _has_path(data, path):
            raise ValueError(f"Cannot migrate {yaml_file}: missing required path '{path}'")

    required_any_of = schema_def.get('required_any_of')
    if required_any_of is None:
        required_any_of = schema_def.get('required_any_of_paths', [])

    for group in required_any_of:
        if not any(_has_path(data, candidate) for candidate in group):
            group_str = ', '.join(group)
            raise ValueError(
                f"Cannot migrate {yaml_file}: missing required key path group; expected one of [{group_str}]"
            )


def _is_v2_schema(data: dict) -> bool:
    """Check whether a loaded cache document matches v2 schema."""
    return (
        isinstance(data, dict)
        and data.get('schema_version') == SCHEMA_VERSION
        and isinstance(data.get(VARIABLES_KEY), dict)
        and isinstance(data.get(DATA_KEY), dict)
    )


def _detect_schema_version(data: dict) -> int | None:
    """Return schema version from document, or None if unversioned legacy."""
    raw_version = data.get('schema_version')
    if raw_version is None:
        return None

    try:
        return int(raw_version)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid schema_version value: {raw_version!r}") from exc


def _normalize_temp_map(temp_map: dict) -> dict:
    """Normalize year/month/day keys to integers."""
    normalized = {}
    for year_key, months in temp_map.items():
        year_int = int(year_key)
        if year_int not in normalized:
            normalized[year_int] = {}
        for month_key, days in months.items():
            month_int = int(month_key)
            if month_int not in normalized[year_int]:
                normalized[year_int][month_int] = {}
            for day_key, temp in days.items():
                day_int = int(day_key)
                normalized[year_int][month_int][day_int] = float(temp)
    return normalized


def _write_cache_yaml_v2(cache_data: dict, out_file: Path) -> None:
    """Write cache document in v2 compact YAML format."""
    with open(out_file, 'w') as f:
        f.write(f"schema_version: {SCHEMA_VERSION}\n")
        f.write("place:\n")
        f.write(f"  name: {cache_data['place']['name']}\n")
        f.write(f"  lat: {cache_data['place']['lat']}\n")
        f.write(f"  lon: {cache_data['place']['lon']}\n")
        f.write(f"  timezone: {cache_data['place']['timezone']}\n")
        f.write(f"  grid_lat: {cache_data['place']['grid_lat']}\n")
        f.write(f"  grid_lon: {cache_data['place']['grid_lon']}\n")

        f.write(f"{VARIABLES_KEY}:\n")
        for variable_name, variable_meta in cache_data.get(VARIABLES_KEY, {}).items():
            f.write(f"  {variable_name}:\n")
            for key in ('units', 'source_variable', 'source_dataset', 'temporal_definition', 'precision'):
                if key in variable_meta:
                    f.write(f"    {key}: {variable_meta[key]}\n")

        f.write(f"{DATA_KEY}:\n")
        for variable_name, value_map in cache_data.get(DATA_KEY, {}).items():
            f.write(f"  {variable_name}:\n")
            for year in sorted(value_map.keys()):
                f.write(f"    {year}:\n")
                for month in sorted(value_map[year].keys()):
                    days_dict = value_map[year][month]
                    days_str = '{' + ', '.join(f'{day}: {value}' for day, value in sorted(days_dict.items())) + '}'
                    f.write(f"      {month}: {days_str}\n")


def migrate_cache_file_to_v2(yaml_file: Path) -> bool:
    """
    Migrate a legacy cache file to schema v2 in-place.

    Returns:
        bool: True if migration occurred, False if already v2.
    """
    with open(yaml_file, 'r') as f:
        data = yaml.safe_load(f) or {}

    if _is_v2_schema(data):
        return False

    schema_version = _detect_schema_version(data)
    if schema_version is None:
        raise ValueError(
            f"Cannot migrate {yaml_file}: unversioned cache documents are no longer supported"
        )
    if schema_version is not None and schema_version > SCHEMA_VERSION:
        raise ValueError(
            f"Cannot migrate {yaml_file}: schema_version {schema_version} is newer than supported {SCHEMA_VERSION}"
        )

    schema_def = _SCHEMA_REGISTRY['versions'].get(str(schema_version))
    if not isinstance(schema_def, dict):
        raise ValueError(f"Cannot migrate {yaml_file}: unsupported schema_version {schema_version}")

    _validate_required_schema_fields(data, schema_def, yaml_file)

    legacy_temps = _extract_legacy_noon_temps(data)
    if not legacy_temps or 'place' not in data:
        raise ValueError(f"Cannot migrate {yaml_file}: missing legacy temperature data or place metadata")

    normalized = _normalize_temp_map(legacy_temps)
    migrated = {
        'schema_version': SCHEMA_VERSION,
        'place': data['place'],
        VARIABLES_KEY: _build_variables_metadata(),
        DATA_KEY: {
            NOON_TEMP_VAR: normalized,
        },
    }
    _write_cache_yaml_v2(migrated, yaml_file)
    logger.info(f"Migrated cache file to schema v2: {yaml_file}")
    return True


def _load_cache_data_v2(yaml_file: Path, auto_migrate: bool = True) -> dict:
    """Load cache file and ensure it is schema v2 (optionally auto-migrating)."""
    with open(yaml_file, 'r') as f:
        data = yaml.safe_load(f) or {}

    if _is_v2_schema(data):
        return data

    schema_version = _detect_schema_version(data)
    if schema_version is None:
        raise ValueError(
            f"Cannot migrate {yaml_file}: unversioned cache documents are no longer supported"
        )
    if schema_version is not None and schema_version > SCHEMA_VERSION:
        raise ValueError(
            f"Cache file '{yaml_file}' uses newer schema_version {schema_version}; "
            f"max supported is {SCHEMA_VERSION}."
        )

    should_migrate = auto_migrate and (schema_version is not None and schema_version < SCHEMA_VERSION)
    if should_migrate:
        migrated = migrate_cache_file_to_v2(yaml_file)
        if migrated:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f) or {}
            if _is_v2_schema(data):
                return data

    raise ValueError(
        f"Cache file '{yaml_file}' is not schema v2. "
        "Run migration before reading this file."
    )
