"""YAML codec helpers for cache read/write and migration."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from .measure_mapping import _build_variables_metadata
from .migration import (
    _detect_schema_version,
    _extract_legacy_noon_temps,
    _is_v2_schema,
    _normalize_temp_map,
    _validate_required_schema_fields,
)
from .schema import DEFAULT_SCHEMA

logger = logging.getLogger("geo")


def _write_cache_yaml_v2(cache_data: dict, out_file: Path) -> None:
    """Write cache document in v2 compact YAML format."""
    with open(out_file, 'w') as f:
        f.write(f"schema_version: {DEFAULT_SCHEMA.version}\n")
        f.write("place:\n")
        f.write(f"  name: {cache_data['place']['name']}\n")
        f.write(f"  lat: {cache_data['place']['lat']}\n")
        f.write(f"  lon: {cache_data['place']['lon']}\n")
        f.write(f"  timezone: {cache_data['place']['timezone']}\n")
        f.write(f"  grid_lat: {cache_data['place']['grid_lat']}\n")
        f.write(f"  grid_lon: {cache_data['place']['grid_lon']}\n")

        f.write(f"{DEFAULT_SCHEMA.variables_key}:\n")
        for variable_name, variable_meta in cache_data.get(DEFAULT_SCHEMA.variables_key, {}).items():
            f.write(f"  {variable_name}:\n")
            for key in ('units', 'source_variable', 'source_dataset', 'temporal_definition', 'precision'):
                if key in variable_meta:
                    f.write(f"    {key}: {variable_meta[key]}\n")

        f.write(f"{DEFAULT_SCHEMA.data_key}:\n")
        for variable_name, value_map in cache_data.get(DEFAULT_SCHEMA.data_key, {}).items():
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
    if schema_version is not None and schema_version > DEFAULT_SCHEMA.version:
        raise ValueError(
            f"Cannot migrate {yaml_file}: schema_version {schema_version} is newer than supported {DEFAULT_SCHEMA.version}"
        )

    schema_def = DEFAULT_SCHEMA.registry['versions'].get(str(schema_version))
    if not isinstance(schema_def, dict):
        raise ValueError(f"Cannot migrate {yaml_file}: unsupported schema_version {schema_version}")

    _validate_required_schema_fields(data, schema_def, yaml_file)

    legacy_temps = _extract_legacy_noon_temps(data)
    if not legacy_temps or 'place' not in data:
        raise ValueError(f"Cannot migrate {yaml_file}: missing legacy temperature data or place metadata")

    normalized = _normalize_temp_map(legacy_temps)
    migrated = {
        'schema_version': DEFAULT_SCHEMA.version,
        'place': data['place'],
        DEFAULT_SCHEMA.variables_key: _build_variables_metadata(),
        DEFAULT_SCHEMA.data_key: {
            DEFAULT_SCHEMA.primary_variable: normalized,
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
    if schema_version is not None and schema_version > DEFAULT_SCHEMA.version:
        raise ValueError(
            f"Cache file '{yaml_file}' uses newer schema_version {schema_version}; "
            f"max supported is {DEFAULT_SCHEMA.version}."
        )

    should_migrate = auto_migrate and (schema_version is not None and schema_version < DEFAULT_SCHEMA.version)
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
