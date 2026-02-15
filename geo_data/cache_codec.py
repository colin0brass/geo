"""Cache codec helpers for cache read/write and migration."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from .cache_migration import DEFAULT_CACHE_MIGRATION, CacheMigration
from .measure_mapping import DEFAULT_MEASURE_REGISTRY
from .schema import DEFAULT_SCHEMA

logger = logging.getLogger("geo")


try:
    _SAFE_LOADER = yaml.CSafeLoader
except AttributeError:
    _SAFE_LOADER = yaml.SafeLoader


def _yaml_safe_load(stream) -> dict[str, Any]:
    """Load YAML using fastest available safe loader."""
    return yaml.load(stream, Loader=_SAFE_LOADER) or {}


class CacheCodec:
    """Class-based codec for cache YAML read/write and schema migration."""

    def __init__(
        self,
        schema=DEFAULT_SCHEMA,
        measure_registry=DEFAULT_MEASURE_REGISTRY,
        cache_migration: CacheMigration | None = None,
    ) -> None:
        self.schema = schema
        self.measure_registry = measure_registry
        self.cache_migration = DEFAULT_CACHE_MIGRATION if cache_migration is None else cache_migration

    def write_cache_yaml_v2(self, cache_data: dict[str, Any], out_file: Path) -> None:
        """Write cache document in v2 compact YAML format."""
        with open(out_file, 'w') as f:
            f.write(f"schema_version: {self.schema.version}\n")
            f.write("place:\n")
            f.write(f"  name: {cache_data['place']['name']}\n")
            f.write(f"  lat: {cache_data['place']['lat']}\n")
            f.write(f"  lon: {cache_data['place']['lon']}\n")
            f.write(f"  timezone: {cache_data['place']['timezone']}\n")
            f.write(f"  grid_lat: {cache_data['place']['grid_lat']}\n")
            f.write(f"  grid_lon: {cache_data['place']['grid_lon']}\n")

            f.write(f"{self.schema.variables_key}:\n")
            for variable_name, variable_meta in cache_data.get(self.schema.variables_key, {}).items():
                f.write(f"  {variable_name}:\n")
                for key in ('units', 'source_variable', 'source_dataset', 'temporal_definition', 'precision'):
                    if key in variable_meta:
                        f.write(f"    {key}: {variable_meta[key]}\n")

            f.write(f"{self.schema.data_key}:\n")
            for variable_name, value_map in cache_data.get(self.schema.data_key, {}).items():
                f.write(f"  {variable_name}:\n")
                for year in sorted(value_map.keys()):
                    f.write(f"    {year}:\n")
                    for month in sorted(value_map[year].keys()):
                        days_dict = value_map[year][month]
                        days_str = '{' + ', '.join(f'{day}: {value}' for day, value in sorted(days_dict.items())) + '}'
                        f.write(f"      {month}: {days_str}\n")

    def migrate_cache_file_to_v2(self, yaml_file: Path) -> bool:
        """
        Migrate a legacy cache file to schema v2 in-place.

        Returns:
            bool: True if migration occurred, False if already v2.
        """
        with open(yaml_file, 'r') as f:
            data = _yaml_safe_load(f)

        if self.cache_migration.is_v2_schema(data):
            return False

        schema_version = self.cache_migration.detect_schema_version(data)
        if schema_version is None:
            raise ValueError(
                f"Cannot migrate {yaml_file}: unversioned cache documents are no longer supported"
            )
        if schema_version is not None and schema_version > self.schema.version:
            raise ValueError(
                f"Cannot migrate {yaml_file}: schema_version {schema_version} is newer than supported {self.schema.version}"
            )

        schema_def = self.schema.registry['versions'].get(str(schema_version))
        if not isinstance(schema_def, dict):
            raise ValueError(f"Cannot migrate {yaml_file}: unsupported schema_version {schema_version}")

        self.cache_migration.validate_required_schema_fields(data, schema_def, yaml_file)

        legacy_temps = self.cache_migration.extract_legacy_noon_temps(data)
        if not legacy_temps or 'place' not in data:
            raise ValueError(f"Cannot migrate {yaml_file}: missing legacy temperature data or place metadata")

        normalized = self.cache_migration.normalize_temp_map(legacy_temps)
        migrated = {
            'schema_version': self.schema.version,
            'place': data['place'],
            self.schema.variables_key: self.measure_registry.build_variables_metadata(),
            self.schema.data_key: {
                self.schema.primary_variable: normalized,
            },
        }
        self.write_cache_yaml_v2(migrated, yaml_file)
        logger.info(f"Migrated cache file to schema v2: {yaml_file}")
        return True

    def load_cache_data_v2(self, yaml_file: Path, auto_migrate: bool = True) -> dict[str, Any]:
        """Load cache file and ensure it is schema v2 (optionally auto-migrating)."""
        with open(yaml_file, 'r') as f:
            data = _yaml_safe_load(f)

        if self.cache_migration.is_v2_schema(data):
            return data

        schema_version = self.cache_migration.detect_schema_version(data)
        if schema_version is None:
            raise ValueError(
                f"Cannot migrate {yaml_file}: unversioned cache documents are no longer supported"
            )
        if schema_version is not None and schema_version > self.schema.version:
            raise ValueError(
                f"Cache file '{yaml_file}' uses newer schema_version {schema_version}; "
                f"max supported is {self.schema.version}."
            )

        should_migrate = auto_migrate and (schema_version is not None and schema_version < self.schema.version)
        if should_migrate:
            migrated = self.migrate_cache_file_to_v2(yaml_file)
            if migrated:
                with open(yaml_file, 'r') as f:
                    data = _yaml_safe_load(f)
                if self.cache_migration.is_v2_schema(data):
                    return data

        raise ValueError(
            f"Cache file '{yaml_file}' is not schema v2. "
            "Run migration before reading this file."
        )


DEFAULT_CACHE_CODEC = CacheCodec()
