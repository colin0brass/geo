"""Schema-version detection and cache migration helpers."""

from __future__ import annotations

from pathlib import Path

from .schema import DEFAULT_SCHEMA


class CacheMigration:
    """Utility class for cache schema detection, validation, and normalization."""

    def __init__(self, schema=DEFAULT_SCHEMA) -> None:
        self.schema = schema

    def schema_legacy_data_paths(self, schema_def: dict) -> list[str]:
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

    def get_by_path(self, data: dict, path: str):
        """Get a nested value from dict using dot-separated path."""
        node = data
        for part in path.split('.'):
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
        return node

    def has_path(self, data: dict, path: str) -> bool:
        """Check whether a dot-separated key path exists in a mapping."""
        node = data
        for part in path.split('.'):
            if not isinstance(node, dict) or part not in node:
                return False
            node = node[part]
        return True

    def extract_temp_map_from_schema_mapping(self, data: dict) -> dict:
        """Extract temperature map using versioned migration field mapping if present."""
        schema_version = data.get('schema_version')
        if schema_version is None:
            return {}

        target_path = f"{self.schema.data_key}.{self.schema.primary_variable}"

        mapping = None

        migration = self.schema.current.get('migration', {})
        if isinstance(migration, dict):
            from_version = migration.get('from_version')
            if from_version is not None and str(from_version) == str(schema_version):
                field_mappings = migration.get('field_mappings', {})
                if isinstance(field_mappings, dict):
                    mapping = field_mappings.get(target_path)

        if mapping is None:
            current_migrations = self.schema.current.get('migration_from_previous', {})
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
            schema_def = self.schema.registry['versions'].get(str(schema_version), {})
            migration = schema_def.get('migration_to_next', {})
            if isinstance(migration, dict):
                field_mappings = migration.get('field_mappings', {})
                if isinstance(field_mappings, dict):
                    mapping = field_mappings.get(target_path)

        if isinstance(mapping, str):
            value = self.get_by_path(data, mapping)
            return value if isinstance(value, dict) else {}

        if isinstance(mapping, dict):
            source_path = mapping.get('source_path')
            if isinstance(source_path, str):
                value = self.get_by_path(data, source_path)
                if isinstance(value, dict):
                    return value

            for candidate in mapping.get('source_candidates', []):
                if not isinstance(candidate, str):
                    continue
                value = self.get_by_path(data, candidate)
                if isinstance(value, dict):
                    return value

        return {}

    def extract_legacy_noon_temps(self, data: dict) -> dict:
        """Extract legacy temperature map if present."""
        mapped = self.extract_temp_map_from_schema_mapping(data)
        if mapped:
            return mapped

        schema_version = data.get('schema_version')
        if schema_version is None:
            return {}

        schema_def = self.schema.registry['versions'].get(str(schema_version), {})
        for legacy_key in self.schema_legacy_data_paths(schema_def):
            if legacy_key in data:
                return data[legacy_key]
        return {}

    def validate_required_schema_fields(self, data: dict, schema_def: dict, yaml_file: Path) -> None:
        """Validate required key-path contracts declared in schema metadata."""
        required_paths = schema_def.get('required')
        if required_paths is None:
            required_paths = schema_def.get('required_paths', [])

        for path in required_paths:
            if not self.has_path(data, path):
                raise ValueError(f"Cannot migrate {yaml_file}: missing required path '{path}'")

        required_any_of = schema_def.get('required_any_of')
        if required_any_of is None:
            required_any_of = schema_def.get('required_any_of_paths', [])

        for group in required_any_of:
            if not any(self.has_path(data, candidate) for candidate in group):
                group_str = ', '.join(group)
                raise ValueError(
                    f"Cannot migrate {yaml_file}: missing required key path group; expected one of [{group_str}]"
                )

    def is_v2_schema(self, data: dict) -> bool:
        """Check whether a loaded cache document matches v2 schema."""
        return (
            isinstance(data, dict)
            and data.get('schema_version') == self.schema.version
            and isinstance(data.get(self.schema.variables_key), dict)
            and isinstance(data.get(self.schema.data_key), dict)
        )

    @staticmethod
    def detect_schema_version(data: dict) -> int | None:
        """Return schema version from document, or None if unversioned legacy."""
        raw_version = data.get('schema_version')
        if raw_version is None:
            return None

        try:
            return int(raw_version)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid schema_version value: {raw_version!r}") from exc

    @staticmethod
    def normalize_temp_map(temp_map: dict) -> dict:
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


DEFAULT_CACHE_MIGRATION = CacheMigration()
