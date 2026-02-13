"""Schema model, registry loading, and core schema constants."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

SCHEMA_REGISTRY_FILE = Path(__file__).resolve().parent / 'schema.yaml'


@dataclass(frozen=True)
class Schema:
    """Typed view of the active schema and registry metadata."""

    registry_file: Path
    registry: dict
    version: int
    current: dict
    data_key: str
    variables_key: str
    primary_variable: str
    variables_template: dict

    @staticmethod
    def _read_registry_yaml(schema_file: Path) -> dict:
        """Read raw schema registry YAML content as a mapping."""
        with open(schema_file, 'r') as f:
            return yaml.safe_load(f) or {}

    @staticmethod
    def _validate_schema_definition(version_key: str, schema_def: dict, current_key: str) -> None:
        """Validate one schema version definition block."""
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

    @staticmethod
    def _normalize_versions(raw_versions: dict) -> dict[str, dict]:
        """Normalize registry version keys to strings."""
        return {str(version): schema_def for version, schema_def in raw_versions.items()}

    @classmethod
    def load_registry(cls, schema_file: Path = SCHEMA_REGISTRY_FILE) -> dict:
        """Load and validate cache schema registry metadata from YAML."""
        raw = cls._read_registry_yaml(schema_file)

        current_version = raw.get('current_version')
        versions = raw.get('versions', {})
        if current_version is None or not isinstance(versions, dict) or not versions:
            raise ValueError(f"Invalid cache schema registry: {schema_file}")

        normalized_versions = cls._normalize_versions(versions)
        current_key = str(current_version)
        if current_key not in normalized_versions:
            raise ValueError(
                f"Cache schema version {current_version} not found in registry: {schema_file}"
            )

        for version_key, schema_def in normalized_versions.items():
            cls._validate_schema_definition(version_key, schema_def, current_key)

        return {
            'current_version': int(current_version),
            'versions': normalized_versions,
        }

    @classmethod
    def load(cls, schema_file: Path = SCHEMA_REGISTRY_FILE) -> "Schema":
        registry = cls.load_registry(schema_file)
        version = registry['current_version']
        current = registry['versions'][str(version)]
        return cls(
            registry_file=schema_file,
            registry=registry,
            version=version,
            current=current,
            data_key=current['data_key'],
            variables_key=current['variables_key'],
            primary_variable=current['primary_variable'],
            variables_template=current.get('variables', {}),
        )


DEFAULT_SCHEMA = Schema.load()

_SCHEMA_REGISTRY = DEFAULT_SCHEMA.registry
SCHEMA_VERSION = DEFAULT_SCHEMA.version
CURRENT_SCHEMA = DEFAULT_SCHEMA.current
DATA_KEY = DEFAULT_SCHEMA.data_key
VARIABLES_KEY = DEFAULT_SCHEMA.variables_key
NOON_TEMP_VAR = DEFAULT_SCHEMA.primary_variable
VARIABLES_METADATA_TEMPLATE = DEFAULT_SCHEMA.variables_template
