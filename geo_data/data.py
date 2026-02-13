"""
Data pipeline utilities for geo.

Handles data retrieval, caching, and I/O operations for temperature data.
"""

from __future__ import annotations

import logging
import inspect
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from collections.abc import Callable

import pandas as pd
import yaml

from .cds import CDS, Location
from geo_core.progress import get_progress_manager

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

MEASURE_TO_CDS_METHOD = {
    'noon_temperature': 'get_noon_series',
    'daily_precipitation': 'get_daily_precipitation_series',
}


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


def _get_measure_cds_method(measure: str) -> str:
    """Resolve CDS client method name for a logical measure name."""
    try:
        return MEASURE_TO_CDS_METHOD[measure]
    except KeyError as exc:
        allowed = ', '.join(sorted(MEASURE_TO_CDS_METHOD.keys()))
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


def _get_legacy_schema_keys() -> list[str]:
    """Collect root-level legacy temperature keys from prior schema versions."""
    collected: list[str] = []
    versions = _SCHEMA_REGISTRY.get('versions', {})
    for version_str, schema_def in versions.items():
        try:
            version = int(version_str)
        except ValueError:
            continue
        if version >= SCHEMA_VERSION:
            continue

        for key in _schema_legacy_data_paths(schema_def):
            if key not in collected:
                collected.append(key)

    return collected


LEGACY_TEMPERATURE_KEYS = _get_legacy_schema_keys()


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
    if schema_version is not None:
        schema_def = _SCHEMA_REGISTRY['versions'].get(str(schema_version), {})
        for legacy_key in _schema_legacy_data_paths(schema_def):
            if legacy_key in data:
                return data[legacy_key]

    for legacy_key in LEGACY_TEMPERATURE_KEYS:
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


def cache_base_name_for_place(place_name: str) -> str:
    """Build standardized cache file stem for a place name."""
    safe_place = place_name.replace(' ', '_').replace(',', '')
    return safe_place


def cache_yaml_path_for_place(data_cache_dir: Path, place_name: str) -> Path:
    """Build full cache YAML path for a place in the configured cache directory."""
    return data_cache_dir / f"{cache_base_name_for_place(place_name)}.yaml"


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
    if schema_version is not None and schema_version > SCHEMA_VERSION:
        raise ValueError(
            f"Cannot migrate {yaml_file}: schema_version {schema_version} is newer than supported {SCHEMA_VERSION}"
        )

    if schema_version is not None:
        schema_def = _SCHEMA_REGISTRY['versions'].get(str(schema_version), {})
        if isinstance(schema_def, dict):
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
    if schema_version is not None and schema_version > SCHEMA_VERSION:
        raise ValueError(
            f"Cache file '{yaml_file}' uses newer schema_version {schema_version}; "
            f"max supported is {SCHEMA_VERSION}."
        )

    should_migrate = auto_migrate and (schema_version is None or schema_version < SCHEMA_VERSION)
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


def get_cached_years(yaml_file: Path, measure: str = 'noon_temperature') -> set[int]:
    """
    Get the set of years available in a YAML cache file.

    Args:
        yaml_file: Path to the YAML cache file.

    Returns:
        Set of years (as integers) available in the file.
    """
    try:
        if not yaml_file.exists():
            return set()

        data = _load_cache_data_v2(yaml_file, auto_migrate=True)
        value_map = data[DATA_KEY].get(_get_measure_cache_var(measure), {})
        if value_map:
            return set(int(year) for year in value_map.keys())
        return set()
    except Exception as e:
        logger.warning(f"Error reading cached years from {yaml_file}: {e}")
        return set()


def format_cds_retrieval_summary(places_needing_cds: list[str]) -> str:
    """Build a user-facing summary for CDS retrieval requirements."""
    if places_needing_cds:
        lines = [
            "",
            f"{'='*60}",
            f"CDS Retrieval Required: {len(places_needing_cds)} place(s)",
            f"{'='*60}",
        ]
        lines.extend([f"  • {place_name}" for place_name in places_needing_cds])
        lines.append(f"{'='*60}\n")
        return "\n".join(lines)

    return "\n".join([
        "",
        f"{'='*60}",
        "All data already cached - no CDS retrieval needed",
        f"{'='*60}\n",
    ])


def retrieve_and_concat_data(
    place_list: list[Location],
    start_year: int,
    end_year: int,
    cache_dir: Path = Path("era5_cache"),
    data_cache_dir: Path = Path("data_cache"),
    measure: str = 'noon_temperature',
    status_reporter: Callable[[str], None] | None = print,
) -> pd.DataFrame:
    """
    Retrieve measure data for all places and concatenate into a single DataFrame.

    Args:
        place_list: List of Location objects to retrieve data for.
        start_year: Start year for data retrieval.
        end_year: End year for data retrieval.
        cache_dir: Directory for caching NetCDF files.
        data_cache_dir: Directory for caching YAML data files.
        status_reporter: Optional callback that receives formatted status text.
            Pass None to suppress user-facing status output.

    Returns:
        pd.DataFrame: Concatenated DataFrame with selected measure data for all places.
    """
    cds_method_name = _get_measure_cds_method(measure)
    df_overall = pd.DataFrame()
    progress_mgr = get_progress_manager()
    requested_years = set(range(start_year, end_year + 1))

    # First pass: determine which places need CDS retrieval
    places_needing_cds = []
    for loc in place_list:
        yaml_file = cache_yaml_path_for_place(data_cache_dir, loc.name)

        # Check which years are already cached
        cached_years = set()
        if yaml_file.exists():
            cached_years = get_cached_years(yaml_file, measure=measure)

        # Determine if this place needs any CDS fetches
        missing_years = sorted(requested_years - cached_years)
        if missing_years:
            places_needing_cds.append(loc.name)

    summary_text = format_cds_retrieval_summary(places_needing_cds)
    if status_reporter:
        status_reporter(summary_text)

    # Second pass: process each location
    cds_place_num = 0  # Track place number for CDS retrieval
    total_cds_places = len(places_needing_cds)

    for loc in place_list:
        yaml_file = cache_yaml_path_for_place(data_cache_dir, loc.name)

        # Check which years are already cached
        cached_years = set()
        if yaml_file.exists():
            cached_years = get_cached_years(yaml_file, measure=measure)

        # Determine which years need to be fetched
        missing_years = sorted(requested_years - cached_years)

        # Load cached data for this location
        if cached_years:
            logger.info(
                f"Loading {loc.name} from cache for {measure} "
                f"(years: {min(cached_years)}-{max(cached_years)})"
            )
            df_cached = read_data_file(yaml_file, start_year, end_year, measure=measure)
            df_overall = pd.concat([df_overall, df_cached], ignore_index=True)

        # Fetch missing years
        if missing_years:
            cds_place_num += 1
            logger.info(f"Fetching {loc.name} from CDS for {len(missing_years)} year(s): {missing_years}")

            # Notify progress manager of location start with total years to fetch
            progress_mgr.notify_location_start(loc.name, cds_place_num, total_cds_places, len(missing_years))

            cds = CDS(cache_dir=cache_dir, progress_manager=progress_mgr)
            if not hasattr(cds, cds_method_name):
                raise NotImplementedError(
                    f"Measure '{measure}' is not implemented by CDS client "
                    f"(missing method '{cds_method_name}')."
                )
            cds_method = getattr(cds, cds_method_name)
            cds_signature = inspect.signature(cds_method)

            for year_idx, year in enumerate(missing_years, 1):
                start_d = date(year, 1, 1)
                end_d = date(year, 12, 31)

                # Notify year start with accurate position
                progress_mgr.notify_year_start(loc.name, year, year_idx, len(missing_years))

                logger.info(f"  Retrieving {year} for {loc.name}...")
                if 'notify_progress' in cds_signature.parameters:
                    df_year = cds_method(loc, start_d, end_d, notify_progress=False)
                else:
                    df_year = cds_method(loc, start_d, end_d)

                # Append to cache file (merges with existing data)
                save_data_file(df_year, yaml_file, loc, append=True, measure=measure)

                # Add to overall dataframe
                df_overall = pd.concat([df_overall, df_year], ignore_index=True)

                # Notify year complete
                progress_mgr.notify_year_complete(loc.name, year, year_idx, len(missing_years))

            # Notify location complete to move to next line
            progress_mgr.notify_location_complete(loc.name)

    if not df_overall.empty:
        df_overall['date'] = pd.to_datetime(df_overall['date'])
    return df_overall


def read_data_file(
    in_file: Path,
    start_year: int | None = None,
    end_year: int | None = None,
    measure: str = 'noon_temperature',
) -> pd.DataFrame:
    """
    Read a YAML data file into a pandas DataFrame.

    Args:
        in_file: Path to the YAML data file.
        start_year: Optional start year to filter data.
        end_year: Optional end year to filter data.
    Returns:
        DataFrame with parsed dates and selected measure data.
    """
    data = _load_cache_data_v2(in_file, auto_migrate=True)

    place_info = data['place']
    cache_var = _get_measure_cache_var(measure)
    value_column = _get_measure_value_column(measure)
    value_map = data[DATA_KEY].get(cache_var, {})

    if not value_map:
        empty_columns = ['date', value_column, 'place_name', 'grid_lat', 'grid_lon']
        if measure == 'noon_temperature':
            empty_columns.insert(2, 'temp_F')
        return pd.DataFrame(columns=empty_columns)

    # Reconstruct DataFrame from hierarchical structure
    rows = []
    for year_str, months in value_map.items():
        year = int(year_str)

        # Filter by year range if specified
        if start_year is not None and year < start_year:
            continue
        if end_year is not None and year > end_year:
            continue

        for month_str, days in months.items():
            month = int(month_str)
            for day_str, value in days.items():
                day = int(day_str)
                date_obj = datetime(year, month, day)
                row = {
                    'date': date_obj,
                    value_column: value,
                    'place_name': place_info['name'],
                    'grid_lat': place_info['grid_lat'],
                    'grid_lon': place_info['grid_lon']
                }
                if measure == 'noon_temperature':
                    temp_c = float(value)
                    row['temp_F'] = temp_c * 9.0 / 5.0 + 32.0
                rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
    return df


def save_data_file(
    df: pd.DataFrame,
    out_file: Path,
    location: Location,
    append: bool = False,
    measure: str = 'noon_temperature',
) -> None:
    """
    Save a DataFrame to a YAML file with hierarchical structure.

    Args:
        df: DataFrame to save.
        out_file: Output file path (.yaml extension).
        location: Location object with place metadata.
        append: If True, merge with existing file; if False, overwrite.
    """
    out_file.parent.mkdir(parents=True, exist_ok=True)
    value_column = _get_measure_value_column(measure)
    cache_var = _get_measure_cache_var(measure)
    if value_column not in df.columns:
        raise KeyError(f"Missing required column '{value_column}' for measure '{measure}'")

    # Extract unique grid coordinates (should be same for all rows)
    grid_lat = float(df['grid_lat'].iloc[0])
    grid_lon = float(df['grid_lon'].iloc[0])

    # Build hierarchical structure for new data: year -> month -> day -> value
    new_values_by_year = {}
    for _, row in df.iterrows():
        date_obj = pd.to_datetime(row['date'])
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        value = round(float(row[value_column]), 2)

        if year not in new_values_by_year:
            new_values_by_year[year] = {}
        if month not in new_values_by_year[year]:
            new_values_by_year[year][month] = {}
        new_values_by_year[year][month][day] = value

    existing_data = None
    if out_file.exists():
        try:
            existing_data = _load_cache_data_v2(out_file, auto_migrate=True)
        except Exception as e:
            if append:
                logger.warning(f"Error loading existing cache for append: {e}. Overwriting.")
            existing_data = None

    # If appending, merge with existing data
    if append and existing_data is not None:
        try:
            # Merge selected measure data (new data overwrites existing for same dates)
            normalized_existing = _normalize_temp_map(existing_data[DATA_KEY].get(cache_var, {}))

            # Merge with new data
            for year, months in new_values_by_year.items():
                if year not in normalized_existing:
                    normalized_existing[year] = {}
                for month, days in months.items():
                    if month not in normalized_existing[year]:
                        normalized_existing[year][month] = {}
                    for day, temp in days.items():
                        normalized_existing[year][month][day] = temp

            # Use merged data
            values_by_year = normalized_existing
        except Exception as e:
            logger.warning(f"Error merging with existing cache: {e}. Overwriting.")
            values_by_year = new_values_by_year
    else:
        values_by_year = new_values_by_year

    metadata = _build_variables_metadata(measure=measure)
    data_section = {}
    if append and existing_data is not None:
        metadata = deepcopy(existing_data.get(VARIABLES_KEY, metadata))
        if cache_var not in metadata:
            metadata.update(_build_variables_metadata(measure=measure))
        data_section = deepcopy(existing_data.get(DATA_KEY, {}))
    data_section[cache_var] = values_by_year

    # Create YAML structure
    yaml_data = {
        'schema_version': SCHEMA_VERSION,
        'place': {
            'name': location.name,
            'lat': location.lat,
            'lon': location.lon,
            'timezone': location.tz,
            'grid_lat': grid_lat,
            'grid_lon': grid_lon
        },
        VARIABLES_KEY: metadata,
        DATA_KEY: data_section,
    }
    _write_cache_yaml_v2(yaml_data, out_file)

    logger.info(f"Saved data to {out_file}")
