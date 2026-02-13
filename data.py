"""
Data pipeline utilities for geo_temp.

Handles data retrieval, caching, and I/O operations for temperature data.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import yaml

from cds import CDS, Location
from progress import get_progress_manager

logger = logging.getLogger("geo_temp")

SCHEMA_REGISTRY_FILE = Path(__file__).with_name('schema.yaml')


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

        primary_key = schema_def.get('temperature_key')
        if isinstance(primary_key, str) and primary_key and primary_key not in collected:
            collected.append(primary_key)

        for legacy_key in schema_def.get('legacy_temperature_keys', []):
            if isinstance(legacy_key, str) and legacy_key and legacy_key not in collected:
                collected.append(legacy_key)

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


def _build_variables_metadata() -> dict:
    """Build default variable metadata for v2 cache schema."""
    if VARIABLES_METADATA_TEMPLATE:
        return deepcopy(VARIABLES_METADATA_TEMPLATE)
    return {
        NOON_TEMP_VAR: {
            'units': 'C',
            'source_variable': '2m_temperature',
            'source_dataset': 'reanalysis-era5-single-levels',
            'temporal_definition': 'daily_local_noon',
            'precision': 2,
        }
    }


def _extract_legacy_noon_temps(data: dict) -> dict:
    """Extract legacy temperature map if present."""
    mapped = _extract_temp_map_from_schema_mapping(data)
    if mapped:
        return mapped

    schema_version = data.get('schema_version')
    if schema_version is not None:
        schema_def = _SCHEMA_REGISTRY['versions'].get(str(schema_version), {})
        schema_key = schema_def.get('temperature_key')
        if isinstance(schema_key, str) and schema_key in data:
            return data[schema_key]

        for legacy_key in schema_def.get('legacy_temperature_keys', []):
            if legacy_key in data:
                return data[legacy_key]

    for legacy_key in LEGACY_TEMPERATURE_KEYS:
        if legacy_key in data:
            return data[legacy_key]
    return {}


def _is_v2_schema(data: dict) -> bool:
    """Check whether a loaded cache document matches v2 schema."""
    return (
        isinstance(data, dict)
        and data.get('schema_version') == SCHEMA_VERSION
        and isinstance(data.get(DATA_KEY), dict)
        and NOON_TEMP_VAR in data[DATA_KEY]
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
        f.write(f"  {NOON_TEMP_VAR}:\n")
        f.write(f"    units: {cache_data[VARIABLES_KEY][NOON_TEMP_VAR]['units']}\n")
        f.write(f"    source_variable: {cache_data[VARIABLES_KEY][NOON_TEMP_VAR]['source_variable']}\n")
        f.write(f"    source_dataset: {cache_data[VARIABLES_KEY][NOON_TEMP_VAR]['source_dataset']}\n")
        f.write(f"    temporal_definition: {cache_data[VARIABLES_KEY][NOON_TEMP_VAR]['temporal_definition']}\n")
        f.write(f"    precision: {cache_data[VARIABLES_KEY][NOON_TEMP_VAR]['precision']}\n")

        f.write(f"{DATA_KEY}:\n")
        f.write(f"  {NOON_TEMP_VAR}:\n")
        temp_map = cache_data[DATA_KEY][NOON_TEMP_VAR]
        for year in sorted(temp_map.keys()):
            f.write(f"    {year}:\n")
            for month in sorted(temp_map[year].keys()):
                days_dict = temp_map[year][month]
                days_str = '{' + ', '.join(f'{day}: {temp}' for day, temp in sorted(days_dict.items())) + '}'
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


def get_cached_years(yaml_file: Path) -> set[int]:
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
        noon_temps = data[DATA_KEY][NOON_TEMP_VAR]
        if noon_temps:
            return set(int(year) for year in noon_temps.keys())
        return set()
    except Exception as e:
        logger.warning(f"Error reading cached years from {yaml_file}: {e}")
        return set()


def retrieve_and_concat_data(
    place_list: list[Location],
    start_year: int,
    end_year: int,
    cache_dir: Path,
    data_cache_dir: Path
) -> pd.DataFrame:
    """
    Retrieve temperature data for all places and concatenate into a single DataFrame.
    
    Args:
        place_list: List of Location objects to retrieve data for.
        start_year: Start year for data retrieval.
        end_year: End year for data retrieval.
        cache_dir: Directory for caching NetCDF files.
        data_cache_dir: Directory for caching YAML data files.
        
    Returns:
        pd.DataFrame: Concatenated DataFrame with temperature data for all places.
    """
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
            cached_years = get_cached_years(yaml_file)
        
        # Determine if this place needs any CDS fetches
        missing_years = sorted(requested_years - cached_years)
        if missing_years:
            places_needing_cds.append(loc.name)
    
    # Print summary of CDS retrieval needs
    if places_needing_cds:
        print(f"\n{'='*60}")
        print(f"CDS Retrieval Required: {len(places_needing_cds)} place(s)")
        print(f"{'='*60}")
        for place_name in places_needing_cds:
            print(f"  • {place_name}")
        print(f"{'='*60}\n")
    else:
        print(f"\n{'='*60}")
        print(f"All data already cached - no CDS retrieval needed")
        print(f"{'='*60}\n")
    
    # Second pass: process each location
    cds_place_num = 0  # Track place number for CDS retrieval
    total_cds_places = len(places_needing_cds)
    
    for loc in place_list:
        yaml_file = cache_yaml_path_for_place(data_cache_dir, loc.name)
        
        # Check which years are already cached
        cached_years = set()
        if yaml_file.exists():
            cached_years = get_cached_years(yaml_file)
        
        # Determine which years need to be fetched
        missing_years = sorted(requested_years - cached_years)
        
        # Load cached data for this location
        if cached_years:
            logger.info(f"Loading {loc.name} from cache (years: {min(cached_years)}-{max(cached_years)})")
            df_cached = read_data_file(yaml_file, start_year, end_year)
            df_overall = pd.concat([df_overall, df_cached], ignore_index=True)
        
        # Fetch missing years
        if missing_years:
            cds_place_num += 1
            logger.info(f"Fetching {loc.name} from CDS for {len(missing_years)} year(s): {missing_years}")
            
            # Notify progress manager of location start with total years to fetch
            progress_mgr.notify_location_start(loc.name, cds_place_num, total_cds_places, len(missing_years))
            
            cds = CDS(cache_dir=cache_dir, progress_manager=progress_mgr)
            
            for year_idx, year in enumerate(missing_years, 1):
                start_d = date(year, 1, 1)
                end_d = date(year, 12, 31)
                
                # Notify year start with accurate position
                progress_mgr.notify_year_start(loc.name, year, year_idx, len(missing_years))
                
                logger.info(f"  Retrieving {year} for {loc.name}...")
                df_year = cds.get_noon_series(loc, start_d, end_d, notify_progress=False)
                
                # Append to cache file (merges with existing data)
                save_data_file(df_year, yaml_file, loc, append=True)
                
                # Add to overall dataframe
                df_overall = pd.concat([df_overall, df_year], ignore_index=True)
                
                # Notify year complete
                progress_mgr.notify_year_complete(loc.name, year, year_idx, len(missing_years))
            
            # Notify location complete to move to next line
            progress_mgr.notify_location_complete(loc.name)
    
    df_overall['date'] = pd.to_datetime(df_overall['date'])
    return df_overall


def read_data_file(in_file: Path, start_year: int | None = None, end_year: int | None = None) -> pd.DataFrame:
    """
    Read a YAML data file into a pandas DataFrame.
    
    Args:
        in_file: Path to the YAML data file.
        start_year: Optional start year to filter data.
        end_year: Optional end year to filter data.
    Returns:
        DataFrame with parsed dates and temperature data.
    """
    data = _load_cache_data_v2(in_file, auto_migrate=True)

    place_info = data['place']
    temps = data[DATA_KEY][NOON_TEMP_VAR]
    
    # Reconstruct DataFrame from hierarchical structure
    rows = []
    for year_str, months in temps.items():
        year = int(year_str)
        
        # Filter by year range if specified
        if start_year is not None and year < start_year:
            continue
        if end_year is not None and year > end_year:
            continue
        
        for month_str, days in months.items():
            month = int(month_str)
            for day_str, temp_c in days.items():
                day = int(day_str)
                date_obj = datetime(year, month, day)
                rows.append({
                    'date': date_obj,
                    'temp_C': temp_c,
                    'temp_F': temp_c * 9.0 / 5.0 + 32.0,
                    'place_name': place_info['name'],
                    'grid_lat': place_info['grid_lat'],
                    'grid_lon': place_info['grid_lon']
                })
    
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df


def save_data_file(df: pd.DataFrame, out_file: Path, location: Location, append: bool = False) -> None:
    """
    Save a DataFrame to a YAML file with hierarchical structure.
    
    Args:
        df: DataFrame to save (must have 'date', 'temp_C', 'grid_lat', 'grid_lon' columns).
        out_file: Output file path (.yaml extension).
        location: Location object with place metadata.
        append: If True, merge with existing file; if False, overwrite.
    """
    out_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Extract unique grid coordinates (should be same for all rows)
    grid_lat = float(df['grid_lat'].iloc[0])
    grid_lon = float(df['grid_lon'].iloc[0])
    
    # Build hierarchical structure for new data: year -> month -> day -> temp_C
    new_temps_by_year = {}
    for _, row in df.iterrows():
        date_obj = pd.to_datetime(row['date'])
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        temp_c = round(float(row['temp_C']), 2)
        
        if year not in new_temps_by_year:
            new_temps_by_year[year] = {}
        if month not in new_temps_by_year[year]:
            new_temps_by_year[year][month] = {}
        new_temps_by_year[year][month][day] = temp_c
    
    # If appending, merge with existing data
    if append and out_file.exists():
        try:
            existing_data = _load_cache_data_v2(out_file, auto_migrate=True)

            # Merge temperature data (new data overwrites existing for same dates)
            normalized_existing = _normalize_temp_map(existing_data[DATA_KEY][NOON_TEMP_VAR])
            
            # Merge with new data
            for year, months in new_temps_by_year.items():
                if year not in normalized_existing:
                    normalized_existing[year] = {}
                for month, days in months.items():
                    if month not in normalized_existing[year]:
                        normalized_existing[year][month] = {}
                    for day, temp in days.items():
                        normalized_existing[year][month][day] = temp
            
            # Use merged data
            temps_by_year = normalized_existing
        except Exception as e:
            logger.warning(f"Error merging with existing cache: {e}. Overwriting.")
            temps_by_year = new_temps_by_year
    else:
        temps_by_year = new_temps_by_year
    
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
        VARIABLES_KEY: _build_variables_metadata(),
        DATA_KEY: {
            NOON_TEMP_VAR: temps_by_year,
        }
    }
    _write_cache_yaml_v2(yaml_data, out_file)
    
    logger.info(f"Saved data to {out_file}")
