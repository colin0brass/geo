"""
Data pipeline utilities for geo_temp.

Handles data retrieval, caching, and I/O operations for temperature data.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import yaml

from cds import CDS, Location
from progress import get_progress_manager

logger = logging.getLogger("geo_temp")


def get_cached_years(yaml_file: Path) -> set[int]:
    """
    Get the set of years available in a YAML cache file.
    
    Args:
        yaml_file: Path to the YAML cache file.
        
    Returns:
        Set of years (as integers) available in the file.
    """
    try:
        with open(yaml_file, 'r') as f:
            data = yaml.safe_load(f)
        
        if 'temperatures' in data:
            return set(int(year) for year in data['temperatures'].keys())
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
        base_name = f"{loc.name.replace(' ', '_').replace(',', '')}_noon_temps"
        yaml_file = data_cache_dir / f"{base_name}.yaml"
        
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
        base_name = f"{loc.name.replace(' ', '_').replace(',', '')}_noon_temps"
        yaml_file = data_cache_dir / f"{base_name}.yaml"
        
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
            
            # Notify progress manager of location start
            progress_mgr.notify_location_start(loc.name, cds_place_num, total_cds_places)
            
            cds = CDS(cache_dir=cache_dir, progress_manager=progress_mgr)
            
            for year in missing_years:
                start_d = date(year, 1, 1)
                end_d = date(year, 12, 31)
                logger.info(f"  Retrieving {year} for {loc.name}...")
                df_year = cds.get_noon_series(loc, start_d, end_d)
                
                # Append to cache file (merges with existing data)
                save_data_file(df_year, yaml_file, loc, append=True)
                
                # Add to overall dataframe
                df_overall = pd.concat([df_overall, df_year], ignore_index=True)
    
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
    # Load YAML format
    with open(in_file, 'r') as f:
        data = yaml.safe_load(f)
    
    place_info = data['place']
    temps = data['temperatures']
    
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
            with open(out_file, 'r') as f:
                existing_data = yaml.safe_load(f)
            
            # Merge temperature data (new data overwrites existing for same dates)
            existing_temps = existing_data.get('temperatures', {})
            # Normalize all keys to integers for merging
            normalized_existing = {}
            for year_key, months in existing_temps.items():
                year_int = int(year_key)
                if year_int not in normalized_existing:
                    normalized_existing[year_int] = {}
                for month_key, days in months.items():
                    month_int = int(month_key)
                    if month_int not in normalized_existing[year_int]:
                        normalized_existing[year_int][month_int] = {}
                    for day_key, temp in days.items():
                        day_int = int(day_key)
                        normalized_existing[year_int][month_int][day_int] = temp
            
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
        'place': {
            'name': location.name,
            'lat': location.lat,
            'lon': location.lon,
            'timezone': location.tz,
            'grid_lat': grid_lat,
            'grid_lon': grid_lon
        },
        'temperatures': temps_by_year
    }
    
    # Write YAML file with compact month formatting (1 line per month)
    with open(out_file, 'w') as f:
        # Write place metadata
        f.write("place:\n")
        f.write(f"  name: {location.name}\n")
        f.write(f"  lat: {location.lat}\n")
        f.write(f"  lon: {location.lon}\n")
        f.write(f"  timezone: {location.tz}\n")
        f.write(f"  grid_lat: {grid_lat}\n")
        f.write(f"  grid_lon: {grid_lon}\n")
        f.write("temperatures:\n")
        
        # Write temperatures in compact format (1 line per month)
        for year in sorted(temps_by_year.keys()):
            f.write(f"  {year}:\n")
            for month in sorted(temps_by_year[year].keys()):
                days_dict = temps_by_year[year][month]
                # Format as inline dict: {1: 10.5, 2: 11.2, ...}
                days_str = '{' + ', '.join(f'{day}: {temp}' for day, temp in sorted(days_dict.items())) + '}'
                f.write(f"    {month}: {days_str}\n")
    
    logger.info(f"Saved data to {out_file}")
