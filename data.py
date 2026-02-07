"""
Data pipeline utilities for geo_temp.

Handles data retrieval, caching, and I/O operations for temperature data.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from cds import CDS, Location
from progress import get_progress_manager

logger = logging.getLogger("geo_temp")


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
        data_cache_dir: Directory for caching CSV data files.
        
    Returns:
        pd.DataFrame: Concatenated DataFrame with temperature data for all places.
    """
    df_overall = pd.DataFrame()
    total_places = len(place_list)
    progress_mgr = get_progress_manager()
    
    for idx, loc in enumerate(place_list, 1):
        logger.info(f"[{idx}/{total_places}] Retrieving data for {loc.name} ({loc.lat}, {loc.lon}) in timezone {loc.tz}...")
        cds = CDS(cache_dir=cache_dir, progress_manager=progress_mgr)
        start_d = date(start_year, 1, 1)
        end_d = date(end_year, 12, 31)
        file_leaf_name = f"{loc.name.replace(' ', '_').replace(',', '')}_noon_temps_{start_year}_{end_year}.csv"
        data_file = data_cache_dir / file_leaf_name
        if data_file.exists():
            logger.info(f"Data file {data_file} already exists. Loading existing data.")
            df = read_data_file(data_file)
        else:
            df = cds.get_noon_series(loc, start_d, end_d)
            save_data_file(df, data_file)
        df_overall = pd.concat([df_overall, df], ignore_index=True)
    df_overall['date'] = pd.to_datetime(df_overall['date'])
    return df_overall


def read_data_file(in_file: Path) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame, parsing date columns.
    
    Args:
        in_file: Path to the CSV file.
    Returns:
        DataFrame with parsed dates.
    """
    df = pd.read_csv(in_file, parse_dates=['date', 'utc_time_used', 'local_noon'])
    return df


def save_data_file(df: pd.DataFrame, out_file: Path) -> None:
    """
    Save a DataFrame to a CSV file, creating parent directories if needed.
    
    Args:
        df: DataFrame to save.
        out_file: Output file path.
    """
    out_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file, index=False)
    logger.info(f"Saved data to {out_file}")
