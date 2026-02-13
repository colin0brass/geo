"""Cache path and YAML I/O helpers for geo data documents."""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import pandas as pd

from .cds_base import Location
from .data_schema import (
    DATA_KEY,
    SCHEMA_VERSION,
    VARIABLES_KEY,
    _build_variables_metadata,
    _get_measure_cache_var,
    _get_measure_value_column,
    _load_cache_data_v2,
    _normalize_temp_map,
    _write_cache_yaml_v2,
)

logger = logging.getLogger("geo")


class CacheStore:
    """Read/write façade for place cache files."""

    @staticmethod
    def cache_base_name_for_place(place_name: str) -> str:
        """Build standardized cache file stem for a place name."""
        return place_name.replace(' ', '_').replace(',', '')

    def cache_yaml_path_for_place(self, data_cache_dir: Path, place_name: str) -> Path:
        """Build full cache YAML path for a place in the configured cache directory."""
        return data_cache_dir / f"{self.cache_base_name_for_place(place_name)}.yaml"

    def get_cached_years(self, yaml_file: Path, measure: str = 'noon_temperature') -> set[int]:
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

    def read_data_file(
        self,
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

        rows = []
        for year_str, months in value_map.items():
            year = int(year_str)

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
                        'grid_lon': place_info['grid_lon'],
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
        self,
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

        grid_lat = float(df['grid_lat'].iloc[0])
        grid_lon = float(df['grid_lon'].iloc[0])

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

        if append and existing_data is not None:
            try:
                normalized_existing = _normalize_temp_map(existing_data[DATA_KEY].get(cache_var, {}))

                for year, months in new_values_by_year.items():
                    if year not in normalized_existing:
                        normalized_existing[year] = {}
                    for month, days in months.items():
                        if month not in normalized_existing[year]:
                            normalized_existing[year][month] = {}
                        for day, temp in days.items():
                            normalized_existing[year][month][day] = temp

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

        yaml_data = {
            'schema_version': SCHEMA_VERSION,
            'place': {
                'name': location.name,
                'lat': location.lat,
                'lon': location.lon,
                'timezone': location.tz,
                'grid_lat': grid_lat,
                'grid_lon': grid_lon,
            },
            VARIABLES_KEY: metadata,
            DATA_KEY: data_section,
        }
        _write_cache_yaml_v2(yaml_data, out_file)

        logger.info(f"Saved data to {out_file}")


DEFAULT_CACHE_STORE = CacheStore()
