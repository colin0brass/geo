"""Cache path and YAML I/O helpers for geo data documents."""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import pandas as pd

from .cds_base import Location
from .cache_migration import DEFAULT_CACHE_MIGRATION
from .measure_mapping import (
    DEFAULT_MEASURE_REGISTRY,
)
from .schema import (
    DATA_KEY,
    SCHEMA_VERSION,
    VARIABLES_KEY,
)
from .cache_codec import (
    DEFAULT_CACHE_CODEC,
    CacheCodec,
)

logger = logging.getLogger("geo")


class CacheStore:
    """Read/write façade for place cache files."""

    def __init__(self, cache_codec: CacheCodec | None = None) -> None:
        self.cache_codec = DEFAULT_CACHE_CODEC if cache_codec is None else cache_codec

    @staticmethod
    def _empty_result_columns(measure: str, value_column: str) -> list[str]:
        """Return empty DataFrame column layout for a logical measure."""
        columns = ['date', value_column, 'place_name', 'grid_lat', 'grid_lon']
        if measure == 'noon_temperature':
            columns.insert(2, 'temp_F')
        return columns

    @staticmethod
    def _build_rows_from_value_map(
        value_map: dict,
        place_info: dict,
        value_column: str,
        start_year: int | None,
        end_year: int | None,
        measure: str,
    ) -> list[dict]:
        """Build flat row records from nested year/month/day mapping."""
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
        return rows

    @staticmethod
    def _build_new_values_by_year(df: pd.DataFrame, value_column: str) -> dict:
        """Build nested year/month/day map from a value DataFrame."""
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

        return new_values_by_year

    def _load_existing_data_for_write(self, out_file: Path, append: bool) -> dict | None:
        """Load existing cache payload when present, handling append overwrite fallback."""
        if not out_file.exists():
            return None
        try:
            return self.cache_codec.load_cache_data_v2(out_file, auto_migrate=True)
        except Exception as exc:
            if append:
                logger.warning(f"Error loading existing cache for append: {exc}. Overwriting.")
            return None

    @staticmethod
    def _merge_values_by_year(existing_data: dict, cache_var: str, new_values_by_year: dict) -> dict:
        """Merge existing nested values with new year/month/day values."""
        normalized_existing = DEFAULT_CACHE_MIGRATION.normalize_temp_map(
            existing_data[DATA_KEY].get(cache_var, {})
        )

        for year, months in new_values_by_year.items():
            if year not in normalized_existing:
                normalized_existing[year] = {}
            for month, days in months.items():
                if month not in normalized_existing[year]:
                    normalized_existing[year][month] = {}
                for day, temp in days.items():
                    normalized_existing[year][month][day] = temp

        return normalized_existing

    @staticmethod
    def _build_yaml_payload(
        location: Location,
        grid_lat: float,
        grid_lon: float,
        metadata: dict,
        data_section: dict,
    ) -> dict:
        """Build schema-v2 YAML payload for cache write."""
        return {
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
            measure: Logical measure key (for example, 'noon_temperature').

        Returns:
            Set of years (as integers) available in the file.
        """
        try:
            if not yaml_file.exists():
                return set()

            data = self.cache_codec.load_cache_data_v2(yaml_file, auto_migrate=True)
            value_map = data[DATA_KEY].get(DEFAULT_MEASURE_REGISTRY.get_cache_var(measure), {})
            if value_map:
                return set(int(year) for year in value_map.keys())
            return set()
        except Exception as exc:
            logger.warning(f"Error reading cached years from {yaml_file}: {exc}")
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
            measure: Logical measure key (for example, 'noon_temperature').
        Returns:
            DataFrame with parsed dates and selected measure data.
        """
        data = self.cache_codec.load_cache_data_v2(in_file, auto_migrate=True)

        place_info = data['place']
        cache_var = DEFAULT_MEASURE_REGISTRY.get_cache_var(measure)
        value_column = DEFAULT_MEASURE_REGISTRY.get_value_column(measure)
        value_map = data[DATA_KEY].get(cache_var, {})

        if not value_map:
            return pd.DataFrame(columns=self._empty_result_columns(measure, value_column))

        rows = self._build_rows_from_value_map(
            value_map,
            place_info,
            value_column,
            start_year,
            end_year,
            measure,
        )

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
            measure: Logical measure key (for example, 'noon_temperature').
        """
        out_file.parent.mkdir(parents=True, exist_ok=True)
        value_column = DEFAULT_MEASURE_REGISTRY.get_value_column(measure)
        cache_var = DEFAULT_MEASURE_REGISTRY.get_cache_var(measure)
        if value_column not in df.columns:
            raise KeyError(f"Missing required column '{value_column}' for measure '{measure}'")

        grid_lat = float(df['grid_lat'].iloc[0])
        grid_lon = float(df['grid_lon'].iloc[0])

        new_values_by_year = self._build_new_values_by_year(df, value_column)

        existing_data = self._load_existing_data_for_write(out_file, append)

        if append and existing_data is not None:
            try:
                values_by_year = self._merge_values_by_year(existing_data, cache_var, new_values_by_year)
            except Exception as e:
                logger.warning(f"Error merging with existing cache: {e}. Overwriting.")
                values_by_year = new_values_by_year
        else:
            values_by_year = new_values_by_year

        metadata = DEFAULT_MEASURE_REGISTRY.build_variables_metadata(measure=measure)
        data_section = {}
        if append and existing_data is not None:
            metadata = deepcopy(existing_data.get(VARIABLES_KEY, metadata))
            if cache_var not in metadata:
                metadata.update(DEFAULT_MEASURE_REGISTRY.build_variables_metadata(measure=measure))
            data_section = deepcopy(existing_data.get(DATA_KEY, {}))
        data_section[cache_var] = values_by_year

        yaml_data = self._build_yaml_payload(location, grid_lat, grid_lon, metadata, data_section)
        self.cache_codec.write_cache_yaml_v2(yaml_data, out_file)

        logger.info(f"Saved data to {out_file}")
