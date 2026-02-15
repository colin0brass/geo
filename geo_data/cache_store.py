"""Cache path and YAML I/O helpers for geo data documents."""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from .cds_base import Location
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

US_STATE_CODES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC',
}


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
        is_hourly_precip = measure == 'hourly_precipitation'
        for year_str, months in value_map.items():
            year = int(year_str)

            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue

            for month_str, days in months.items():
                month = int(month_str)
                for day_str, day_payload in days.items():
                    day = int(day_str)
                    if is_hourly_precip:
                        if not isinstance(day_payload, dict):
                            continue
                        for hour_str, value in day_payload.items():
                            hour = int(hour_str)
                            date_obj = datetime(year, month, day, hour)
                            row = {
                                'date': date_obj,
                                value_column: value,
                                'place_name': place_info['name'],
                                'grid_lat': place_info['grid_lat'],
                                'grid_lon': place_info['grid_lon'],
                            }
                            rows.append(row)
                        continue

                    date_obj = datetime(year, month, day)
                    row = {
                        'date': date_obj,
                        value_column: day_payload,
                        'place_name': place_info['name'],
                        'grid_lat': place_info['grid_lat'],
                        'grid_lon': place_info['grid_lon'],
                    }
                    if measure == 'noon_temperature':
                        temp_c = float(day_payload)
                        row['temp_F'] = temp_c * 9.0 / 5.0 + 32.0
                    rows.append(row)
        return rows

    @staticmethod
    def _normalize_value_map_for_measure(measure: str, value_map: dict) -> dict:
        """Normalize cached value map keys/types for a measure, handling legacy nested payloads."""
        normalized: dict[int, dict[int, dict[int, float | dict[int, float]]]] = {}
        is_hourly_precip = measure == 'hourly_precipitation'

        for year_key, months in value_map.items():
            year_int = int(year_key)
            if year_int not in normalized:
                normalized[year_int] = {}

            for month_key, days in months.items():
                month_int = int(month_key)
                if month_int not in normalized[year_int]:
                    normalized[year_int][month_int] = {}

                for day_key, raw_payload in days.items():
                    day_int = int(day_key)

                    if is_hourly_precip:
                        if isinstance(raw_payload, dict):
                            normalized[year_int][month_int][day_int] = {
                                int(hour_key): float(hour_value)
                                for hour_key, hour_value in raw_payload.items()
                            }
                        else:
                            normalized[year_int][month_int][day_int] = {0: float(raw_payload)}
                        continue

                    if isinstance(raw_payload, dict):
                        numeric_values = [float(value) for value in raw_payload.values()]
                        if not numeric_values:
                            continue
                        if measure in ('daily_precipitation', 'daily_solar_radiation_energy'):
                            normalized[year_int][month_int][day_int] = float(sum(numeric_values))
                        else:
                            normalized[year_int][month_int][day_int] = float(numeric_values[0])
                    else:
                        normalized[year_int][month_int][day_int] = float(raw_payload)

        return normalized

    @staticmethod
    def _build_new_values_by_year(
        df: pd.DataFrame,
        value_column: str,
        measure: str,
        round_decimals: int,
    ) -> dict:
        """Build nested year/month/day map from a value DataFrame."""
        new_values_by_year = {}
        is_hourly_precip = measure == 'hourly_precipitation'
        for _, row in df.iterrows():
            date_obj = pd.to_datetime(row['date'])
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            value = round(float(row[value_column]), round_decimals)

            if year not in new_values_by_year:
                new_values_by_year[year] = {}
            if month not in new_values_by_year[year]:
                new_values_by_year[year][month] = {}
            if is_hourly_precip:
                hour = date_obj.hour
                if day not in new_values_by_year[year][month]:
                    new_values_by_year[year][month][day] = {}
                new_values_by_year[year][month][day][hour] = value
            else:
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
    def _merge_values_by_year(
        existing_data: dict,
        cache_var: str,
        new_values_by_year: dict,
        measure: str,
        overwrite_existing_values: bool = False,
    ) -> dict:
        """Merge existing nested values with new year/month/day values."""
        normalized_existing = CacheStore._normalize_value_map_for_measure(
            measure,
            existing_data[DATA_KEY].get(cache_var, {}),
        )

        for year, months in new_values_by_year.items():
            if year not in normalized_existing:
                normalized_existing[year] = {}
            for month, days in months.items():
                if month not in normalized_existing[year]:
                    normalized_existing[year][month] = {}
                for day, temp in days.items():
                    if overwrite_existing_values or day not in normalized_existing[year][month]:
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

    @staticmethod
    def cache_summary_path(data_cache_dir: Path) -> Path:
        """Return path for the cache summary index file within a data cache directory."""
        return data_cache_dir / "cache_summary.yaml"

    @staticmethod
    def _empty_cache_summary() -> dict:
        """Return default empty cache summary document."""
        return {
            'summary_version': 1,
            'updated_at': None,
            'files': {},
        }

    def _load_cache_summary(self, summary_file: Path) -> dict:
        """Load summary index document, returning empty structure when unavailable/invalid."""
        if not summary_file.exists():
            return self._empty_cache_summary()

        try:
            with open(summary_file, 'r') as f:
                summary = yaml.safe_load(f) or {}
        except Exception as exc:
            logger.warning(f"Failed to read cache summary '{summary_file}': {exc}")
            return self._empty_cache_summary()

        if not isinstance(summary, dict):
            return self._empty_cache_summary()

        files = summary.get('files')
        if not isinstance(files, dict):
            summary['files'] = {}

        if 'summary_version' not in summary:
            summary['summary_version'] = 1

        return summary

    @staticmethod
    def _prepare_compact_summary_payload(summary: dict) -> dict:
        """Return a summary payload with inline `years` lists for compact YAML output."""
        class InlineList(list):
            """List marker type represented in YAML flow style."""

        class InlineDict(dict):
            """Mapping marker type represented in YAML flow style."""

        class SummaryDumper(yaml.SafeDumper):
            """YAML dumper with custom inline-list representation."""

        def _represent_inline_list(dumper, data):
            return dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True)

        def _represent_inline_dict(dumper, data):
            return dumper.represent_mapping('tag:yaml.org,2002:map', data, flow_style=True)

        SummaryDumper.add_representer(InlineList, _represent_inline_list)
        SummaryDumper.add_representer(InlineDict, _represent_inline_dict)

        def _convert(node):
            if isinstance(node, dict):
                converted = {}
                for key, value in node.items():
                    if key == 'year_ranges' and isinstance(value, list):
                        converted[key] = InlineList(value)
                    else:
                        converted[key] = _convert(value)
                if set(converted.keys()) == {'year_ranges'}:
                    return InlineDict(converted)
                return converted
            if isinstance(node, list):
                return [_convert(value) for value in node]
            return node

        converted_payload = _convert(summary)
        converted_payload['_yaml_dumper'] = SummaryDumper
        return converted_payload

    @staticmethod
    def _write_cache_summary(summary_file: Path, summary: dict) -> None:
        """Persist cache summary index to disk in YAML format."""
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        payload = deepcopy(summary)
        payload['summary_version'] = 1
        payload['updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        compact_payload = CacheStore._prepare_compact_summary_payload(payload)
        dumper = compact_payload.pop('_yaml_dumper')
        with open(summary_file, 'w') as f:
            yaml.dump(
                compact_payload,
                f,
                Dumper=dumper,
                sort_keys=False,
                allow_unicode=True,
                default_flow_style=False,
            )

    @staticmethod
    def _country_from_place_name(place_name: str) -> str:
        """Best-effort country extraction from place display name."""
        if not isinstance(place_name, str):
            return ""
        if ',' not in place_name:
            return ""
        suffix = place_name.split(',')[-1].strip()
        if suffix.upper() in US_STATE_CODES:
            return 'USA'
        return suffix

    @classmethod
    def _count_nested_values(cls, node) -> int:
        """Count scalar leaf values in nested mapping structures."""
        if isinstance(node, dict):
            return sum(cls._count_nested_values(child) for child in node.values())
        return 1

    @classmethod
    def _compress_years_to_ranges(cls, years: list[int]) -> list[str]:
        """Compress sorted year list into contiguous range strings."""
        if not years:
            return []

        unique_years = sorted(set(int(year) for year in years))
        ranges: list[str] = []
        start = unique_years[0]
        end = unique_years[0]

        for year in unique_years[1:]:
            if year == end + 1:
                end = year
                continue

            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = year
            end = year

        if start == end:
            ranges.append(str(start))
        else:
            ranges.append(f"{start}-{end}")
        return ranges

    @classmethod
    def _expand_year_ranges(cls, year_ranges: list[object]) -> set[int]:
        """Expand range strings (e.g., 1970-1999) into a set of years."""
        years: set[int] = set()
        for entry in year_ranges:
            if isinstance(entry, int):
                years.add(int(entry))
                continue
            if not isinstance(entry, str):
                continue

            token = entry.strip()
            if not token:
                continue

            if '-' in token:
                parts = token.split('-', 1)
                if len(parts) != 2:
                    continue
                try:
                    start = int(parts[0])
                    end = int(parts[1])
                except ValueError:
                    continue
                if start > end:
                    continue
                years.update(range(start, end + 1))
                continue

            try:
                years.add(int(token))
            except ValueError:
                continue
        return years

    @classmethod
    def _build_measure_summary_entry(cls, value_map: dict) -> dict:
        """Build summary metadata for one logical measure value map."""
        if not isinstance(value_map, dict) or not value_map:
            return {
                'year_ranges': [],
            }

        years = sorted(int(year) for year in value_map.keys())
        return {
            'year_ranges': cls._compress_years_to_ranges(years),
        }

    def _build_summary_entry_from_payload(self, cache_data: dict) -> dict:
        """Build one summary-file entry from a v2 cache payload."""
        place = cache_data.get('place', {}) if isinstance(cache_data, dict) else {}
        place_name = str(place.get('name', ''))
        data_section = cache_data.get(DATA_KEY, {}) if isinstance(cache_data, dict) else {}
        measures: dict[str, dict] = {}
        for measure_name, cache_var_name in DEFAULT_MEASURE_REGISTRY.cache_var_by_measure.items():
            value_map = {}
            if isinstance(data_section, dict):
                value_map = data_section.get(cache_var_name, {})
            measures[measure_name] = self._build_measure_summary_entry(value_map)

        return {
            'place_name': place_name,
            'country': self._country_from_place_name(place_name),
            'measures': measures,
        }

    def _update_cache_summary_from_payload(self, cache_file: Path, cache_data: dict) -> None:
        """Upsert one file entry in cache summary using current payload state."""
        summary_file = self.cache_summary_path(cache_file.parent)
        summary = self._load_cache_summary(summary_file)
        files = summary.setdefault('files', {})
        files[cache_file.name] = self._build_summary_entry_from_payload(cache_data)
        self._write_cache_summary(summary_file, summary)

    def ensure_cache_summary(self, data_cache_dir: Path) -> None:
        """Build cache summary index when missing by scanning existing cache files once."""
        summary_file = self.cache_summary_path(data_cache_dir)
        if summary_file.exists():
            return

        summary = self._empty_cache_summary()
        files = summary.setdefault('files', {})
        for yaml_file in sorted(data_cache_dir.glob('*.yaml')):
            if yaml_file.name == summary_file.name:
                continue
            try:
                cache_data = self.cache_codec.load_cache_data_v2(yaml_file, auto_migrate=True)
                files[yaml_file.name] = self._build_summary_entry_from_payload(cache_data)
            except Exception as exc:
                logger.warning(f"Skipping cache summary entry for {yaml_file}: {exc}")

        self._write_cache_summary(summary_file, summary)

    def rebuild_cache_summary(self, data_cache_dir: Path) -> None:
        """Force rebuild of cache summary index by deleting and re-scanning cache files."""
        summary_file = self.cache_summary_path(data_cache_dir)
        if summary_file.exists():
            summary_file.unlink()
        self.ensure_cache_summary(data_cache_dir)

    def get_cache_summary(self, data_cache_dir: Path, rebuild: bool = False) -> dict:
        """Return cache summary index, creating (or rebuilding) it when requested."""
        if rebuild:
            self.rebuild_cache_summary(data_cache_dir)
        else:
            self.ensure_cache_summary(data_cache_dir)

        summary_file = self.cache_summary_path(data_cache_dir)
        return self._load_cache_summary(summary_file)

    def _get_cached_years_from_summary(self, yaml_file: Path, measure: str) -> set[int]:
        """Return cached years from summary index for one file+measure, if available."""
        summary_file = self.cache_summary_path(yaml_file.parent)
        summary = self._load_cache_summary(summary_file)
        files = summary.get('files', {})
        if not isinstance(files, dict):
            return set()

        file_entry = files.get(yaml_file.name)
        if not isinstance(file_entry, dict):
            return set()

        measures = file_entry.get('measures', {})
        if not isinstance(measures, dict):
            return set()

        measure_entry = measures.get(measure)
        if not isinstance(measure_entry, dict):
            return set()

        # New compact format
        year_ranges = measure_entry.get('year_ranges', [])
        if isinstance(year_ranges, list) and year_ranges:
            expanded = self._expand_year_ranges(year_ranges)
            if expanded:
                return expanded

        # Backward-compatibility with older summary versions
        years = measure_entry.get('years', [])
        if isinstance(years, list) and years:
            return {int(year) for year in years}

        return set()

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

            years_from_summary = self._get_cached_years_from_summary(yaml_file, measure)
            if years_from_summary:
                return years_from_summary

            data = self.cache_codec.load_cache_data_v2(yaml_file, auto_migrate=True)
            self._update_cache_summary_from_payload(yaml_file, data)
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
        overwrite_existing_values: bool = False,
    ) -> None:
        """
        Save a DataFrame to a YAML file with hierarchical structure.

        Args:
            df: DataFrame to save.
            out_file: Output file path (.yaml extension).
            location: Location object with place metadata.
            append: If True, merge with existing file; if False, overwrite.
            measure: Logical measure key (for example, 'noon_temperature').
            overwrite_existing_values: If True (and append=True), replace existing
                date values when new data includes the same date; if False, keep
                existing cached values for conflicting dates.
        """
        out_file.parent.mkdir(parents=True, exist_ok=True)
        value_column = DEFAULT_MEASURE_REGISTRY.get_value_column(measure)
        cache_var = DEFAULT_MEASURE_REGISTRY.get_cache_var(measure)
        if value_column not in df.columns:
            raise KeyError(f"Missing required column '{value_column}' for measure '{measure}'")

        grid_lat = float(df['grid_lat'].iloc[0])
        grid_lon = float(df['grid_lon'].iloc[0])

        round_decimals = 3 if measure == 'hourly_precipitation' else 2
        new_values_by_year = self._build_new_values_by_year(
            df,
            value_column,
            measure,
            round_decimals,
        )

        existing_data = self._load_existing_data_for_write(out_file, append)

        if append and existing_data is not None:
            try:
                values_by_year = self._merge_values_by_year(
                    existing_data,
                    cache_var,
                    new_values_by_year,
                    measure,
                    overwrite_existing_values=overwrite_existing_values,
                )
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

        cache_var_to_measure = {
            cache_var_name: measure_name
            for measure_name, cache_var_name in DEFAULT_MEASURE_REGISTRY.cache_var_by_measure.items()
        }
        for data_key_name in data_section.keys():
            if data_key_name in metadata:
                continue
            measure_name = cache_var_to_measure.get(data_key_name)
            if measure_name is None:
                continue
            metadata.update(DEFAULT_MEASURE_REGISTRY.build_variables_metadata(measure=measure_name))

        yaml_data = self._build_yaml_payload(location, grid_lat, grid_lon, metadata, data_section)
        self.cache_codec.write_cache_yaml_v2(yaml_data, out_file)
        self._update_cache_summary_from_payload(out_file, yaml_data)

        logger.info(f"Saved data to {out_file}")
