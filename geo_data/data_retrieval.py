"""Data retrieval orchestration for geo data layer.

Preferred API is class-based (`RetrievalCoordinator`).
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from collections.abc import Callable

import pandas as pd

from geo_core.progress import get_progress_manager
from geo_core.config import load_retrieval_settings

from .cds_base import CDS, Location, ZoneInfo
from .cds_precipitation import PrecipitationCDS
from .cds_solar_radiation import SolarRadiationCDS
from .cds_temperature import TemperatureCDS
from .schema import (
    DATA_KEY,
    NOON_TEMP_VAR,
    SCHEMA_VERSION,
)
from .cache_store import CacheStore

logger = logging.getLogger("geo")

MEASURE_TO_CDS_CLIENT = {
    'noon_temperature': lambda: TemperatureCDS,
    'daily_precipitation': lambda: PrecipitationCDS,
    'daily_solar_radiation_energy': lambda: SolarRadiationCDS,
}

MEASURE_TO_CDS_METHOD = {
    'noon_temperature': 'get_noon_series',
    'daily_precipitation': 'get_daily_precipitation_series',
    'daily_solar_radiation_energy': 'get_daily_solar_radiation_energy_series',
}


def _get_measure_cds_client_class(measure: str) -> type[CDS]:
    """Resolve measure-specific CDS client class for a logical measure name."""
    try:
        return MEASURE_TO_CDS_CLIENT[measure]()
    except KeyError as exc:
        allowed = ', '.join(sorted(MEASURE_TO_CDS_CLIENT.keys()))
        raise ValueError(f"Unsupported measure '{measure}'. Allowed: {allowed}") from exc


def _get_measure_cds_method(measure: str) -> str:
    """Resolve CDS client method name for a logical measure name."""
    try:
        return MEASURE_TO_CDS_METHOD[measure]
    except KeyError as exc:
        allowed = ', '.join(sorted(MEASURE_TO_CDS_METHOD.keys()))
        raise ValueError(f"Unsupported measure '{measure}'. Allowed: {allowed}") from exc


def _create_measure_cds_client(
    measure: str,
    cache_dir: Path,
    progress_mgr,
    config_path: Path,
) -> CDS:
    """Create a measure-specific CDS client backed by the shared CDS implementation."""
    measure_client_class = _get_measure_cds_client_class(measure)
    return measure_client_class(cache_dir=cache_dir, progress_manager=progress_mgr, config_path=config_path)


class RetrievalCoordinator:
    """Coordinate measure-aware CDS retrieval and cache merge workflows."""

    def __init__(
        self,
        cache_dir: Path = Path("era5_cache"),
        data_cache_dir: Path = Path("data_cache"),
        config_path: Path = Path("config.yaml"),
        status_reporter: Callable[[str], None] | None = print,
        fetch_mode_override: str | None = None,
        overwrite_existing_cache_values: bool = False,
    ) -> None:
        if fetch_mode_override not in (None, 'month', 'year'):
            raise ValueError("fetch_mode_override must be one of: None, 'month', 'year'")

        self.cache_dir = cache_dir
        self.data_cache_dir = data_cache_dir
        self.config_path = config_path
        self.status_reporter = status_reporter
        self.fetch_mode_override = fetch_mode_override
        self.overwrite_existing_cache_values = overwrite_existing_cache_values
        self.progress_mgr = get_progress_manager()
        self.cache_store = CacheStore()
        self.cache_store.ensure_cache_summary(self.data_cache_dir)
        retrieval_settings = load_retrieval_settings(self.config_path)
        self.wet_hour_threshold_mm = float(retrieval_settings['wet_hour_threshold_mm'])

    def _apply_fetch_mode_override(self, measure_cds_client: CDS, measure: str) -> None:
        """Apply runtime fetch chunking override to the active measure CDS client."""
        if self.fetch_mode_override is None:
            return

        override_mode = 'monthly' if self.fetch_mode_override == 'month' else 'yearly'
        attribute_by_measure = {
            'noon_temperature': 'temp_fetch_mode',
            'daily_precipitation': 'precipitation_fetch_mode',
            'daily_solar_radiation_energy': 'solar_fetch_mode',
        }

        try:
            attr_name = attribute_by_measure[measure]
        except KeyError as exc:
            allowed = ', '.join(sorted(attribute_by_measure.keys()))
            raise ValueError(f"Unsupported measure '{measure}'. Allowed: {allowed}") from exc

        setattr(measure_cds_client, attr_name, override_mode)

    def _cache_status_for_location(
        self,
        loc: Location,
        requested_years: set[int],
        measure: str,
    ) -> tuple[Path, set[int], list[int]]:
        """Return cache file path, cached years, and missing years for one location."""
        yaml_file = self.cache_store.cache_yaml_path_for_place(self.data_cache_dir, loc.name)
        required_measures = self._required_cache_measures_for_measure(measure)
        cached_year_sets: list[set[int]] = []
        if yaml_file.exists():
            for required_measure in required_measures:
                cached_year_sets.append(
                    self.cache_store.get_cached_years(yaml_file, measure=required_measure)
                )

        if cached_year_sets:
            cached_years = set.intersection(*cached_year_sets)
        else:
            cached_years = set()

        if self.overwrite_existing_cache_values:
            missing_years = sorted(requested_years)
        else:
            missing_years = sorted(requested_years - cached_years)
        return yaml_file, cached_years, missing_years

    @staticmethod
    def _required_cache_measures_for_measure(measure: str) -> tuple[str, ...]:
        """Return cache measures that must be present to consider a year fully cached."""
        if measure == 'daily_precipitation':
            return ('daily_precipitation', 'hourly_precipitation')
        return (measure,)

    @staticmethod
    def _build_daily_wet_hours_from_hourly(
        df_hourly: pd.DataFrame,
        tz_name: str,
        wet_threshold_mm: float = 1.0,
    ) -> pd.DataFrame:
        """Aggregate hourly precipitation rows into local-day wet-hour summaries."""
        if df_hourly.empty:
            return pd.DataFrame(columns=[
                'date',
                'wet_hours_per_day',
                'max_hourly_precip_mm',
                'total_precip_mm',
                'observed_hours',
            ])

        tz_local = ZoneInfo(tz_name)
        hourly = df_hourly.copy()
        hourly['date'] = pd.to_datetime(hourly['date'], utc=True)
        hourly['date_local'] = hourly['date'].dt.tz_convert(tz_local).dt.date

        summary = (
            hourly
            .groupby('date_local', as_index=False)
            .agg(
                wet_hours_per_day=('precip_mm', lambda s: int((s >= wet_threshold_mm).sum())),
                max_hourly_precip_mm=('precip_mm', 'max'),
                total_precip_mm=('precip_mm', 'sum'),
                observed_hours=('precip_mm', 'size'),
            )
            .rename(columns={'date_local': 'date'})
        )
        summary['date'] = pd.to_datetime(summary['date'])
        return summary

    def _enrich_precipitation_with_wet_hours(
        self,
        df_precip: pd.DataFrame,
        place_list: list[Location],
        start_year: int,
        end_year: int,
    ) -> pd.DataFrame:
        """Attach wet-hours-per-day metrics derived from cached hourly precipitation data."""
        if df_precip.empty:
            return df_precip

        daily = df_precip.copy()
        daily['date'] = pd.to_datetime(daily['date'])

        hourly_cache_locations = [
            loc for loc in place_list
            if self.cache_store.cache_yaml_path_for_place(self.data_cache_dir, loc.name).exists()
        ]
        total_hourly_cache_locations = len(hourly_cache_locations)

        wet_daily_frames: list[pd.DataFrame] = []
        for cache_loc_idx, loc in enumerate(hourly_cache_locations, 1):
            yaml_file = self.cache_store.cache_yaml_path_for_place(self.data_cache_dir, loc.name)

            self.progress_mgr.notify_stage_progress(
                "Cache load",
                loc.name,
                cache_loc_idx,
                total_hourly_cache_locations,
                detail="hourly_precipitation",
            )

            df_hourly = self.cache_store.read_data_file(
                yaml_file,
                start_year,
                end_year,
                measure='hourly_precipitation',
            )
            if df_hourly.empty:
                continue

            wet_daily = self._build_daily_wet_hours_from_hourly(
                df_hourly,
                loc.tz,
                wet_threshold_mm=self.wet_hour_threshold_mm,
            )
            if wet_daily.empty:
                continue

            wet_daily['place_name'] = loc.name
            wet_daily_frames.append(wet_daily)

        if total_hourly_cache_locations:
            self.progress_mgr.notify_stage_complete("Cache load")

        if wet_daily_frames:
            wet_daily_all = pd.concat(wet_daily_frames, ignore_index=True)
            daily = daily.merge(
                wet_daily_all,
                on=['place_name', 'date'],
                how='left',
            )
        else:
            daily['wet_hours_per_day'] = 0
            daily['max_hourly_precip_mm'] = 0.0
            daily['total_precip_mm'] = daily['precip_mm']
            daily['observed_hours'] = 0

        daily['wet_hours_per_day'] = daily['wet_hours_per_day'].fillna(0).astype(int)
        daily['observed_hours'] = daily['observed_hours'].fillna(0).astype(int)
        daily['max_hourly_precip_mm'] = daily['max_hourly_precip_mm'].fillna(0.0)
        daily['total_precip_mm'] = daily['total_precip_mm'].fillna(daily['precip_mm'])

        return daily

    def _plan_places_needing_cds(
        self,
        place_list: list[Location],
        requested_years: set[int],
        measure: str,
    ) -> list[str]:
        """Return ordered location names that require CDS retrieval."""
        places_needing_cds: list[str] = []
        for loc in place_list:
            _, _, missing_years = self._cache_status_for_location(loc, requested_years, measure)
            if missing_years:
                places_needing_cds.append(loc.name)
        return places_needing_cds

    def _load_cached_location_data(
        self,
        df_overall: pd.DataFrame,
        loc: Location,
        yaml_file: Path,
        cached_years: set[int],
        start_year: int,
        end_year: int,
        measure: str,
    ) -> pd.DataFrame:
        """Append cached location data for the given logical measure when available."""
        if not cached_years:
            return df_overall

        logger.info(
            f"Loading {loc.name} from cache for {measure} "
            f"(years: {min(cached_years)}-{max(cached_years)})"
        )
        df_cached = self.cache_store.read_data_file(yaml_file, start_year, end_year, measure=measure)
        return pd.concat([df_overall, df_cached], ignore_index=True)

    def _fetch_and_cache_missing_years(
        self,
        loc: Location,
        missing_years: list[int],
        measure: str,
        yaml_file: Path,
        cds_place_num: int,
        total_cds_places: int,
    ) -> pd.DataFrame:
        """Fetch missing years from CDS for one location, append to cache, and return rows."""
        if not missing_years:
            return pd.DataFrame()

        logger.info(f"Fetching {loc.name} from CDS for {len(missing_years)} year(s): {missing_years}")

        self.progress_mgr.notify_location_start(loc.name, cds_place_num, total_cds_places, len(missing_years))

        cds_method_name = _get_measure_cds_method(measure)
        measure_cds_client = _create_measure_cds_client(
            measure,
            self.cache_dir,
            self.progress_mgr,
            self.config_path,
        )
        self._apply_fetch_mode_override(measure_cds_client, measure)
        if not hasattr(measure_cds_client, cds_method_name):
            raise NotImplementedError(
                f"Measure '{measure}' is not implemented by CDS client "
                f"(missing method '{cds_method_name}')."
            )
        cds_method = getattr(measure_cds_client, cds_method_name)

        df_new = pd.DataFrame()
        for year_idx, year in enumerate(missing_years, 1):
            start_d = date(year, 1, 1)
            end_d = date(year, 12, 31)

            self.progress_mgr.notify_year_start(loc.name, year, year_idx, len(missing_years))

            logger.info(f"  Retrieving {year} for {loc.name}...")
            cds_kwargs = {"notify_progress": False}
            if measure in ('daily_precipitation', 'daily_solar_radiation_energy'):
                cds_kwargs['notify_month_progress'] = True
            df_year = cds_method(loc, start_d, end_d, **cds_kwargs)

            self.cache_store.save_data_file(
                df_year,
                yaml_file,
                loc,
                append=True,
                measure=measure,
                overwrite_existing_values=self.overwrite_existing_cache_values,
            )

            # Keep the in-place CLI progress indicator visible after cache write/log output.
            self.progress_mgr.notify_year_start(loc.name, year, year_idx, len(missing_years))

            if measure == 'daily_precipitation':
                self._update_hourly_precipitation_cache(
                    measure_cds_client,
                    loc,
                    yaml_file,
                    start_d,
                    end_d,
                )
                # Hourly cache updates can emit logs; redraw active progress line afterwards.
                self.progress_mgr.notify_year_start(loc.name, year, year_idx, len(missing_years))

            df_new = pd.concat([df_new, df_year], ignore_index=True)

            self.progress_mgr.notify_year_complete(loc.name, year, year_idx, len(missing_years))

        self.progress_mgr.notify_location_complete(loc.name)
        return df_new

    def _update_hourly_precipitation_cache(
        self,
        measure_cds_client: CDS,
        loc: Location,
        yaml_file: Path,
        start_d: date,
        end_d: date,
    ) -> None:
        """Update hourly precipitation cache for a CDS-fetched daily-precipitation span."""
        if not hasattr(measure_cds_client, 'get_hourly_precipitation_series'):
            raise NotImplementedError(
                "daily_precipitation client is missing method 'get_hourly_precipitation_series'"
            )

        df_hourly = measure_cds_client.get_hourly_precipitation_series(
            loc,
            start_d,
            end_d,
            notify_progress=False,
        )
        if df_hourly is None or df_hourly.empty:
            logger.warning(
                "Hourly precipitation cache update yielded no rows for %s (%s..%s)",
                loc.name,
                start_d.isoformat(),
                end_d.isoformat(),
            )
            return

        self.cache_store.save_data_file(
            df_hourly,
            yaml_file,
            loc,
            append=True,
            measure='hourly_precipitation',
            overwrite_existing_values=self.overwrite_existing_cache_values,
        )

    def retrieve(
        self,
        place_list: list[Location],
        start_year: int,
        end_year: int,
        measure: str = 'noon_temperature',
    ) -> pd.DataFrame:
        """Retrieve measure data for all places and concatenate into one DataFrame."""
        df_overall = pd.DataFrame()
        requested_years = set(range(start_year, end_year + 1))

        location_cache_status = []
        places_needing_cds: list[str] = []
        total_cache_load_locations = 0
        for loc in place_list:
            yaml_file, cached_years, missing_years = self._cache_status_for_location(
                loc,
                requested_years,
                measure,
            )
            location_cache_status.append((loc, yaml_file, cached_years, missing_years))
            if missing_years:
                places_needing_cds.append(loc.name)
            if cached_years and not (self.overwrite_existing_cache_values and missing_years):
                total_cache_load_locations += 1

        summary_text = format_retrieval_summary(places_needing_cds, measure)
        if self.status_reporter:
            self.status_reporter(summary_text)

        cds_place_num = 0
        total_cds_places = len(places_needing_cds)
        cache_load_idx = 0

        for loc, yaml_file, cached_years, missing_years in location_cache_status:

            if not (self.overwrite_existing_cache_values and missing_years):
                if cached_years:
                    cache_load_idx += 1
                    self.progress_mgr.notify_stage_progress(
                        "Cache load",
                        loc.name,
                        cache_load_idx,
                        total_cache_load_locations,
                        detail=f"{measure} ({min(cached_years)}-{max(cached_years)})",
                    )
                df_overall = self._load_cached_location_data(
                    df_overall,
                    loc,
                    yaml_file,
                    cached_years,
                    start_year,
                    end_year,
                    measure,
                )

            if missing_years:
                cds_place_num += 1
                df_new = self._fetch_and_cache_missing_years(
                    loc,
                    missing_years,
                    measure,
                    yaml_file,
                    cds_place_num,
                    total_cds_places,
                )
                df_overall = pd.concat([df_overall, df_new], ignore_index=True)

        if total_cache_load_locations:
            self.progress_mgr.notify_stage_complete("Cache load")

        if measure == 'daily_precipitation' and not df_overall.empty:
            df_overall = self._enrich_precipitation_with_wet_hours(
                df_overall,
                place_list,
                start_year,
                end_year,
            )

        if not df_overall.empty:
            df_overall['date'] = pd.to_datetime(df_overall['date'])
        return df_overall


def format_retrieval_summary(places_needing_cds: list[str], measure: str) -> str:
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

    return f"CDS retrieval: none needed ({measure}; all data already cached)"


__all__ = [
    "CDS",
    "Location",
    "TemperatureCDS",
    "PrecipitationCDS",
    "SolarRadiationCDS",
    "CacheStore",
    "SCHEMA_VERSION",
    "DATA_KEY",
    "NOON_TEMP_VAR",
    "format_retrieval_summary",
    "RetrievalCoordinator",
]
