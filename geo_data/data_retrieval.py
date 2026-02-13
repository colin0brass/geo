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

from .cds_base import CDS, Location
from .cds_precipitation import PrecipitationCDS
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
}

MEASURE_TO_CDS_METHOD = {
    'noon_temperature': 'get_noon_series',
    'daily_precipitation': 'get_daily_precipitation_series',
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
    ) -> None:
        self.cache_dir = cache_dir
        self.data_cache_dir = data_cache_dir
        self.config_path = config_path
        self.status_reporter = status_reporter
        self.progress_mgr = get_progress_manager()
        self.cache_store = CacheStore()

    def _cache_status_for_location(
        self,
        loc: Location,
        requested_years: set[int],
        measure: str,
    ) -> tuple[Path, set[int], list[int]]:
        """Return cache file path, cached years, and missing years for one location."""
        yaml_file = self.cache_store.cache_yaml_path_for_place(self.data_cache_dir, loc.name)
        cached_years = set()
        if yaml_file.exists():
            cached_years = self.cache_store.get_cached_years(yaml_file, measure=measure)
        missing_years = sorted(requested_years - cached_years)
        return yaml_file, cached_years, missing_years

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
            df_year = cds_method(loc, start_d, end_d, notify_progress=False)

            self.cache_store.save_data_file(df_year, yaml_file, loc, append=True, measure=measure)
            df_new = pd.concat([df_new, df_year], ignore_index=True)

            self.progress_mgr.notify_year_complete(loc.name, year, year_idx, len(missing_years))

        self.progress_mgr.notify_location_complete(loc.name)
        return df_new

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

        places_needing_cds = self._plan_places_needing_cds(place_list, requested_years, measure)

        summary_text = format_retrieval_summary(places_needing_cds)
        if self.status_reporter:
            self.status_reporter(summary_text)

        cds_place_num = 0
        total_cds_places = len(places_needing_cds)

        for loc in place_list:
            yaml_file, cached_years, missing_years = self._cache_status_for_location(
                loc,
                requested_years,
                measure,
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

        if not df_overall.empty:
            df_overall['date'] = pd.to_datetime(df_overall['date'])
        return df_overall


def format_retrieval_summary(places_needing_cds: list[str]) -> str:
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


__all__ = [
    "CDS",
    "Location",
    "TemperatureCDS",
    "PrecipitationCDS",
    "CacheStore",
    "SCHEMA_VERSION",
    "DATA_KEY",
    "NOON_TEMP_VAR",
    "format_retrieval_summary",
    "RetrievalCoordinator",
]
