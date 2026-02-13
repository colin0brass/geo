from __future__ import annotations

import sys
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

from geo_core.config import load_retrieval_settings

logger = logging.getLogger("geo")

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    logger.error("This script requires Python 3.9+ (zoneinfo).")
    sys.exit(1)

try:
    from timezonefinder import TimezoneFinder
    _tf = TimezoneFinder()
except ImportError:
    logger.warning("timezonefinder not installed. Timezone must be specified manually.")
    _tf = None


@dataclass
class Location:
    """
    Represents a geographic location with a name, latitude, longitude, and timezone.

    Timezone is automatically determined from coordinates if not provided.

    Attributes:
        name: Name of the location.
        lat: Latitude.
        lon: Longitude.
        tz: Timezone string (IANA format). Auto-detected from lat/lon if None.
    """

    name: str
    lat: float
    lon: float
    tz: str | None = None

    def __post_init__(self):
        """Automatically detect timezone from coordinates if not provided."""
        if self.tz is None:
            if _tf is not None:
                detected_tz = _tf.timezone_at(lat=self.lat, lng=self.lon)
                if detected_tz:
                    object.__setattr__(self, 'tz', detected_tz)
                    logger.debug(f"Auto-detected timezone for {self.name}: {detected_tz}")
                else:
                    logger.error(f"Could not determine timezone for {self.name} at ({self.lat}, {self.lon})")
                    raise ValueError(f"Unable to determine timezone for coordinates ({self.lat}, {self.lon})")
            else:
                logger.error(f"Timezone not provided for {self.name} and timezonefinder not available")
                raise ValueError("Timezone must be specified when timezonefinder is not installed")


class CDS:
    """
    Client for downloading and processing ERA5 reanalysis data from the CDS API.
    Handles data retrieval, caching, and conversion to pandas/xarray objects.
    """

    def __init__(
        self,
        cache_dir: Path = Path("era5_cache"),
        progress_manager=None,
        config_path: Path = Path("config.yaml"),
    ) -> None:
        """
        Initialize the CDS client and set the cache directory.

        Args:
            cache_dir: Directory to cache downloaded NetCDF files.
            progress_manager: Optional ProgressManager for progress reporting.
        """
        self.client = cdsapi.Client(quiet=True, debug=False)
        self.cache_dir = cache_dir
        self.progress_manager = progress_manager

        retrieval_settings = load_retrieval_settings(config_path)
        self.default_half_box_deg = float(retrieval_settings["half_box_deg"])
        self.max_nearest_time_delta = pd.Timedelta(
            minutes=int(retrieval_settings["max_nearest_time_delta_minutes"])
        )
        self.month_fetch_day_span_threshold = int(retrieval_settings["month_fetch_day_span_threshold"])

        cdsapi_logger = logging.getLogger('cdsapi')
        cdsapi_logger.handlers.clear()
        cdsapi_logger.setLevel(logging.WARNING)
        cdsapi_logger.propagate = False

    def _resolve_half_box_deg(self, half_box_deg: float | None) -> float:
        """Resolve retrieval box size from explicit arg or configured default."""
        return self.default_half_box_deg if half_box_deg is None else float(half_box_deg)

    @staticmethod
    def _safe_location_name(location: Location) -> str:
        """Create a filesystem-safe location name for cache files."""
        return location.name.replace(' ', '_').replace(',', '')

    @staticmethod
    def _build_local_noon_timestamps(
        start_d: date,
        end_d: date,
        tz_local: ZoneInfo,
    ) -> tuple[list[datetime], pd.DatetimeIndex]:
        """Build local-noon timestamps and their UTC equivalents for an inclusive date range."""
        days = pd.date_range(start=start_d, end=end_d, freq="D")
        local_noons = [
            datetime(day.year, day.month, day.day, 12, 0, 0, tzinfo=tz_local)
            for day in days.to_pydatetime()
        ]
        noon_utc = pd.DatetimeIndex([dt.astimezone(timezone.utc) for dt in local_noons])
        return local_noons, noon_utc

    @staticmethod
    def _select_location_point(ds: xr.Dataset, location: Location) -> xr.Dataset:
        """Select the nearest grid point for a location and normalize time coordinate naming."""
        ds_point = ds.sel(
            latitude=location.lat,
            longitude=location.lon,
            method="nearest",
        )

        if "valid_time" in ds_point.coords:
            ds_point = ds_point.rename({"valid_time": "time"})

        return ds_point

    @staticmethod
    def _build_noon_temperature_dataframe(
        da: xr.DataArray,
        noon_utc: pd.DatetimeIndex,
        local_noons: list[datetime],
        max_nearest_time_delta: pd.Timedelta,
        grid_lat: float,
        grid_lon: float,
        place_name: str,
    ) -> pd.DataFrame:
        """Build daily local-noon temperature output dataframe from an ERA5 data array."""
        selected = da.sel(time=noon_utc.tz_convert(None), method="nearest")
        selected_times = pd.to_datetime(selected["time"].values).tz_localize("UTC")
        delta = np.abs(selected_times - noon_utc)
        if (delta > max_nearest_time_delta).any():
            bad = delta[delta > max_nearest_time_delta]
            raise RuntimeError(
                f"Some noon selections were more than {max_nearest_time_delta} away from requested local noon UTC.\n"
                f"Examples:\n{bad[:5]}"
            )

        temp_k = selected.values.astype(np.float64)
        temp_c = temp_k - 273.15
        temp_f = temp_c * 9.0 / 5.0 + 32.0
        return pd.DataFrame(
            {
                "date": [dt.date().isoformat() for dt in local_noons],
                "local_noon": [dt.isoformat() for dt in local_noons],
                "utc_time_used": [dt.isoformat() for dt in noon_utc],
                "temp_C": np.round(temp_c, 3),
                "temp_F": np.round(temp_f, 3),
                "grid_lat": grid_lat,
                "grid_lon": grid_lon,
                "place_name": place_name,
            }
        )

    @staticmethod
    def _build_daily_precipitation_dataframe(
        da: xr.DataArray,
        tz_local: ZoneInfo,
        grid_lat: float,
        grid_lon: float,
        place_name: str,
    ) -> pd.DataFrame:
        """Build daily local-date precipitation totals in millimeters from an ERA5 data array."""
        utc_times = pd.to_datetime(da["time"].values, utc=True)
        local_dates = utc_times.tz_convert(tz_local).date
        precip_mm = da.values.astype(np.float64) * 1000.0

        df_daily = pd.DataFrame({
            "date": local_dates,
            "precip_mm": precip_mm,
        })
        df_daily = df_daily.groupby("date", as_index=False, sort=True)["precip_mm"].sum()
        df_daily["grid_lat"] = grid_lat
        df_daily["grid_lon"] = grid_lon
        df_daily["place_name"] = place_name
        return df_daily

    def _cds_retrieve_era5_month(
        self,
        out_nc: Path,
        year: int,
        month: int,
        loc: Location,
        day: int = None,
        hour: int = None,
        variable: str = "2m_temperature",
        half_box_deg: float | None = None,
    ) -> Path:
        """
        Download ERA5 data for a specific month and location, or use cached file if available.
        Args:
            out_nc (Path): Output NetCDF file path.
            year (int): Year.
            month (int): Month.
            loc (Location): Location object.
            day (int, optional): Specific day. Defaults to None (all days).
            hour (int, optional): Specific hour. Defaults to None (all hours).
            half_box_deg (float, optional): Half-size of the grid box in degrees. Defaults to retrieval.half_box_deg from config.
        Returns:
            Path: Path to the downloaded or cached NetCDF file.
        """
        out_nc.parent.mkdir(parents=True, exist_ok=True)

        resolved_half_box_deg = self._resolve_half_box_deg(half_box_deg)
        north = loc.lat + resolved_half_box_deg
        south = loc.lat - resolved_half_box_deg
        west = loc.lon - resolved_half_box_deg
        east = loc.lon + resolved_half_box_deg

        day_range = [f"{d:02d}" for d in range(1, 32)] if day is None else [f"{day:02d}"]
        time_range = [f"{h:02d}:00" for h in range(0, 24)] if hour is None else [f"{hour:02d}:00"]

        request = {
            "product_type": "reanalysis",
            "variable": [variable],
            "year": f"{year:04d}",
            "month": f"{month:02d}",
            "day": day_range,
            "time": time_range,
            "area": [north, west, south, east],
            "format": "netcdf",
        }

        if out_nc.exists() and out_nc.stat().st_size > 0:
            return out_nc

        logger.info(f"Downloading ERA5 {year:04d}-{month:02d} to {out_nc} ...")
        self.client.retrieve("reanalysis-era5-single-levels", request, str(out_nc))
        return out_nc

    def _cds_retrieve_era5_date_series(
        self,
        out_nc: Path,
        loc: Location,
        dates_utc: pd.DatetimeIndex,
        variable: str = "2m_temperature",
        half_box_deg: float | None = None,
    ) -> Path:
        """
        Download ERA5 data for a series of UTC dates for a location, or use cached file if available.
        Args:
            out_nc (Path): Output NetCDF file path.
            loc (Location): Location object.
            dates_utc (pd.DatetimeIndex): UTC datetime index for retrieval.
            half_box_deg (float, optional): Half-size of the grid box in degrees. Defaults to retrieval.half_box_deg from config.
        Returns:
            Path: Path to the downloaded or cached NetCDF file.
        """
        out_nc.parent.mkdir(parents=True, exist_ok=True)

        resolved_half_box_deg = self._resolve_half_box_deg(half_box_deg)
        north = loc.lat + resolved_half_box_deg
        south = loc.lat - resolved_half_box_deg
        west = loc.lon - resolved_half_box_deg
        east = loc.lon + resolved_half_box_deg

        years = sorted(set(d.year for d in dates_utc))
        months = sorted(set(f"{d.month:02d}" for d in dates_utc))
        days = sorted(set(f"{d.day:02d}" for d in dates_utc))
        times = sorted(set(f"{d.hour:02d}:00" for d in dates_utc))

        request = {
            "product_type": "reanalysis",
            "variable": [variable],
            "year": [f"{y:04d}" for y in years],
            "month": months,
            "day": days,
            "time": times,
            "area": [north, west, south, east],
            "format": "netcdf",
        }

        if out_nc.exists() and out_nc.stat().st_size > 0:
            return out_nc

        logger.info(f"Downloading ERA5 date series to {out_nc} ...")
        self.client.retrieve("reanalysis-era5-single-levels", request, str(out_nc))
        return out_nc

    def _open_and_concat_for_var(self, nc_files: list[Path], expected_var: str) -> xr.Dataset:
        """
        Open multiple NetCDF files and concatenate along the time dimension.
        Args:
            nc_files (list[Path]): List of NetCDF file paths.
        Returns:
            xr.Dataset: Concatenated xarray Dataset.
        Raises:
            ValueError: If no files are provided.
            KeyError: If expected variable is not found.
        """
        if not nc_files:
            raise ValueError("No NetCDF files to open.")

        ds = xr.open_mfdataset(
            [str(p) for p in nc_files],
            combine="by_coords",
            parallel=False,
            engine="netcdf4",
        )

        if expected_var not in ds.data_vars:
            raise KeyError(
                f"Expected variable '{expected_var}' not found. Variables: {list(ds.data_vars)}"
            )

        return ds

    def get_month_data(
        self,
        location: Location,
        year: int,
        month: int,
        day: int = None,
        hour: int = None,
        half_box_deg: float | None = None,
    ) -> xr.Dataset:
        """
        Retrieve and return an xarray Dataset of ERA5 data for a given month and location.
        Args:
            location (Location): Location object.
            year (int): Year.
            month (int): Month.
            day (int, optional): Specific day. Defaults to None.
            hour (int, optional): Specific hour. Defaults to None.
            half_box_deg (float, optional): Half-size of the grid box in degrees. Defaults to retrieval.half_box_deg from config.
        Returns:
            xr.Dataset: ERA5 data for the specified month and location.
        """
        safe_name = self._safe_location_name(location)
        cache_file = self.cache_dir / f"era5_t2m_{safe_name}_{year:04d}_{month:02d}.nc"
        nc_file = self._cds_retrieve_era5_month(
            cache_file,
            year=year,
            month=month,
            loc=location,
            day=day,
            hour=hour,
            half_box_deg=half_box_deg,
        )
        return self._open_and_concat_for_var([nc_file], "t2m")

    def _month_range(self, start_d: date, end_d: date):
        """
        Yield (year, month) pairs covering [start_d, end_d].
        Args:
            start_d (date): Start date.
            end_d (date): End date.
        Yields:
            Tuple[int, int]: (year, month) pairs.
        """
        y, m = start_d.year, start_d.month
        while (y, m) <= (end_d.year, end_d.month):
            yield y, m
            if m == 12:
                y, m = y + 1, 1
            else:
                m += 1

    @staticmethod
    def _empty_series_frame(columns: list[str]) -> pd.DataFrame:
        """Create an empty DataFrame with the provided column order."""
        return pd.DataFrame(columns=columns)

    def _collect_period_frames(
        self,
        location: Location,
        periods: list[tuple[int, int | None]],
        fetch_period_df,
        notify_progress: bool,
    ) -> list[pd.DataFrame]:
        """Collect per-period DataFrames with shared progress notifications."""
        all_dfs: list[pd.DataFrame] = []
        total_periods = len(periods)

        for period_idx, period in enumerate(periods, 1):
            period_year = period[0]
            if self.progress_manager and notify_progress:
                self.progress_manager.notify_year_start(location.name, period_year, period_idx, total_periods)

            all_dfs.append(fetch_period_df(period))

            if self.progress_manager and notify_progress:
                self.progress_manager.notify_year_complete(location.name, period_year, period_idx, total_periods)

        return all_dfs

    def _finalize_series_dataframe(
        self,
        all_dfs: list[pd.DataFrame],
        start_d: date,
        end_d: date,
        empty_columns: list[str],
        *,
        date_as_string: bool = False,
        round_column: str | None = None,
        round_decimals: int = 3,
    ) -> pd.DataFrame:
        """Concatenate period frames, filter to date range, and normalize output shape."""
        if not all_dfs:
            return self._empty_series_frame(empty_columns)

        df_all = pd.concat(all_dfs, ignore_index=True)
        df_all["date"] = pd.to_datetime(df_all["date"]).dt.date
        mask = (df_all["date"] >= start_d) & (df_all["date"] <= end_d)
        df_filtered = df_all.loc[mask].copy()

        if df_filtered.empty:
            return self._empty_series_frame(empty_columns)

        if date_as_string:
            df_filtered["date"] = df_filtered["date"].astype(str)
        if round_column is not None:
            df_filtered[round_column] = df_filtered[round_column].round(round_decimals)

        return df_filtered.reset_index(drop=True)
