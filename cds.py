from __future__ import annotations

import sys
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

logger = logging.getLogger("geo")

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    logger.error("This script requires Python 3.9+ (zoneinfo).")
    sys.exit(1)

import cdsapi

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
    def __init__(self, cache_dir: Path = Path("era5_cache"), progress_manager=None) -> None:
        """
        Initialize the CDS client and set the cache directory.
        
        Args:
            cache_dir: Directory to cache downloaded NetCDF files.
            progress_manager: Optional ProgressManager for progress reporting.
        """
        self.client = cdsapi.Client(quiet=True, debug=False)
        self.cache_dir = cache_dir
        self.progress_manager = progress_manager
        
        # Suppress verbose cdsapi logging after client initialization
        cdsapi_logger = logging.getLogger('cdsapi')
        cdsapi_logger.handlers.clear()
        cdsapi_logger.setLevel(logging.WARNING)
        cdsapi_logger.propagate = False

    def _cds_retrieve_era5_month(
        self,
        out_nc: Path,
        year: int,
        month: int,
        loc: Location,
        day: int = None,
        hour: int = None,
        half_box_deg: float = 0.25
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
            half_box_deg (float, optional): Half-size of the grid box in degrees. Defaults to 0.25.
        Returns:
            Path: Path to the downloaded or cached NetCDF file.
        """
        out_nc.parent.mkdir(parents=True, exist_ok=True)

        north = loc.lat + half_box_deg
        south = loc.lat - half_box_deg
        west = loc.lon - half_box_deg
        east = loc.lon + half_box_deg

        day_range = [f"{d:02d}" for d in range(1, 32)] if day is None else [f"{day:02d}"]
        time_range = [f"{h:02d}:00" for h in range(0, 24)] if hour is None else [f"{hour:02d}:00"]

        request = {
            "product_type": "reanalysis",
            "variable": ["2m_temperature"],
            "year": f"{year:04d}",
            "month": f"{month:02d}",
            "day": day_range,
            "time": time_range,
            "area": [north, west, south, east],
            "format": "netcdf",
        }

        if out_nc.exists() and out_nc.stat().st_size > 0:
            return out_nc  # cache hit

        logger.info(f"Downloading ERA5 {year:04d}-{month:02d} to {out_nc} ...")
        self.client.retrieve("reanalysis-era5-single-levels", request, str(out_nc))
        return out_nc

    def _cds_retrieve_era5_date_series(
        self,
        out_nc: Path,
        loc: Location,
        dates_utc: pd.DatetimeIndex,
        half_box_deg: float = 0.25
    ) -> Path:
        """
        Download ERA5 data for a series of UTC dates for a location, or use cached file if available.
        Args:
            out_nc (Path): Output NetCDF file path.
            loc (Location): Location object.
            dates_utc (pd.DatetimeIndex): UTC datetime index for retrieval.
            half_box_deg (float, optional): Half-size of the grid box in degrees. Defaults to 0.25.
        Returns:
            Path: Path to the downloaded or cached NetCDF file.
        """
        out_nc.parent.mkdir(parents=True, exist_ok=True)

        north = loc.lat + half_box_deg
        south = loc.lat - half_box_deg
        west = loc.lon - half_box_deg
        east = loc.lon + half_box_deg

        years = sorted(set(d.year for d in dates_utc))
        months = sorted(set(f"{d.month:02d}" for d in dates_utc))
        days = sorted(set(f"{d.day:02d}" for d in dates_utc))
        times = sorted(set(f"{d.hour:02d}:00" for d in dates_utc))

        request = {
            "product_type": "reanalysis",
            "variable": ["2m_temperature"],
            "year": [f"{y:04d}" for y in years],
            "month": months,
            "day": days,
            "time": times,
            "area": [north, west, south, east],
            "format": "netcdf",
        }

        if out_nc.exists() and out_nc.stat().st_size > 0:
            return out_nc  # cache hit

        logger.info(f"Downloading ERA5 date series to {out_nc} ...")
        self.client.retrieve("reanalysis-era5-single-levels", request, str(out_nc))
        return out_nc
    
    def _open_and_concat(self, nc_files: list[Path]) -> xr.Dataset:
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

        # Use open_mfdataset for efficiency; fallback to concat if needed
        ds = xr.open_mfdataset(
            [str(p) for p in nc_files],
            combine="by_coords",
            parallel=False,
            engine="netcdf4",
        )

        # Standardize variable name (ERA5 uses 't2m')
        if "t2m" not in ds.data_vars:
            raise KeyError(f"Expected variable 't2m' not found. Variables: {list(ds.data_vars)}")

        return ds

    def get_month_data(
        self,
        location: Location,
        year: int,
        month: int,
        day: int = None,
        hour: int = None,
        half_box_deg: float = 0.25
    ) -> xr.Dataset:
        """
        Retrieve and return an xarray Dataset of ERA5 data for a given month and location.
        Args:
            location (Location): Location object.
            year (int): Year.
            month (int): Month.
            day (int, optional): Specific day. Defaults to None.
            hour (int, optional): Specific hour. Defaults to None.
            half_box_deg (float, optional): Half-size of the grid box in degrees. Defaults to 0.25.
        Returns:
            xr.Dataset: ERA5 data for the specified month and location.
        """
        """
        Retrieve and return a DataFrame of daily local noon temperatures for a given month and location.
        """
        cache_file = self.cache_dir / f"era5_t2m_{location.name.replace(' ', '_').replace(',', '')}_{year:04d}_{month:02d}.nc"
        nc_file = self._cds_retrieve_era5_month(
            cache_file, year=year, month=month, loc=location, day=day, hour=hour, half_box_deg=half_box_deg)
        ds = self._open_and_concat([nc_file])

        return ds
    
    def get_year_daily_noon_data(
        self,
        location: Location,
        year: int,
        half_box_deg: float = 0.25
    ) -> pd.DataFrame:
        """
        Retrieve and return a DataFrame of daily local noon temperatures for an entire year.
        This is much more efficient than calling get_month_daily_noon_data 12 times.
        
        Args:
            location (Location): Location object.
            year (int): Year.
            half_box_deg (float, optional): Half-size of the grid box in degrees. Defaults to 0.25.
        Returns:
            pd.DataFrame: DataFrame with daily local noon temperatures for the entire year.
        Raises:
            RuntimeError: If selected times are too far from requested local noon.
        """
        tz_local = ZoneInfo(location.tz)

        # Get all days in the year
        start_d = date(year, 1, 1)
        end_d = date(year, 12, 31)

        # Build daily local-noon timestamps, convert to UTC
        days = pd.date_range(start=start_d, end=end_d, freq="D")
        local_noons = [
            datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=tz_local) for d in days.to_pydatetime()
        ]
        noon_utc = pd.DatetimeIndex([dt.astimezone(timezone.utc) for dt in local_noons])

        # Cache entire year in one file
        cache_file = self.cache_dir / f"era5_t2m_{location.name.replace(' ', '_').replace(',', '')}_{year:04d}_noons.nc"
        nc_file = self._cds_retrieve_era5_date_series(cache_file, location, noon_utc, half_box_deg=half_box_deg)
        ds = self._open_and_concat([nc_file])

        # Find nearest grid point to location
        ds_point = ds.sel(
            latitude=location.lat,
            longitude=location.lon,
            method="nearest",
        )

        if "valid_time" in ds_point.coords:
            ds_point = ds_point.rename({"valid_time": "time"})
        da = ds_point["t2m"]

        selected = da.sel(time=noon_utc.tz_convert(None), method="nearest")
        selected_times = pd.to_datetime(selected["time"].values).tz_localize("UTC")
        delta = np.abs(selected_times - noon_utc)
        if (delta > pd.Timedelta("30min")).any():
            bad = delta[delta > pd.Timedelta("30min")]
            raise RuntimeError(
                "Some noon selections were more than 30 minutes away from requested local noon UTC.\n"
                f"Examples:\n{bad[:5]}"
            )
        temp_k = selected.values.astype(np.float64)
        temp_c = temp_k - 273.15
        temp_f = temp_c * 9.0 / 5.0 + 32.0
        df = pd.DataFrame(
            {
                "date": [dt.date().isoformat() for dt in local_noons],
                "local_noon": [dt.isoformat() for dt in local_noons],
                "utc_time_used": [dt.isoformat() for dt in noon_utc],
                "temp_C": np.round(temp_c, 3),
                "temp_F": np.round(temp_f, 3),
                "grid_lat": float(ds_point["latitude"].values),
                "grid_lon": float(ds_point["longitude"].values),
                "place_name": location.name,
            }
        )
        return df

    def get_month_daily_noon_data(
        self,
        location: Location,
        year: int,
        month: int,
        half_box_deg: float = 0.25
    ) -> pd.DataFrame:
        """
        Retrieve and return a DataFrame of daily local noon temperatures for a given month and location.
        Args:
            location (Location): Location object.
            year (int): Year.
            month (int): Month.
            half_box_deg (float, optional): Half-size of the grid box in degrees. Defaults to 0.25.
        Returns:
            pd.DataFrame: DataFrame with daily local noon temperatures.
        Raises:
            RuntimeError: If selected times are too far from requested local noon.
        """
        """
        Retrieve and return a DataFrame of daily local noon temperatures for a given month and location.
        """
        # ds = self.get_month_data(
        #     location, year, month, half_box_deg=half_box_deg)

        tz_local = ZoneInfo(location.tz)

        # Get the first and last day of the month
        start_d = date(year, month, 1)
        if month == 12:
            end_d = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_d = date(year, month + 1, 1) - timedelta(days=1)

        # Build daily local-noon timestamps, convert to UTC
        days = pd.date_range(start=start_d, end=end_d, freq="D")
        local_noons = [
            datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=tz_local) for d in days.to_pydatetime()
        ]
        noon_utc = pd.DatetimeIndex([dt.astimezone(timezone.utc) for dt in local_noons])

        cache_file = self.cache_dir / f"era5_t2m_{location.name.replace(' ', '_').replace(',', '')}_{year:04d}_{month:02d}_noons.nc"
        nc_file = self._cds_retrieve_era5_date_series(cache_file, location, noon_utc, half_box_deg=half_box_deg)
        ds = self._open_and_concat([nc_file])

        # Find nearest grid point to location
        ds_point = ds.sel(
            latitude=location.lat,
            longitude=location.lon,
            method="nearest",
        )

        if "valid_time" in ds_point.coords:
            ds_point = ds_point.rename({"valid_time": "time"})
        da = ds_point["t2m"]

        selected = da.sel(time=noon_utc.tz_convert(None), method="nearest")  # xarray expects naive UTC-like
        selected_times = pd.to_datetime(selected["time"].values).tz_localize("UTC")
        delta = np.abs(selected_times - noon_utc)
        if (delta > pd.Timedelta("30min")).any():
            bad = delta[delta > pd.Timedelta("30min")]
            raise RuntimeError(
                "Some noon selections were more than 30 minutes away from requested local noon UTC.\n"
                f"Examples:\n{bad[:5]}"
            )
        temp_k = selected.values.astype(np.float64)
        temp_c = temp_k - 273.15
        temp_f = temp_c * 9.0 / 5.0 + 32.0
        df = pd.DataFrame(
            {
                "date": [dt.date().isoformat() for dt in local_noons],
                "local_noon": [dt.isoformat() for dt in local_noons],
                "utc_time_used": [dt.isoformat() for dt in noon_utc],
                "temp_C": np.round(temp_c, 3),
                "temp_F": np.round(temp_f, 3),
                "grid_lat": float(ds_point["latitude"].values),
                "grid_lon": float(ds_point["longitude"].values),
                "place_name": location.name,
            }
        )
        return df

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

    def get_noon_series(
        self,
        location: Location,
        start_d: date,
        end_d: date,
        half_box_deg: float = 0.25,
        notify_progress: bool = True
    ) -> pd.DataFrame:
        """
        Retrieve and return a DataFrame of daily local noon temperatures for the given location and date range.
        
        Optimized to download entire years at once rather than month-by-month, reducing API calls by 12x.
        
        Args:
            location (Location): Location object.
            start_d (date): Start date.
            end_d (date): End date.
            half_box_deg (float, optional): Half-size of the grid box in degrees. Defaults to 0.25.
            notify_progress (bool, optional): Whether to notify progress manager. Defaults to True.
        Returns:
            pd.DataFrame: DataFrame with daily local noon temperatures for the date range.
        """
        all_dfs = []
        
        # Get list of years to process
        years = list(range(start_d.year, end_d.year + 1))
        total_years = len(years)
        
        for year_idx, year in enumerate(years, 1):
            if self.progress_manager and notify_progress:
                self.progress_manager.notify_year_start(location.name, year, year_idx, total_years)
            
            # Download entire year at once
            df_year = self.get_year_daily_noon_data(location, year, half_box_deg)
            all_dfs.append(df_year)
            
            if self.progress_manager and notify_progress:
                self.progress_manager.notify_year_complete(location.name, year, year_idx, total_years)

        df_all = pd.concat(all_dfs, ignore_index=True)

        # Filter to the exact date range requested
        df_all["date"] = pd.to_datetime(df_all["date"]).dt.date
        mask = (df_all["date"] >= start_d) & (df_all["date"] <= end_d)
        df_filtered = df_all.loc[mask].reset_index(drop=True)

        return df_filtered
    