from __future__ import annotations

import calendar
from datetime import date, datetime, timezone

import pandas as pd

from .cds_base import CDS, Location, ZoneInfo


class PrecipitationCDS(CDS):
    """CDS client specialized for daily precipitation retrieval."""

    @staticmethod
    def _daily_stats_time_zone_for_location(
        location: Location,
        year: int,
        month: int | None,
    ) -> str:
        """Build CDS daily-statistics UTC offset string from a location timezone.

        CDS daily-statistics expects `utc±HH:MM` rather than an IANA timezone name.
        """
        tz_local = ZoneInfo(location.tz)
        sample_month = 1 if month is None else month
        sample_dt = datetime(year, sample_month, 1, 12, 0, 0, tzinfo=tz_local)
        offset = sample_dt.utcoffset()
        if offset is None:
            return "utc+00:00"

        total_minutes = int(offset.total_seconds() // 60)
        sign = "+" if total_minutes >= 0 else "-"
        absolute_minutes = abs(total_minutes)
        hours = absolute_minutes // 60
        minutes = absolute_minutes % 60
        return f"utc{sign}{hours:02d}:{minutes:02d}"

    def get_series(
        self,
        location: Location,
        start_d: date,
        end_d: date,
        notify_progress: bool = True,
    ) -> pd.DataFrame:
        return self.get_daily_precipitation_series(location, start_d, end_d, notify_progress=notify_progress)

    def get_daily_precipitation_series(
        self,
        location: Location,
        start_d: date,
        end_d: date,
        half_box_deg: float | None = None,
        notify_progress: bool = True,
        notify_month_progress: bool = False,
    ) -> pd.DataFrame:
        empty_columns = [
            "date",
            "precip_mm",
            "grid_lat",
            "grid_lon",
            "place_name",
        ]

        if start_d > end_d:
            return self._empty_series_frame(empty_columns)

        day_span = (end_d - start_d).days + 1
        fetch_mode = getattr(self, "precipitation_fetch_mode", "monthly")

        if fetch_mode == "auto":
            fetch_mode = "monthly" if day_span <= self.month_fetch_day_span_threshold else "yearly"

        if fetch_mode == "yearly":
            years = list(range(start_d.year, end_d.year + 1))
            all_dfs = self._collect_period_frames(
                location,
                [(year, None) for year in years],
                lambda period: self.get_year_daily_precipitation_data(location, period[0], half_box_deg),
                notify_progress,
            )
        elif fetch_mode == "monthly":
            month_pairs = list(self._month_range(start_d, end_d))
            all_dfs = []
            total_months = len(month_pairs)
            for month_idx, (year, month) in enumerate(month_pairs, 1):
                if self.progress_manager and notify_month_progress:
                    self.progress_manager.notify_month_start(
                        location.name,
                        year,
                        month,
                        month_idx,
                        total_months,
                    )

                df_month = self.get_month_daily_precipitation_data(location, year, month, half_box_deg)
                all_dfs.append(df_month)

                if self.progress_manager and notify_month_progress:
                    self.progress_manager.notify_month_complete(
                        location.name,
                        year,
                        month,
                        month_idx,
                        total_months,
                    )
        else:
            raise ValueError(
                f"Unsupported precipitation fetch mode '{fetch_mode}'. "
                "Expected one of: monthly, yearly, auto"
            )

        return self._finalize_series_dataframe(
            all_dfs,
            start_d,
            end_d,
            empty_columns,
            date_as_string=True,
            round_column="precip_mm",
            round_decimals=3,
        )

    def get_month_daily_precipitation_data(
        self,
        location: Location,
        year: int,
        month: int,
        half_box_deg: float | None = None,
    ) -> pd.DataFrame:
        if not 1 <= month <= 12:
            raise ValueError(f"Invalid month '{month}'; expected 1..12")

        source_mode = getattr(self, "precipitation_daily_source", "hourly")
        if source_mode == "daily_statistics":
            return self._get_month_daily_precipitation_data_from_daily_stats(location, year, month, half_box_deg)
        if source_mode == "timeseries":
            return self._get_month_daily_precipitation_data_from_timeseries(location, year, month)

        if source_mode != "hourly":
            raise ValueError(
                f"Unsupported precipitation daily source '{source_mode}'. "
                "Expected one of: hourly, daily_statistics, timeseries"
            )

        tz_local = ZoneInfo(location.tz)
        safe_name = self._safe_location_name(location)

        cache_file = self.cache_dir / f"era5_tp_{safe_name}_{year:04d}_{month:02d}.nc"
        nc_file = self._cds_retrieve_era5_month(
            cache_file,
            year=year,
            month=month,
            loc=location,
            variable="total_precipitation",
            half_box_deg=half_box_deg,
        )
        ds = self._open_and_concat_for_var([nc_file], "tp")

        ds_point = self._select_location_point(ds, location)
        return self._build_daily_precipitation_dataframe(
            ds_point["tp"],
            tz_local,
            float(ds_point["latitude"].values),
            float(ds_point["longitude"].values),
            location.name,
        )

    def _get_month_daily_precipitation_data_from_daily_stats(
        self,
        location: Location,
        year: int,
        month: int,
        half_box_deg: float | None = None,
    ) -> pd.DataFrame:
        safe_name = self._safe_location_name(location)
        cache_file = self.cache_dir / f"era5_tp_dailystats_{safe_name}_{year:04d}_{month:02d}.nc"
        time_zone = self._daily_stats_time_zone_for_location(location, year, month)
        nc_file = self._cds_retrieve_era5_daily_statistics(
            cache_file,
            year=year,
            month=month,
            loc=location,
            variable="total_precipitation",
            daily_statistic="daily_sum",
            frequency="1_hourly",
            time_zone=time_zone,
            half_box_deg=half_box_deg,
        )
        ds = self._open_and_concat_for_var([nc_file], "tp")
        ds_point = self._select_location_point(ds, location)

        time_coord = "time" if "time" in ds_point.coords else "valid_time"
        daily_dates = pd.to_datetime(ds_point[time_coord].values).date
        precip_mm = ds_point["tp"].values.astype(float) * 1000.0
        return pd.DataFrame({
            "date": [d.isoformat() for d in daily_dates],
            "precip_mm": precip_mm,
            "grid_lat": float(ds_point["latitude"].values),
            "grid_lon": float(ds_point["longitude"].values),
            "place_name": location.name,
        })

    def _get_month_daily_precipitation_data_from_timeseries(
        self,
        location: Location,
        year: int,
        month: int,
    ) -> pd.DataFrame:
        tz_local = ZoneInfo(location.tz)
        safe_name = self._safe_location_name(location)
        cache_file = self.cache_dir / f"era5_tp_timeseries_{safe_name}_{year:04d}_{month:02d}.nc"

        start_d = date(year, month, 1)
        end_d = date(year, month, calendar.monthrange(year, month)[1])
        nc_file = self._cds_retrieve_era5_timeseries(
            cache_file,
            location,
            start_d=start_d,
            end_d=end_d,
            variable="total_precipitation",
        )
        ds = self._open_and_concat_for_var([nc_file], "tp")

        ds_point, grid_lat, grid_lon = self._resolve_timeseries_point_dataset(ds, location)

        da = ds_point["tp"]
        if "time" not in da.coords:
            if "valid_time" in da.coords:
                da = da.rename({"valid_time": "time"})
            elif "date" in da.coords:
                da = da.rename({"date": "time"})
            else:
                raise KeyError("Timeseries precipitation data is missing a time coordinate")

        return self._build_daily_precipitation_dataframe(
            da,
            tz_local,
            grid_lat,
            grid_lon,
            location.name,
        )

    @staticmethod
    def _resolve_timeseries_point_dataset(
        ds: pd.DataFrame | object,
        location: Location,
    ) -> tuple[object, float, float]:
        """Resolve point dataset and grid coordinates for timeseries responses.

        Handles both gridded lat/lon dimensions and scalar point lat/lon coordinates.
        """
        if "latitude" in ds.coords and "longitude" in ds.coords:
            lat_coord = ds["latitude"]
            lon_coord = ds["longitude"]
            if getattr(lat_coord, "ndim", 0) > 0 and getattr(lon_coord, "ndim", 0) > 0:
                ds_point = ds.sel(
                    latitude=location.lat,
                    longitude=location.lon,
                    method="nearest",
                )
                grid_lat = float(ds_point["latitude"].values)
                grid_lon = float(ds_point["longitude"].values)
                return ds_point, grid_lat, grid_lon

            ds_point = ds
            grid_lat = float(lat_coord.values)
            grid_lon = float(lon_coord.values)
            return ds_point, grid_lat, grid_lon

        ds_point = ds
        return ds_point, float(location.lat), float(location.lon)

    def get_year_daily_precipitation_data(
        self,
        location: Location,
        year: int,
        half_box_deg: float | None = None,
    ) -> pd.DataFrame:
        source_mode = getattr(self, "precipitation_daily_source", "hourly")
        if source_mode == "daily_statistics":
            return self._get_year_daily_precipitation_data_from_daily_stats(location, year, half_box_deg)
        if source_mode == "timeseries":
            return self._get_year_daily_precipitation_data_from_timeseries(location, year)

        if source_mode != "hourly":
            raise ValueError(
                f"Unsupported precipitation daily source '{source_mode}'. "
                "Expected one of: hourly, daily_statistics, timeseries"
            )

        tz_local = ZoneInfo(location.tz)
        safe_name = self._safe_location_name(location)

        start_dt_utc = datetime(year, 1, 1, 0, 0, tzinfo=timezone.utc)
        end_dt_utc = datetime(year, 12, 31, 23, 0, tzinfo=timezone.utc)
        hourly_utc = pd.date_range(start=start_dt_utc, end=end_dt_utc, freq="h")

        cache_file = self.cache_dir / f"era5_tp_{safe_name}_{year:04d}_daily.nc"
        nc_file = self._cds_retrieve_era5_date_series(
            cache_file,
            location,
            hourly_utc,
            variable="total_precipitation",
            half_box_deg=half_box_deg,
        )
        ds = self._open_and_concat_for_var([nc_file], "tp")

        ds_point = self._select_location_point(ds, location)
        return self._build_daily_precipitation_dataframe(
            ds_point["tp"],
            tz_local,
            float(ds_point["latitude"].values),
            float(ds_point["longitude"].values),
            location.name,
        )

    def _get_year_daily_precipitation_data_from_daily_stats(
        self,
        location: Location,
        year: int,
        half_box_deg: float | None = None,
    ) -> pd.DataFrame:
        safe_name = self._safe_location_name(location)
        cache_file = self.cache_dir / f"era5_tp_dailystats_{safe_name}_{year:04d}_daily.nc"
        time_zone = self._daily_stats_time_zone_for_location(location, year, None)
        nc_file = self._cds_retrieve_era5_daily_statistics(
            cache_file,
            year=year,
            month=None,
            loc=location,
            variable="total_precipitation",
            daily_statistic="daily_sum",
            frequency="1_hourly",
            time_zone=time_zone,
            half_box_deg=half_box_deg,
        )
        ds = self._open_and_concat_for_var([nc_file], "tp")
        ds_point = self._select_location_point(ds, location)

        time_coord = "time" if "time" in ds_point.coords else "valid_time"
        daily_dates = pd.to_datetime(ds_point[time_coord].values).date
        precip_mm = ds_point["tp"].values.astype(float) * 1000.0
        return pd.DataFrame({
            "date": [d.isoformat() for d in daily_dates],
            "precip_mm": precip_mm,
            "grid_lat": float(ds_point["latitude"].values),
            "grid_lon": float(ds_point["longitude"].values),
            "place_name": location.name,
        })

    def _get_year_daily_precipitation_data_from_timeseries(
        self,
        location: Location,
        year: int,
    ) -> pd.DataFrame:
        tz_local = ZoneInfo(location.tz)
        safe_name = self._safe_location_name(location)
        cache_file = self.cache_dir / f"era5_tp_timeseries_{safe_name}_{year:04d}_daily.nc"

        nc_file = self._cds_retrieve_era5_timeseries(
            cache_file,
            location,
            start_d=date(year, 1, 1),
            end_d=date(year, 12, 31),
            variable="total_precipitation",
        )
        ds = self._open_and_concat_for_var([nc_file], "tp")

        ds_point, grid_lat, grid_lon = self._resolve_timeseries_point_dataset(ds, location)

        da = ds_point["tp"]
        if "time" not in da.coords:
            if "valid_time" in da.coords:
                da = da.rename({"valid_time": "time"})
            elif "date" in da.coords:
                da = da.rename({"date": "time"})
            else:
                raise KeyError("Timeseries precipitation data is missing a time coordinate")

        return self._build_daily_precipitation_dataframe(
            da,
            tz_local,
            grid_lat,
            grid_lon,
            location.name,
        )
