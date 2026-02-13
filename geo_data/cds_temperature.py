from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from .cds_base import CDS, Location, ZoneInfo


class TemperatureCDS(CDS):
    """CDS client specialized for daily local-noon temperature retrieval."""

    def get_series(
        self,
        location: Location,
        start_d: date,
        end_d: date,
        notify_progress: bool = True,
    ) -> pd.DataFrame:
        return self.get_noon_series(location, start_d, end_d, notify_progress=notify_progress)

    def get_year_daily_noon_data(
        self,
        location: Location,
        year: int,
        half_box_deg: float | None = None,
    ) -> pd.DataFrame:
        tz_local = ZoneInfo(location.tz)

        start_d = date(year, 1, 1)
        end_d = date(year, 12, 31)
        local_noons, noon_utc = self._build_local_noon_timestamps(start_d, end_d, tz_local)

        safe_name = self._safe_location_name(location)
        cache_file = self.cache_dir / f"era5_t2m_{safe_name}_{year:04d}_noons.nc"
        nc_file = self._cds_retrieve_era5_date_series(cache_file, location, noon_utc, half_box_deg=half_box_deg)
        ds = self._open_and_concat_for_var([nc_file], "t2m")

        ds_point = self._select_location_point(ds, location)
        return self._build_noon_temperature_dataframe(
            ds_point["t2m"],
            noon_utc,
            local_noons,
            self.max_nearest_time_delta,
            float(ds_point["latitude"].values),
            float(ds_point["longitude"].values),
            location.name,
        )

    def get_month_daily_noon_data(
        self,
        location: Location,
        year: int,
        month: int,
        half_box_deg: float | None = None,
    ) -> pd.DataFrame:
        tz_local = ZoneInfo(location.tz)

        start_d = date(year, month, 1)
        if month == 12:
            end_d = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_d = date(year, month + 1, 1) - timedelta(days=1)

        local_noons, noon_utc = self._build_local_noon_timestamps(start_d, end_d, tz_local)

        safe_name = self._safe_location_name(location)
        cache_file = self.cache_dir / f"era5_t2m_{safe_name}_{year:04d}_{month:02d}_noons.nc"
        nc_file = self._cds_retrieve_era5_date_series(cache_file, location, noon_utc, half_box_deg=half_box_deg)
        ds = self._open_and_concat_for_var([nc_file], "t2m")

        ds_point = self._select_location_point(ds, location)
        return self._build_noon_temperature_dataframe(
            ds_point["t2m"],
            noon_utc,
            local_noons,
            self.max_nearest_time_delta,
            float(ds_point["latitude"].values),
            float(ds_point["longitude"].values),
            location.name,
        )

    def get_noon_series(
        self,
        location: Location,
        start_d: date,
        end_d: date,
        half_box_deg: float | None = None,
        notify_progress: bool = True,
    ) -> pd.DataFrame:
        empty_columns = [
            "date",
            "local_noon",
            "utc_time_used",
            "temp_C",
            "temp_F",
            "grid_lat",
            "grid_lon",
            "place_name",
        ]

        if start_d > end_d:
            return self._empty_series_frame(empty_columns)

        day_span = (end_d - start_d).days + 1
        fetch_mode = getattr(self, "temp_fetch_mode", "auto")

        if fetch_mode == "auto":
            fetch_mode = "monthly" if day_span <= self.month_fetch_day_span_threshold else "yearly"

        if fetch_mode == "monthly":
            month_pairs = list(self._month_range(start_d, end_d))
            all_dfs = self._collect_period_frames(
                location,
                [(year, month) for year, month in month_pairs],
                lambda period: self.get_month_daily_noon_data(location, period[0], period[1], half_box_deg),
                notify_progress,
            )
            return self._finalize_series_dataframe(
                all_dfs,
                start_d,
                end_d,
                empty_columns,
            )

        if fetch_mode == "yearly":
            years = list(range(start_d.year, end_d.year + 1))
            all_dfs = self._collect_period_frames(
                location,
                [(year, None) for year in years],
                lambda period: self.get_year_daily_noon_data(location, period[0], half_box_deg),
                notify_progress,
            )
            return self._finalize_series_dataframe(
                all_dfs,
                start_d,
                end_d,
                empty_columns,
            )

        raise ValueError(
            f"Unsupported temperature fetch mode '{fetch_mode}'. "
            "Expected one of: monthly, yearly, auto"
        )
