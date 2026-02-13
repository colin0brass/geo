from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd

from .cds_base import CDS, Location, ZoneInfo


class PrecipitationCDS(CDS):
    """CDS client specialized for daily precipitation retrieval."""

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

        if day_span <= self.month_fetch_day_span_threshold:
            month_pairs = list(self._month_range(start_d, end_d))
            all_dfs = self._collect_period_frames(
                location,
                [(year, month) for year, month in month_pairs],
                lambda period: self.get_month_daily_precipitation_data(location, period[0], period[1], half_box_deg),
                notify_progress,
            )
        else:
            years = list(range(start_d.year, end_d.year + 1))
            all_dfs = self._collect_period_frames(
                location,
                [(year, None) for year in years],
                lambda period: self.get_year_daily_precipitation_data(location, period[0], half_box_deg),
                notify_progress,
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

    def get_year_daily_precipitation_data(
        self,
        location: Location,
        year: int,
        half_box_deg: float | None = None,
    ) -> pd.DataFrame:
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
