from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd

from .cds_base import CDS, Location, ZoneInfo


class SolarRadiationCDS(CDS):
    """CDS client specialized for daily solar radiation energy retrieval."""

    def get_series(
        self,
        location: Location,
        start_d: date,
        end_d: date,
        notify_progress: bool = True,
    ) -> pd.DataFrame:
        return self.get_daily_solar_radiation_energy_series(
            location,
            start_d,
            end_d,
            notify_progress=notify_progress,
        )

    def get_daily_solar_radiation_energy_series(
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
            "solar_energy_MJ_m2",
            "grid_lat",
            "grid_lon",
            "place_name",
        ]

        if start_d > end_d:
            return self._empty_series_frame(empty_columns)

        day_span = (end_d - start_d).days + 1
        fetch_mode = getattr(self, "solar_fetch_mode", "monthly")

        if fetch_mode == "auto":
            fetch_mode = "monthly" if day_span <= self.month_fetch_day_span_threshold else "yearly"

        if fetch_mode == "yearly":
            years = list(range(start_d.year, end_d.year + 1))
            all_dfs = self._collect_period_frames(
                location,
                [(year, None) for year in years],
                lambda period: self.get_year_daily_solar_radiation_energy_data(
                    location,
                    period[0],
                    half_box_deg,
                ),
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

                df_month = self.get_month_daily_solar_radiation_energy_data(
                    location,
                    year,
                    month,
                    half_box_deg,
                )
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
                f"Unsupported solar fetch mode '{fetch_mode}'. "
                "Expected one of: monthly, yearly, auto"
            )

        return self._finalize_series_dataframe(
            all_dfs,
            start_d,
            end_d,
            empty_columns,
            date_as_string=True,
            round_column="solar_energy_MJ_m2",
            round_decimals=3,
        )

    def get_month_daily_solar_radiation_energy_data(
        self,
        location: Location,
        year: int,
        month: int,
        half_box_deg: float | None = None,
    ) -> pd.DataFrame:
        tz_local = ZoneInfo(location.tz)
        safe_name = self._safe_location_name(location)

        cache_file = self.cache_dir / f"era5_ssrd_{safe_name}_{year:04d}_{month:02d}.nc"
        nc_file = self._cds_retrieve_era5_month(
            cache_file,
            year=year,
            month=month,
            loc=location,
            variable="surface_solar_radiation_downwards",
            half_box_deg=half_box_deg,
        )
        ds = self._open_and_concat_for_var([nc_file], "ssrd")

        ds_point = self._select_location_point(ds, location)
        return self._build_daily_solar_radiation_dataframe(
            ds_point["ssrd"],
            tz_local,
            float(ds_point["latitude"].values),
            float(ds_point["longitude"].values),
            location.name,
        )

    def get_year_daily_solar_radiation_energy_data(
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

        cache_file = self.cache_dir / f"era5_ssrd_{safe_name}_{year:04d}_daily.nc"
        nc_file = self._cds_retrieve_era5_date_series(
            cache_file,
            location,
            hourly_utc,
            variable="surface_solar_radiation_downwards",
            half_box_deg=half_box_deg,
        )
        ds = self._open_and_concat_for_var([nc_file], "ssrd")

        ds_point = self._select_location_point(ds, location)
        return self._build_daily_solar_radiation_dataframe(
            ds_point["ssrd"],
            tz_local,
            float(ds_point["latitude"].values),
            float(ds_point["longitude"].values),
            location.name,
        )

    @staticmethod
    def _build_daily_solar_radiation_dataframe(
        da,
        tz_local: ZoneInfo,
        grid_lat: float,
        grid_lon: float,
        place_name: str,
    ) -> pd.DataFrame:
        """Build daily local-date solar radiation totals in MJ/m² from an ERA5 data array."""
        utc_times = pd.to_datetime(da["time"].values, utc=True)
        local_dates = utc_times.tz_convert(tz_local).date
        solar_energy_mj_m2 = da.values.astype(float) / 1_000_000.0

        df_daily = pd.DataFrame({
            "date": local_dates,
            "solar_energy_MJ_m2": solar_energy_mj_m2,
        })
        df_daily = df_daily.groupby("date", as_index=False, sort=True)["solar_energy_MJ_m2"].sum()
        df_daily["grid_lat"] = grid_lat
        df_daily["grid_lon"] = grid_lon
        df_daily["place_name"] = place_name
        return df_daily
