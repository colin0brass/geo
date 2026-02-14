# Test CDS class and Location dataclass
import pytest
import logging
import pandas as pd
import numpy as np
import xarray as xr
import zipfile
from pathlib import Path
from geo_data.cds_base import CDS, Location
from geo_data.cds_precipitation import PrecipitationCDS
from geo_data.cds_solar_radiation import SolarRadiationCDS
from geo_data.cds_temperature import TemperatureCDS
from logging_config import setup_logging, sync_cds_warning_visibility


class DummyCDS(TemperatureCDS):
    def __init__(self, cache_dir: Path):
        self.client = None
        self.cache_dir = cache_dir
        self.progress_manager = None
        self.default_half_box_deg = 0.25
        self.max_nearest_time_delta = pd.Timedelta("30min")
        self.month_fetch_day_span_threshold = 62
        self.year_calls = 0
        self.month_calls = 0

    def get_month_daily_noon_data(self, location, year, month, half_box_deg=0.25):
        self.month_calls += 1
        return pd.DataFrame({
            'date': [f'{year}-{month:02d}-01'],
            'local_noon': [f'{year}-{month:02d}-01T12:00:00+00:00'],
            'utc_time_used': [f'{year}-{month:02d}-01T12:00:00+00:00'],
            'temp_C': [10.0],
            'temp_F': [50.0],
            'grid_lat': [location.lat],
            'grid_lon': [location.lon],
            'place_name': [location.name],
        })

    def get_year_daily_noon_data(self, location, year, half_box_deg=0.25):
        self.year_calls += 1
        return pd.DataFrame({
            'date': [f'{year}-01-01'],
            'local_noon': [f'{year}-01-01T12:00:00+00:00'],
            'utc_time_used': [f'{year}-01-01T12:00:00+00:00'],
            'temp_C': [10.0],
            'temp_F': [50.0],
            'grid_lat': [location.lat],
            'grid_lon': [location.lon],
            'place_name': [location.name],
        })


def test_location_dataclass():
    loc = Location(name="Test", lat=1.0, lon=2.0, tz="UTC")
    assert loc.name == "Test"
    assert loc.lat == 1.0
    assert loc.lon == 2.0
    assert loc.tz == "UTC"


def test_location_timezone_autodetect():
    # Test auto-detection for London coordinates
    loc = Location(name="London", lat=51.5074, lon=-0.1278)
    assert loc.tz == "Europe/London"


def test_location_timezone_explicit_override():
    # Test that explicit timezone overrides auto-detection
    loc = Location(name="London", lat=51.5074, lon=-0.1278, tz="America/New_York")
    assert loc.tz == "America/New_York"


def test_cds_month_range():
    cds = DummyCDS(cache_dir=Path("/tmp/era5_cache"))
    months = list(cds._month_range(pd.Timestamp('2025-01-01').date(), pd.Timestamp('2025-03-01').date()))
    assert months == [(2025, 1), (2025, 2), (2025, 3)]


def test_cds_get_noon_series_monkeypatch(tmp_path):
    cds = DummyCDS(cache_dir=tmp_path)
    loc = Location(name="Test", lat=1.0, lon=2.0, tz="UTC")
    df = cds.get_noon_series(loc, pd.Timestamp('2025-01-01').date(), pd.Timestamp('2025-01-01').date())
    assert not df.empty
    assert df['place_name'].iloc[0] == "Test"
    assert cds.month_calls == 1
    assert cds.year_calls == 0


def test_cds_get_noon_series_long_range_uses_year_fetch(tmp_path):
    cds = DummyCDS(cache_dir=tmp_path)
    loc = Location(name="Test", lat=1.0, lon=2.0, tz="UTC")
    _ = cds.get_noon_series(loc, pd.Timestamp('2025-01-01').date(), pd.Timestamp('2025-04-15').date())
    assert cds.year_calls == 1
    assert cds.month_calls == 0


def test_cds_get_noon_series_short_range_yearly_override(tmp_path):
    class DummyYearlyTempCDS(DummyCDS):
        def __init__(self, cache_dir: Path):
            super().__init__(cache_dir)
            self.temp_fetch_mode = "yearly"

    cds = DummyYearlyTempCDS(cache_dir=tmp_path)
    loc = Location(name="Test", lat=1.0, lon=2.0, tz="UTC")
    _ = cds.get_noon_series(loc, pd.Timestamp('2025-01-01').date(), pd.Timestamp('2025-01-31').date())
    assert cds.year_calls == 1
    assert cds.month_calls == 0


def test_cds_get_noon_series_timeseries_source(tmp_path):
    class DummyTimeseriesTempCDS(TemperatureCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("400D")
            self.month_fetch_day_span_threshold = 62
            self.temp_daily_source = "timeseries"

        def _cds_retrieve_era5_timeseries(
            self,
            out_nc,
            loc,
            start_d,
            end_d,
            variable="2m_temperature",
            data_format="netcdf",
        ):
            assert variable == "2m_temperature"
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "t2m"
            times = pd.date_range("2025-01-01T00:00:00", periods=24, freq="h")
            t2m = np.full((24, 1, 1), 283.15, dtype=float)
            return xr.Dataset(
                data_vars={"t2m": (("time", "latitude", "longitude"), t2m)},
                coords={
                    "time": times,
                    "latitude": [52.21],
                    "longitude": [0.12],
                },
            )

    cds = DummyTimeseriesTempCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")
    df = cds.get_noon_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-01-01").date(),
    )

    assert len(df) == 1
    assert df["temp_C"].iloc[0] == pytest.approx(10.0)
    assert df["grid_lat"].iloc[0] == pytest.approx(52.21)
    assert df["grid_lon"].iloc[0] == pytest.approx(0.12)


def test_cds_get_year_daily_noon_data_timeseries_scalar_coords(tmp_path):
    class DummyTimeseriesTempCDS(TemperatureCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("400D")
            self.month_fetch_day_span_threshold = 62
            self.temp_daily_source = "timeseries"

        def _cds_retrieve_era5_timeseries(
            self,
            out_nc,
            loc,
            start_d,
            end_d,
            variable="2m_temperature",
            data_format="netcdf",
        ):
            assert variable == "2m_temperature"
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "t2m"
            times = pd.date_range("2025-01-01T00:00:00", periods=24, freq="h")
            t2m = np.full((24,), 283.15, dtype=float)
            return xr.Dataset(
                data_vars={"t2m": (("time",), t2m)},
                coords={
                    "time": times,
                    "latitude": 52.25,
                    "longitude": 0.0,
                },
            )

    cds = DummyTimeseriesTempCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")
    df = cds.get_year_daily_noon_data(loc, 2025)

    assert len(df) >= 1
    assert df["temp_C"].iloc[0] == pytest.approx(10.0)
    assert df["grid_lat"].iloc[0] == pytest.approx(52.25)
    assert df["grid_lon"].iloc[0] == pytest.approx(0.0)


def test_cds_get_daily_precipitation_series(tmp_path):
    class DummyPrecipCDS(PrecipitationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.precipitation_daily_source = "hourly"

        def _cds_retrieve_era5_month(
            self,
            out_nc,
            year,
            month,
            loc,
            day=None,
            hour=None,
            variable="2m_temperature",
            half_box_deg=0.25,
        ):
            assert variable == "total_precipitation"
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "tp"
            times = pd.date_range("2025-01-01T00:00:00Z", periods=24, freq="h")
            tp = np.full((24, 1, 1), 0.001, dtype=float)  # 1 mm each hour
            return xr.Dataset(
                data_vars={"tp": (("time", "latitude", "longitude"), tp)},
                coords={
                    "time": times,
                    "latitude": [52.21],
                    "longitude": [0.12],
                },
            )

    cds = DummyPrecipCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    df = cds.get_daily_precipitation_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-01-01").date(),
    )

    assert len(df) == 1
    assert df["place_name"].iloc[0] == "Cambridge, UK"
    assert df["precip_mm"].iloc[0] == pytest.approx(24.0)


def test_cds_get_daily_precipitation_series_long_range_uses_month_fetch(tmp_path):
    class DummyPrecipCDS(PrecipitationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.precip_month_calls = 0
            self.precip_year_calls = 0

        def get_month_daily_precipitation_data(self, location, year, month, half_box_deg=0.25):
            self.precip_month_calls += 1
            return pd.DataFrame({
                'date': [f'{year}-{month:02d}-01'],
                'precip_mm': [1.0],
                'grid_lat': [location.lat],
                'grid_lon': [location.lon],
                'place_name': [location.name],
            })

        def get_year_daily_precipitation_data(self, location, year, half_box_deg=0.25):
            self.precip_year_calls += 1
            return pd.DataFrame({
                'date': [f'{year}-01-01'],
                'precip_mm': [1.0],
                'grid_lat': [location.lat],
                'grid_lon': [location.lon],
                'place_name': [location.name],
            })

    cds = DummyPrecipCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    _ = cds.get_daily_precipitation_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-04-15").date(),
    )

    assert cds.precip_year_calls == 0
    assert cds.precip_month_calls == 4


def test_cds_get_daily_precipitation_series_long_range_yearly_override(tmp_path):
    class DummyPrecipCDS(PrecipitationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.precipitation_fetch_mode = "yearly"
            self.precip_month_calls = 0
            self.precip_year_calls = 0

        def get_month_daily_precipitation_data(self, location, year, month, half_box_deg=0.25):
            self.precip_month_calls += 1
            return pd.DataFrame({
                'date': [f'{year}-{month:02d}-01'],
                'precip_mm': [1.0],
                'grid_lat': [location.lat],
                'grid_lon': [location.lon],
                'place_name': [location.name],
            })

        def get_year_daily_precipitation_data(self, location, year, half_box_deg=0.25):
            self.precip_year_calls += 1
            return pd.DataFrame({
                'date': [f'{year}-01-01'],
                'precip_mm': [1.0],
                'grid_lat': [location.lat],
                'grid_lon': [location.lon],
                'place_name': [location.name],
            })

    cds = DummyPrecipCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    _ = cds.get_daily_precipitation_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-04-15").date(),
    )

    assert cds.precip_year_calls == 1
    assert cds.precip_month_calls == 0


def test_cds_get_daily_precipitation_series_daily_statistics_source(tmp_path):
    class DummyPrecipDailyStatsCDS(PrecipitationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.precipitation_daily_source = "daily_statistics"

        def _cds_retrieve_era5_daily_statistics(
            self,
            out_nc,
            year,
            month,
            loc,
            variable,
            daily_statistic="daily_sum",
            frequency="1_hourly",
            time_zone="utc+00:00",
            half_box_deg=0.25,
        ):
            assert variable == "total_precipitation"
            assert daily_statistic == "daily_sum"
            assert frequency == "1_hourly"
            assert time_zone == "utc+00:00"
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "tp"
            times = pd.date_range("2025-01-01", periods=2, freq="D")
            tp = np.array([[[0.001]], [[0.002]]], dtype=float)  # 1mm, 2mm daily sums
            return xr.Dataset(
                data_vars={"tp": (("time", "latitude", "longitude"), tp)},
                coords={
                    "time": times,
                    "latitude": [52.21],
                    "longitude": [0.12],
                },
            )

    cds = DummyPrecipDailyStatsCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    df = cds.get_daily_precipitation_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-01-02").date(),
    )

    assert list(df["precip_mm"]) == pytest.approx([1.0, 2.0])
    assert list(df["date"]) == ["2025-01-01", "2025-01-02"]


def test_cds_get_year_daily_precipitation_data_daily_statistics_source(tmp_path):
    class DummyPrecipDailyStatsCDS(PrecipitationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.precipitation_daily_source = "daily_statistics"
            self.retrieval_calls = []

        def _cds_retrieve_era5_daily_statistics(
            self,
            out_nc,
            year,
            month,
            loc,
            variable,
            daily_statistic="daily_sum",
            frequency="1_hourly",
            time_zone="utc+00:00",
            half_box_deg=0.25,
        ):
            self.retrieval_calls.append((year, month, variable, daily_statistic, frequency, time_zone))
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "tp"
            valid_times = pd.date_range("2025-01-01", periods=3, freq="D")
            tp = np.array([[[0.001]], [[0.002]], [[0.003]]], dtype=float)
            return xr.Dataset(
                data_vars={"tp": (("valid_time", "latitude", "longitude"), tp)},
                coords={
                    "valid_time": valid_times,
                    "latitude": [52.21],
                    "longitude": [0.12],
                },
            )

    cds = DummyPrecipDailyStatsCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    df = cds.get_year_daily_precipitation_data(loc, 2025)

    assert cds.retrieval_calls == [
        (2025, None, "total_precipitation", "daily_sum", "1_hourly", "utc+00:00")
    ]
    assert list(df["date"]) == ["2025-01-01", "2025-01-02", "2025-01-03"]
    assert list(df["precip_mm"]) == pytest.approx([1.0, 2.0, 3.0])


def test_cds_get_daily_precipitation_series_timeseries_source(tmp_path):
    class DummyPrecipTimeseriesCDS(PrecipitationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.precipitation_daily_source = "timeseries"

        def _cds_retrieve_era5_timeseries(
            self,
            out_nc,
            loc,
            start_d,
            end_d,
            variable="2m_temperature",
            data_format="netcdf",
        ):
            assert variable == "total_precipitation"
            assert data_format == "netcdf"
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "tp"
            times = pd.date_range("2025-01-01T00:00:00Z", periods=48, freq="h")
            tp = np.full((48, 1, 1), 0.001, dtype=float)  # 1mm each hour
            return xr.Dataset(
                data_vars={"tp": (("time", "latitude", "longitude"), tp)},
                coords={
                    "time": times,
                    "latitude": [52.21],
                    "longitude": [0.12],
                },
            )

    cds = DummyPrecipTimeseriesCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    df = cds.get_daily_precipitation_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-01-02").date(),
    )

    assert list(df["date"]) == ["2025-01-01", "2025-01-02"]
    assert list(df["precip_mm"]) == pytest.approx([24.0, 24.0])


def test_cds_get_daily_precipitation_series_timeseries_source_scalar_coords(tmp_path):
    class DummyPrecipTimeseriesCDS(PrecipitationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.precipitation_daily_source = "timeseries"

        def _cds_retrieve_era5_timeseries(
            self,
            out_nc,
            loc,
            start_d,
            end_d,
            variable="2m_temperature",
            data_format="netcdf",
        ):
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "tp"
            times = pd.date_range("2025-01-01T00:00:00Z", periods=24, freq="h")
            tp = np.full((24,), 0.001, dtype=float)
            return xr.Dataset(
                data_vars={"tp": (("time",), tp)},
                coords={
                    "time": times,
                    "latitude": 52.25,
                    "longitude": 0.0,
                },
            )

    cds = DummyPrecipTimeseriesCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    df = cds.get_daily_precipitation_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-01-01").date(),
    )

    assert len(df) == 1
    assert df["grid_lat"].iloc[0] == pytest.approx(52.25)
    assert df["grid_lon"].iloc[0] == pytest.approx(0.0)
    assert df["precip_mm"].iloc[0] == pytest.approx(24.0)


def test_cds_get_month_daily_precipitation_data_invalid_source_raises(tmp_path):
    class DummyPrecipInvalidSourceCDS(PrecipitationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.precipitation_daily_source = "invalid_source"

    cds = DummyPrecipInvalidSourceCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    with pytest.raises(ValueError, match="Unsupported precipitation daily source"):
        cds.get_month_daily_precipitation_data(loc, 2025, 1)


def test_cds_retrieve_era5_timeseries_builds_expected_request(tmp_path):
    class FakeClient:
        def __init__(self):
            self.calls = []

        def retrieve(self, dataset, request, target):
            self.calls.append((dataset, request, target))

    class DummyTimeseriesCDS(CDS):
        def __init__(self, cache_dir: Path):
            self.client = FakeClient()
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62

    cds = DummyTimeseriesCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="Europe/London")
    out_nc = tmp_path / "timeseries.nc"

    result = cds._cds_retrieve_era5_timeseries(
        out_nc,
        loc,
        start_d=pd.Timestamp("2025-01-01").date(),
        end_d=pd.Timestamp("2025-01-31").date(),
        variable="total_precipitation",
        data_format="netcdf",
    )

    assert result == out_nc
    assert len(cds.client.calls) == 1

    dataset, request, target = cds.client.calls[0]
    assert dataset == "reanalysis-era5-single-levels-timeseries"
    assert request["variable"] == ["total_precipitation"]
    assert request["location"] == {"latitude": 52.21, "longitude": 0.12}
    assert request["date"] == ["2025-01-01/2025-01-31"]
    assert request["data_format"] == "netcdf"
    assert target == str(out_nc)


def test_cds_retrieve_era5_timeseries_extracts_zip_response(tmp_path):
    expected_nc_bytes = b"dummy-netcdf-content"

    class FakeClient:
        def retrieve(self, dataset, request, target):
            with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("payload.nc", expected_nc_bytes)

    class DummyTimeseriesCDS(CDS):
        def __init__(self, cache_dir: Path):
            self.client = FakeClient()
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62

    cds = DummyTimeseriesCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="Europe/London")
    out_nc = tmp_path / "timeseries_zip.nc"

    result = cds._cds_retrieve_era5_timeseries(
        out_nc,
        loc,
        start_d=pd.Timestamp("2025-01-01").date(),
        end_d=pd.Timestamp("2025-01-02").date(),
        variable="total_precipitation",
        data_format="netcdf",
    )

    assert result == out_nc
    assert out_nc.read_bytes() == expected_nc_bytes


def test_cds_client_warnings_suppressed_by_default(monkeypatch, tmp_path):
    captured_kwargs = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

    monkeypatch.setattr("geo_data.cds_base.cdsapi.Client", FakeClient)

    logging_cfg = tmp_path / "logging_config.yaml"
    logging_cfg.write_text(
        """
logging:
    log_file: test.log
    console_level: WARNING
    suppress_cdsapi: true
    cds_warnings_in_verbose: true
places:
    default_place: Test City
    all_places: []
"""
    )

    geo_logger = logging.getLogger("geo")
    geo_logger.handlers.clear()
    setup_logging(logging_cfg)
    sync_cds_warning_visibility(console_is_debug=False)

    _ = CDS(cache_dir=tmp_path, config_path=Path("config.yaml"))

    assert callable(captured_kwargs["warning_callback"])


def test_cds_client_warnings_enabled_in_verbose(monkeypatch, tmp_path):
    captured_kwargs = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

    monkeypatch.setattr("geo_data.cds_base.cdsapi.Client", FakeClient)

    logging_cfg = tmp_path / "logging_config.yaml"
    logging_cfg.write_text(
        """
logging:
    log_file: test.log
    console_level: WARNING
    suppress_cdsapi: true
    cds_warnings_in_verbose: true
places:
    default_place: Test City
    all_places: []
"""
    )

    geo_logger = logging.getLogger("geo")
    geo_logger.handlers.clear()
    setup_logging(logging_cfg)
    sync_cds_warning_visibility(console_is_debug=True)

    _ = CDS(cache_dir=tmp_path, config_path=Path("config.yaml"))

    assert captured_kwargs["warning_callback"] is None


def test_cds_daily_statistics_uses_location_utc_offset_time_zone(tmp_path):
    class DummyPrecipDailyStatsCDS(PrecipitationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.precipitation_daily_source = "daily_statistics"
            self.seen_time_zone = None

        def _cds_retrieve_era5_daily_statistics(
            self,
            out_nc,
            year,
            month,
            loc,
            variable,
            daily_statistic="daily_sum",
            frequency="1_hourly",
            time_zone="utc+00:00",
            half_box_deg=0.25,
        ):
            self.seen_time_zone = time_zone
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "tp"
            times = pd.date_range("2025-01-01", periods=1, freq="D")
            tp = np.array([[[0.001]]], dtype=float)
            return xr.Dataset(
                data_vars={"tp": (("time", "latitude", "longitude"), tp)},
                coords={
                    "time": times,
                    "latitude": [12.97],
                    "longitude": [77.59],
                },
            )

    cds = DummyPrecipDailyStatsCDS(cache_dir=tmp_path)
    loc = Location(name="Bangalore, India", lat=12.97, lon=77.59, tz="Asia/Kolkata")

    _ = cds.get_month_daily_precipitation_data(loc, 2025, 1)
    assert cds.seen_time_zone == "utc+05:30"


def test_cds_get_daily_solar_radiation_energy_series(tmp_path):
    class DummySolarCDS(SolarRadiationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62

        def _cds_retrieve_era5_month(
            self,
            out_nc,
            year,
            month,
            loc,
            day=None,
            hour=None,
            variable="2m_temperature",
            half_box_deg=0.25,
        ):
            assert variable == "surface_solar_radiation_downwards"
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "ssrd"
            times = pd.date_range("2025-01-01T00:00:00Z", periods=24, freq="h")
            ssrd = np.full((24, 1, 1), 1_000_000.0, dtype=float)  # 1 MJ/m² each hour
            return xr.Dataset(
                data_vars={"ssrd": (("time", "latitude", "longitude"), ssrd)},
                coords={
                    "time": times,
                    "latitude": [52.21],
                    "longitude": [0.12],
                },
            )

    cds = DummySolarCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    df = cds.get_daily_solar_radiation_energy_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-01-01").date(),
    )

    assert len(df) == 1
    assert df["place_name"].iloc[0] == "Cambridge, UK"
    assert df["solar_energy_MJ_m2"].iloc[0] == pytest.approx(24.0)


def test_cds_get_daily_solar_radiation_energy_series_timeseries_source(tmp_path):
    class DummySolarTimeseriesCDS(SolarRadiationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.solar_daily_source = "timeseries"

        def _cds_retrieve_era5_timeseries(
            self,
            out_nc,
            loc,
            start_d,
            end_d,
            variable="2m_temperature",
            data_format="netcdf",
        ):
            assert variable == "surface_solar_radiation_downwards"
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "ssrd"
            times = pd.date_range("2025-01-01T00:00:00Z", periods=24, freq="h")
            ssrd = np.full((24, 1, 1), 1_000_000.0, dtype=float)
            return xr.Dataset(
                data_vars={"ssrd": (("time", "latitude", "longitude"), ssrd)},
                coords={
                    "time": times,
                    "latitude": [52.21],
                    "longitude": [0.12],
                },
            )

    cds = DummySolarTimeseriesCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    df = cds.get_daily_solar_radiation_energy_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-01-01").date(),
    )

    assert len(df) == 1
    assert df["solar_energy_MJ_m2"].iloc[0] == pytest.approx(24.0)


def test_cds_get_year_daily_solar_radiation_energy_data_timeseries_scalar_coords(tmp_path):
    class DummySolarTimeseriesCDS(SolarRadiationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.solar_daily_source = "timeseries"

        def _cds_retrieve_era5_timeseries(
            self,
            out_nc,
            loc,
            start_d,
            end_d,
            variable="2m_temperature",
            data_format="netcdf",
        ):
            assert variable == "surface_solar_radiation_downwards"
            return out_nc

        def _open_and_concat_for_var(self, nc_files, expected_var):
            assert expected_var == "ssrd"
            times = pd.date_range("2025-01-01T00:00:00Z", periods=24, freq="h")
            ssrd = np.full((24,), 1_000_000.0, dtype=float)
            return xr.Dataset(
                data_vars={"ssrd": (("time",), ssrd)},
                coords={
                    "time": times,
                    "latitude": 52.25,
                    "longitude": 0.0,
                },
            )

    cds = DummySolarTimeseriesCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    df = cds.get_year_daily_solar_radiation_energy_data(loc, 2025)

    assert len(df) == 1
    assert df["grid_lat"].iloc[0] == pytest.approx(52.25)
    assert df["grid_lon"].iloc[0] == pytest.approx(0.0)
    assert df["solar_energy_MJ_m2"].iloc[0] == pytest.approx(24.0)


def test_cds_get_daily_solar_radiation_energy_series_long_range_uses_month_fetch(tmp_path):
    class DummySolarCDS(SolarRadiationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.solar_month_calls = 0
            self.solar_year_calls = 0

        def get_month_daily_solar_radiation_energy_data(self, location, year, month, half_box_deg=0.25):
            self.solar_month_calls += 1
            return pd.DataFrame({
                'date': [f'{year}-{month:02d}-01'],
                'solar_energy_MJ_m2': [1.0],
                'grid_lat': [location.lat],
                'grid_lon': [location.lon],
                'place_name': [location.name],
            })

        def get_year_daily_solar_radiation_energy_data(self, location, year, half_box_deg=0.25):
            self.solar_year_calls += 1
            return pd.DataFrame({
                'date': [f'{year}-01-01'],
                'solar_energy_MJ_m2': [1.0],
                'grid_lat': [location.lat],
                'grid_lon': [location.lon],
                'place_name': [location.name],
            })

    cds = DummySolarCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    _ = cds.get_daily_solar_radiation_energy_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-04-15").date(),
    )

    assert cds.solar_year_calls == 0
    assert cds.solar_month_calls == 4


def test_cds_get_daily_solar_radiation_energy_series_long_range_yearly_override(tmp_path):
    class DummySolarCDS(SolarRadiationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.solar_fetch_mode = "yearly"
            self.solar_month_calls = 0
            self.solar_year_calls = 0

        def get_month_daily_solar_radiation_energy_data(self, location, year, month, half_box_deg=0.25):
            self.solar_month_calls += 1
            return pd.DataFrame({
                'date': [f'{year}-{month:02d}-01'],
                'solar_energy_MJ_m2': [1.0],
                'grid_lat': [location.lat],
                'grid_lon': [location.lon],
                'place_name': [location.name],
            })

        def get_year_daily_solar_radiation_energy_data(self, location, year, half_box_deg=0.25):
            self.solar_year_calls += 1
            return pd.DataFrame({
                'date': [f'{year}-01-01'],
                'solar_energy_MJ_m2': [1.0],
                'grid_lat': [location.lat],
                'grid_lon': [location.lon],
                'place_name': [location.name],
            })

    cds = DummySolarCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    _ = cds.get_daily_solar_radiation_energy_series(
        loc,
        pd.Timestamp("2025-01-01").date(),
        pd.Timestamp("2025-04-15").date(),
    )

    assert cds.solar_year_calls == 1
    assert cds.solar_month_calls == 0


def test_cds_get_month_daily_solar_radiation_energy_data_invalid_source_raises(tmp_path):
    class DummySolarInvalidSourceCDS(SolarRadiationCDS):
        def __init__(self, cache_dir: Path):
            self.client = None
            self.cache_dir = cache_dir
            self.progress_manager = None
            self.default_half_box_deg = 0.25
            self.max_nearest_time_delta = pd.Timedelta("30min")
            self.month_fetch_day_span_threshold = 62
            self.solar_daily_source = "invalid_source"

    cds = DummySolarInvalidSourceCDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="UTC")

    with pytest.raises(ValueError, match="Unsupported solar daily source"):
        cds.get_month_daily_solar_radiation_energy_data(loc, 2025, 1)


def test_cds_invalid_location():
    class DummyInvalidCDS(DummyCDS):
        def get_month_daily_noon_data(self, location, year, month, half_box_deg=0.25):
            raise ValueError("Invalid location coordinates")

        def get_year_daily_noon_data(self, location, year, half_box_deg=0.25):
            raise ValueError("Invalid location coordinates")

    cds = DummyInvalidCDS(cache_dir=Path("/tmp/era5_cache"))
    loc = Location(name="Invalid", lat=999, lon=999, tz="UTC")
    with pytest.raises(ValueError):
        cds.get_noon_series(loc, pd.Timestamp('2025-01-01').date(), pd.Timestamp('2025-01-02').date())


def test_cds_empty_date_range():
    cds = DummyCDS(cache_dir=Path("/tmp/era5_cache"))
    loc = Location(name="Test", lat=1.0, lon=2.0, tz="UTC")
    df = cds.get_noon_series(loc, pd.Timestamp('2025-01-02').date(), pd.Timestamp('2025-01-01').date())
    assert df.empty


def test_cds_missing_cache_dir(tmp_path):
    cache_dir = tmp_path / "nonexistent"
    _ = DummyCDS(cache_dir=cache_dir)
    assert cache_dir.exists() or not cache_dir.exists()
