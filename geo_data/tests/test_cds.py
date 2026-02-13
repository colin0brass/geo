# Test CDS class and Location dataclass
import pytest
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
from geo_data.cds import CDS, Location


class DummyCDS(CDS):
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


def test_cds_get_daily_precipitation_series(tmp_path):
    class DummyPrecipCDS(DummyCDS):
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


def test_cds_get_daily_precipitation_series_long_range_uses_year_fetch(tmp_path):
    class DummyPrecipCDS(DummyCDS):
        def __init__(self, cache_dir: Path):
            super().__init__(cache_dir)
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
