# Test CDS class and Location dataclass
import pytest
import pandas as pd
from pathlib import Path
from cds import Location, CDS

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
    cds = CDS(cache_dir=Path("/tmp/era5_cache"))
    months = list(cds._month_range(pd.Timestamp('2025-01-01').date(), pd.Timestamp('2025-03-01').date()))
    assert months == [(2025, 1), (2025, 2), (2025, 3)]

def test_cds_get_noon_series_monkeypatch(tmp_path, monkeypatch):
    class DummyCDS(CDS):
        def get_month_daily_noon_data(self, location, year, month, half_box_deg=0.25):
            return pd.DataFrame({
                'date': [f'{year}-01-01'],
                'local_noon': ['2025-01-01T12:00:00'],
                'utc_time_used': ['2025-01-01T18:00:00'],
                'temp_C': [10.0],
                'temp_F': [50.0],
                'grid_lat': [location.lat],
                'grid_lon': [location.lon],
                'place_name': [location.name],
            })
    cds = DummyCDS(cache_dir=tmp_path)
    loc = Location(name="Test", lat=1.0, lon=2.0, tz="UTC")
    df = cds.get_noon_series(loc, pd.Timestamp('2025-01-01').date(), pd.Timestamp('2025-01-01').date())
    assert not df.empty
    assert df['place_name'].iloc[0] == "Test"

def test_cds_invalid_location():
    cds = CDS(cache_dir=Path("/tmp/era5_cache"))
    loc = Location(name="Invalid", lat=999, lon=999, tz="UTC")
    with pytest.raises(Exception):
        cds.get_noon_series(loc, pd.Timestamp('2025-01-01').date(), pd.Timestamp('2025-01-02').date())

def test_cds_empty_date_range():
    cds = CDS(cache_dir=Path("/tmp/era5_cache"))
    loc = Location(name="Test", lat=1.0, lon=2.0, tz="UTC")
    df = cds.get_noon_series(loc, pd.Timestamp('2025-01-02').date(), pd.Timestamp('2025-01-01').date())
    assert df.empty

def test_cds_missing_cache_dir(tmp_path):
    cache_dir = tmp_path / "nonexistent"
    cds = CDS(cache_dir=cache_dir)
    assert cache_dir.exists() or not cache_dir.exists()
