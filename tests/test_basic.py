import pytest
import pandas as pd
from pathlib import Path
from plot import Visualizer
from cds import Location, CDS

def test_temp_c_to_f():
    assert Visualizer.temp_c_to_f(0) == 32.0
    assert Visualizer.temp_c_to_f(100) == 212.0
    assert Visualizer.temp_c_to_f(-40) == -40.0

def test_add_data_fields():
    df = pd.DataFrame({'date': ['2025-01-01', '2025-01-02'], 'temp_C': [10, 12]})
    vis = Visualizer(df)
    df2 = vis.add_data_fields(df.copy())
    assert 'day_of_year' in df2.columns
    assert 'angle' in df2.columns
    assert df2['day_of_year'].iloc[0] == 1
    assert df2['angle'].iloc[0] == 0.0

def test_location_dataclass():
    loc = Location(name="Test", lat=1.0, lon=2.0, tz="UTC")
    assert loc.name == "Test"
    assert loc.lat == 1.0
    assert loc.lon == 2.0
    assert loc.tz == "UTC"

def test_cds_month_range():
    cds = CDS(cache_dir=Path("/tmp/era5_cache"))
    months = list(cds._month_range(pd.Timestamp('2025-01-01').date(), pd.Timestamp('2025-03-01').date()))
    assert months == [(2025, 1), (2025, 2), (2025, 3)]

def test_visualizer_init_and_error():
    df = pd.DataFrame({'date': ['2025-01-01'], 'temp_C': [10]})
    vis = Visualizer(df)
    assert vis.df is not None
    assert vis.tmin_c == 10
    assert vis.tmax_c == 10
    # Test error on empty DataFrame
    with pytest.raises(ValueError):
        Visualizer(pd.DataFrame())

def test_visualizer_temp_c_to_f_edge_cases():
    # Test float and negative values
    assert Visualizer.temp_c_to_f(37.5) == 99.5
    assert Visualizer.temp_c_to_f(-273.15) == pytest.approx(-459.67, abs=0.01)

def test_cds_get_noon_series_monkeypatch(tmp_path, monkeypatch):
    # Patch get_month_daily_noon_data to avoid real data download
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
