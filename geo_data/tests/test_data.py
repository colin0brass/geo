"""
Tests for retrieval and cache operations in the geo_data layer.
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock
from types import SimpleNamespace
import yaml
import geo_data.measure_mapping as measure_mapping
from geo_data.data_retrieval import (
    DATA_KEY,
    NOON_TEMP_VAR,
    RetrievalCoordinator,
    SCHEMA_VERSION,
)
from geo_data.cds_base import Location
from geo_data.schema import Schema
from geo_data.cache_store import CacheStore
from geo_data.measure_mapping import MeasureRegistry


cache_store = CacheStore()


@pytest.fixture(autouse=True)
def _cache_store_fixture():
    globals()['cache_store'] = CacheStore()


def test_read_and_save_data_file(tmp_path):
    """Test reading and saving YAML data files."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")
    df = pd.DataFrame({
        'date': ['2025-01-01', '2025-01-02'],
        'temp_C': [10.0, 12.0],
        'temp_F': [50.0, 53.6],
        'grid_lat': [40.0, 40.0],
        'grid_lon': [-73.0, -73.0],
        'place_name': ['Test', 'Test'],
    })
    out_file = tmp_path / "test.yaml"
    cache_store.save_data_file(df, out_file, loc)

    # Verify file was created
    assert out_file.exists()

    # Verify v2 schema structure
    with open(out_file, 'r') as f:
        raw = yaml.safe_load(f)
    assert raw['schema_version'] == SCHEMA_VERSION
    assert 'variables' in raw
    assert DATA_KEY in raw
    assert NOON_TEMP_VAR in raw[DATA_KEY]

    # Read it back
    df2 = cache_store.read_data_file(out_file)
    assert not df2.empty
    assert len(df2) == 2
    assert df2['date'].iloc[0] == pd.Timestamp('2025-01-01')
    assert df2['temp_C'].iloc[0] == 10.0


def test_schema_class_properties(tmp_path):
    """Schema class exposes expected metadata and loader."""
    schema = Schema.load()
    assert schema.version == SCHEMA_VERSION
    assert schema.data_key == DATA_KEY
    assert schema.primary_variable == NOON_TEMP_VAR

    loaded = Schema.load_registry()
    assert isinstance(loaded, dict)
    assert loaded['current_version'] == SCHEMA_VERSION


def test_cache_store_class_roundtrip(tmp_path):
    """CacheStore class mirrors function-based cache read/write behavior."""
    cache_store = CacheStore()
    loc = Location(name="Store Test", lat=40.0, lon=-73.0, tz="America/New_York")
    df = pd.DataFrame({
        'date': ['2025-01-01'],
        'temp_C': [10.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    out_file = tmp_path / "store_test.yaml"
    cache_store.save_data_file(df, out_file, loc)
    loaded = cache_store.read_data_file(out_file)
    assert len(loaded) == 1
    assert loaded['temp_C'].iloc[0] == 10.0


def test_retrieval_coordinator_single_location(tmp_path, monkeypatch):
    """RetrievalCoordinator class retrieves data via measure-specific client."""
    loc = Location(name="Coord City", lat=40.0, lon=-73.0, tz="America/New_York")

    mock_cds = MagicMock()
    mock_df = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'place_name': ['Coord City'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    mock_cds.get_noon_series.return_value = mock_df

    def mock_cds_init(cache_dir, progress_manager=None, config_path=None):
        return mock_cds

    monkeypatch.setattr('geo_data.data_retrieval.TemperatureCDS', mock_cds_init)

    coordinator = RetrievalCoordinator(
        cache_dir=tmp_path,
        data_cache_dir=tmp_path,
    )
    result = coordinator.retrieve([loc], 2024, 2024)
    assert not result.empty
    assert 'Coord City' in result['place_name'].values
    mock_cds.get_noon_series.assert_called_once()


def test_save_data_file_creates_directory(tmp_path):
    """Test that save_data_file creates output directory if needed."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")
    df = pd.DataFrame({
        'date': ['2025-01-01'],
        'temp_C': [10.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    nested_path = tmp_path / "nested" / "dir" / "test.yaml"

    cache_store.save_data_file(df, nested_path, loc)
    assert nested_path.exists()
    assert nested_path.parent.is_dir()


def test_read_data_file_missing_file(tmp_path):
    """Test reading a non-existent file raises appropriate error."""
    missing_file = tmp_path / "missing.yaml"
    with pytest.raises(FileNotFoundError):
        cache_store.read_data_file(missing_file)


def test_read_data_file_date_parsing(tmp_path):
    """Test that date column is properly parsed as datetime."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")
    df = pd.DataFrame({
        'date': ['2025-01-15', '2025-02-20'],
        'temp_C': [15.0, 18.0],
        'grid_lat': [40.0, 40.0],
        'grid_lon': [-73.0, -73.0],
    })
    out_file = tmp_path / "dates.yaml"
    cache_store.save_data_file(df, out_file, loc)

    df2 = cache_store.read_data_file(out_file)
    assert pd.api.types.is_datetime64_any_dtype(df2['date'])
    assert df2['date'].iloc[0].year == 2025
    assert df2['date'].iloc[0].month == 1
    assert df2['date'].iloc[0].day == 15


def test_coordinator_retrieve_single_location(tmp_path, monkeypatch):
    """RetrievalCoordinator retrieves data for a single location."""
    loc = Location(name="Test City", lat=40.0, lon=-73.0, tz="America/New_York")

    # Mock CDS to avoid actual API calls
    mock_cds = MagicMock()
    mock_df = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'place_name': ['Test City'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    mock_cds.get_noon_series.return_value = mock_df

    def mock_cds_init(cache_dir, progress_manager=None, config_path=None):
        return mock_cds

    monkeypatch.setattr('geo_data.data_retrieval.TemperatureCDS', mock_cds_init)

    result = RetrievalCoordinator(cache_dir=tmp_path, data_cache_dir=tmp_path).retrieve(
        [loc], 2024, 2024
    )

    assert not result.empty
    assert 'Test City' in result['place_name'].values
    mock_cds.get_noon_series.assert_called_once()


def test_coordinator_retrieve_precipitation_measure(tmp_path, monkeypatch):
    """RetrievalCoordinator routes precipitation measure to the correct CDS method."""
    loc = Location(name="Rain City", lat=40.0, lon=-73.0, tz="America/New_York")

    mock_cds = MagicMock()
    mock_df = pd.DataFrame({
        'date': ['2024-01-01'],
        'precip_mm': [2.5],
        'place_name': ['Rain City'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    mock_cds.get_daily_precipitation_series.return_value = mock_df

    def mock_cds_init(cache_dir, progress_manager=None, config_path=None):
        return mock_cds

    monkeypatch.setattr('geo_data.data_retrieval.PrecipitationCDS', mock_cds_init)

    result = RetrievalCoordinator(cache_dir=tmp_path, data_cache_dir=tmp_path).retrieve(
        [loc],
        2024,
        2024,
        measure='daily_precipitation',
    )

    assert not result.empty
    assert 'precip_mm' in result.columns
    assert result['precip_mm'].iloc[0] == 2.5
    mock_cds.get_daily_precipitation_series.assert_called_once()


def test_coordinator_retrieve_solar_radiation_measure(tmp_path, monkeypatch):
    """RetrievalCoordinator routes solar-radiation measure to the correct CDS method."""
    loc = Location(name="Solar City", lat=40.0, lon=-73.0, tz="America/New_York")

    mock_cds = MagicMock()
    mock_df = pd.DataFrame({
        'date': ['2024-01-01'],
        'solar_energy_MJ_m2': [8.25],
        'place_name': ['Solar City'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    mock_cds.get_daily_solar_radiation_energy_series.return_value = mock_df

    def mock_cds_init(cache_dir, progress_manager=None, config_path=None):
        return mock_cds

    monkeypatch.setattr('geo_data.data_retrieval.SolarRadiationCDS', mock_cds_init)

    result = RetrievalCoordinator(cache_dir=tmp_path, data_cache_dir=tmp_path).retrieve(
        [loc],
        2024,
        2024,
        measure='daily_solar_radiation_energy',
    )

    assert not result.empty
    assert 'solar_energy_MJ_m2' in result.columns
    assert result['solar_energy_MJ_m2'].iloc[0] == 8.25
    mock_cds.get_daily_solar_radiation_energy_series.assert_called_once()


def test_coordinator_override_fetch_mode_temperature(tmp_path, monkeypatch):
    """RetrievalCoordinator applies runtime month override for temperature."""
    loc = Location(name="Temp City", lat=40.0, lon=-73.0, tz="America/New_York")

    mock_cds = MagicMock()
    mock_cds.temp_fetch_mode = 'auto'
    mock_cds.get_noon_series.return_value = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'place_name': ['Temp City'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })

    def mock_cds_init(cache_dir, progress_manager=None, config_path=None):
        return mock_cds

    monkeypatch.setattr('geo_data.data_retrieval.TemperatureCDS', mock_cds_init)

    RetrievalCoordinator(
        cache_dir=tmp_path,
        data_cache_dir=tmp_path,
        fetch_mode_override='month',
    ).retrieve([loc], 2024, 2024, measure='noon_temperature')

    assert mock_cds.temp_fetch_mode == 'monthly'


def test_coordinator_override_fetch_mode_precipitation(tmp_path, monkeypatch):
    """RetrievalCoordinator applies runtime year override for precipitation."""
    loc = Location(name="Rain City", lat=40.0, lon=-73.0, tz="America/New_York")

    mock_cds = MagicMock()
    mock_cds.precipitation_fetch_mode = 'auto'
    mock_cds.get_daily_precipitation_series.return_value = pd.DataFrame({
        'date': ['2024-01-01'],
        'precip_mm': [2.5],
        'place_name': ['Rain City'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })

    def mock_cds_init(cache_dir, progress_manager=None, config_path=None):
        return mock_cds

    monkeypatch.setattr('geo_data.data_retrieval.PrecipitationCDS', mock_cds_init)

    RetrievalCoordinator(
        cache_dir=tmp_path,
        data_cache_dir=tmp_path,
        fetch_mode_override='year',
    ).retrieve([loc], 2024, 2024, measure='daily_precipitation')

    assert mock_cds.precipitation_fetch_mode == 'yearly'


def test_coordinator_override_fetch_mode_solar(tmp_path, monkeypatch):
    """RetrievalCoordinator applies runtime month override for solar."""
    loc = Location(name="Solar City", lat=40.0, lon=-73.0, tz="America/New_York")

    mock_cds = MagicMock()
    mock_cds.solar_fetch_mode = 'auto'
    mock_cds.get_daily_solar_radiation_energy_series.return_value = pd.DataFrame({
        'date': ['2024-01-01'],
        'solar_energy_MJ_m2': [8.25],
        'place_name': ['Solar City'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })

    def mock_cds_init(cache_dir, progress_manager=None, config_path=None):
        return mock_cds

    monkeypatch.setattr('geo_data.data_retrieval.SolarRadiationCDS', mock_cds_init)

    RetrievalCoordinator(
        cache_dir=tmp_path,
        data_cache_dir=tmp_path,
        fetch_mode_override='month',
    ).retrieve([loc], 2024, 2024, measure='daily_solar_radiation_energy')

    assert mock_cds.solar_fetch_mode == 'monthly'


def test_read_and_save_data_file_precipitation_measure(tmp_path):
    """Test precipitation measure cache round-trip."""
    loc = Location(name="Rain Test", lat=40.0, lon=-73.0, tz="America/New_York")
    df = pd.DataFrame({
        'date': ['2025-01-01', '2025-01-02'],
        'precip_mm': [1.0, 2.5],
        'grid_lat': [40.0, 40.0],
        'grid_lon': [-73.0, -73.0],
        'place_name': ['Rain Test', 'Rain Test'],
    })
    out_file = tmp_path / "rain.yaml"

    cache_store.save_data_file(df, out_file, loc, measure='daily_precipitation')
    df2 = cache_store.read_data_file(out_file, measure='daily_precipitation')

    assert not df2.empty
    assert 'precip_mm' in df2.columns
    assert df2['precip_mm'].tolist() == [1.0, 2.5]
    assert 'temp_F' not in df2.columns


def test_read_and_save_data_file_solar_radiation_measure(tmp_path):
    """Test solar-radiation measure cache round-trip."""
    loc = Location(name="Solar Test", lat=40.0, lon=-73.0, tz="America/New_York")
    df = pd.DataFrame({
        'date': ['2025-01-01', '2025-01-02'],
        'solar_energy_MJ_m2': [3.5, 4.75],
        'grid_lat': [40.0, 40.0],
        'grid_lon': [-73.0, -73.0],
        'place_name': ['Solar Test', 'Solar Test'],
    })
    out_file = tmp_path / "solar.yaml"

    cache_store.save_data_file(df, out_file, loc, measure='daily_solar_radiation_energy')
    df2 = cache_store.read_data_file(out_file, measure='daily_solar_radiation_energy')

    assert not df2.empty
    assert 'solar_energy_MJ_m2' in df2.columns
    assert df2['solar_energy_MJ_m2'].tolist() == [3.5, 4.75]
    assert 'temp_F' not in df2.columns


def test_coordinator_invalid_fetch_mode_override(tmp_path):
    """RetrievalCoordinator rejects unknown runtime fetch mode overrides."""
    with pytest.raises(ValueError, match="fetch_mode_override"):
        RetrievalCoordinator(
            cache_dir=tmp_path,
            data_cache_dir=tmp_path,
            fetch_mode_override='weekly',
        )


def test_coordinator_retrieve_multiple_locations(tmp_path, monkeypatch):
    """RetrievalCoordinator retrieves and concatenates data for multiple locations."""
    loc1 = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    loc2 = Location(name="City B", lat=51.5, lon=-0.1, tz="Europe/London")

    mock_cds = MagicMock()

    def mock_get_noon_series(loc, start_d, end_d, notify_progress=True):
        return pd.DataFrame({
            'date': ['2024-01-01'],
            'temp_C': [10.0],
            'place_name': [loc.name],
            'grid_lat': [loc.lat],
            'grid_lon': [loc.lon],
        })

    mock_cds.get_noon_series.side_effect = mock_get_noon_series

    def mock_cds_init(cache_dir, progress_manager=None, config_path=None):
        return mock_cds

    monkeypatch.setattr('geo_data.data_retrieval.TemperatureCDS', mock_cds_init)

    result = RetrievalCoordinator(cache_dir=tmp_path, data_cache_dir=tmp_path).retrieve(
        [loc1, loc2], 2024, 2024
    )

    assert not result.empty
    assert len(result) == 2
    assert 'City A' in result['place_name'].values
    assert 'City B' in result['place_name'].values
    assert mock_cds.get_noon_series.call_count == 2


def test_coordinator_retrieve_caches_to_yaml(tmp_path, monkeypatch):
    """RetrievalCoordinator persists fetched data to YAML cache files."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")

    mock_cds = MagicMock()
    mock_df = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'temp_F': [50.0],
        'place_name': ['Test'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    mock_cds.get_noon_series.return_value = mock_df

    monkeypatch.setattr('geo_data.data_retrieval.TemperatureCDS', lambda cache_dir, progress_manager=None, config_path=None: mock_cds)

    data_cache_dir = tmp_path / "data_cache"
    RetrievalCoordinator(cache_dir=tmp_path, data_cache_dir=data_cache_dir).retrieve([loc], 2024, 2024)

    # Check that YAML file was created in data_cache_dir
    yaml_files = list(data_cache_dir.glob("*.yaml"))
    assert len(yaml_files) > 0
    assert any('Test' in str(f) for f in yaml_files)


def test_get_cached_years_with_valid_file(tmp_path):
    """Test get_cached_years with a valid YAML file."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")
    df = pd.DataFrame({
        'date': ['2024-01-01', '2024-06-15', '2025-01-01'],
        'temp_C': [10.0, 20.0, 15.0],
        'grid_lat': [40.0, 40.0, 40.0],
        'grid_lon': [-73.0, -73.0, -73.0],
    })
    yaml_file = tmp_path / "test.yaml"
    cache_store.save_data_file(df, yaml_file, loc)

    cached_years = cache_store.get_cached_years(yaml_file)
    assert cached_years == {2024, 2025}


def test_get_cached_years_with_nonexistent_file(tmp_path):
    """Test get_cached_years with non-existent file returns empty set."""
    yaml_file = tmp_path / "nonexistent.yaml"
    cached_years = cache_store.get_cached_years(yaml_file)
    assert cached_years == set()


def test_get_cached_years_with_corrupted_yaml(tmp_path):
    """Test get_cached_years handles corrupted YAML gracefully."""
    yaml_file = tmp_path / "corrupted.yaml"
    with open(yaml_file, 'w') as f:
        f.write("this is not valid: yaml: syntax: [[[")

    cached_years = cache_store.get_cached_years(yaml_file)
    assert cached_years == set()


def test_get_cached_years_with_missing_temperatures_key(tmp_path):
    """Test get_cached_years with missing data key."""
    yaml_file = tmp_path / "no_temps.yaml"
    with open(yaml_file, 'w') as f:
        yaml.dump({'place': {'name': 'Test'}}, f)

    cached_years = cache_store.get_cached_years(yaml_file)
    assert cached_years == set()


def test_save_data_file_append_mode(tmp_path):
    """Test appending data to existing YAML file."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")

    # Save initial data for 2024
    df_2024 = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02'],
        'temp_C': [10.0, 11.0],
        'grid_lat': [40.0, 40.0],
        'grid_lon': [-73.0, -73.0],
    })
    yaml_file = tmp_path / "test.yaml"
    cache_store.save_data_file(df_2024, yaml_file, loc, append=False)

    # Append 2025 data
    df_2025 = pd.DataFrame({
        'date': ['2025-01-01', '2025-01-02'],
        'temp_C': [15.0, 16.0],
        'grid_lat': [40.0, 40.0],
        'grid_lon': [-73.0, -73.0],
    })
    cache_store.save_data_file(df_2025, yaml_file, loc, append=True)

    # Verify both years are present
    cached_years = cache_store.get_cached_years(yaml_file)
    assert cached_years == {2024, 2025}

    # Read back and verify all data
    df_result = cache_store.read_data_file(yaml_file)
    assert len(df_result) == 4
    assert 2024 in df_result['date'].dt.year.values
    assert 2025 in df_result['date'].dt.year.values


def test_save_data_file_append_with_corrupted_file(tmp_path):
    """Test that append mode handles corrupted existing file by overwriting."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")

    # Create corrupted YAML file
    yaml_file = tmp_path / "corrupted.yaml"
    with open(yaml_file, 'w') as f:
        f.write("invalid: yaml: [[[")

    # Append should detect corruption and overwrite
    df = pd.DataFrame({
        'date': ['2025-01-01'],
        'temp_C': [15.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    cache_store.save_data_file(df, yaml_file, loc, append=True)

    # Verify file is now valid
    df_result = cache_store.read_data_file(yaml_file)
    assert len(df_result) == 1
    assert df_result['temp_C'].iloc[0] == 15.0


def test_save_data_file_merge_overwrites_existing_dates(tmp_path):
    """Test that merging overwrites data for existing dates."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")

    # Save initial data
    df_initial = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    yaml_file = tmp_path / "test.yaml"
    cache_store.save_data_file(df_initial, yaml_file, loc)

    # Append with same date but different temperature
    df_update = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [20.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    cache_store.save_data_file(df_update, yaml_file, loc, append=True)

    # Verify temperature was updated
    df_result = cache_store.read_data_file(yaml_file)
    assert len(df_result) == 1
    assert df_result['temp_C'].iloc[0] == 20.0


def test_read_data_file_with_year_filtering(tmp_path):
    """Test reading data with year range filtering."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")

    # Create data spanning multiple years
    df = pd.DataFrame({
        'date': ['2023-01-01', '2024-01-01', '2025-01-01', '2026-01-01'],
        'temp_C': [10.0, 15.0, 20.0, 25.0],
        'grid_lat': [40.0, 40.0, 40.0, 40.0],
        'grid_lon': [-73.0, -73.0, -73.0, -73.0],
    })
    yaml_file = tmp_path / "test.yaml"
    cache_store.save_data_file(df, yaml_file, loc)

    # Test start_year filter
    df_filtered = cache_store.read_data_file(yaml_file, start_year=2024)
    assert len(df_filtered) == 3
    assert df_filtered['date'].dt.year.min() == 2024

    # Test end_year filter
    df_filtered = cache_store.read_data_file(yaml_file, end_year=2025)
    assert len(df_filtered) == 3
    assert df_filtered['date'].dt.year.max() == 2025

    # Test both filters
    df_filtered = cache_store.read_data_file(yaml_file, start_year=2024, end_year=2025)
    assert len(df_filtered) == 2
    assert set(df_filtered['date'].dt.year.values) == {2024, 2025}


def test_coordinator_retrieve_uses_cached_years(tmp_path, monkeypatch):
    """RetrievalCoordinator uses cached data when requested years are present."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")

    # Pre-populate cache with 2024 data
    df_2024 = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    data_cache_dir = tmp_path / "data_cache"
    data_cache_dir.mkdir()
    yaml_file = data_cache_dir / "Test.yaml"
    cache_store.save_data_file(df_2024, yaml_file, loc)

    # Mock CDS for 2025
    mock_cds = MagicMock()
    df_2025 = pd.DataFrame({
        'date': ['2025-01-01'],
        'temp_C': [15.0],
        'place_name': ['Test'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    mock_cds.get_noon_series.return_value = df_2025
    monkeypatch.setattr('geo_data.data_retrieval.TemperatureCDS', lambda cache_dir, progress_manager=None, config_path=None: mock_cds)

    # Request 2024-2025, should only fetch 2025
    result = RetrievalCoordinator(cache_dir=tmp_path, data_cache_dir=data_cache_dir).retrieve(
        [loc], 2024, 2025
    )

    # Should have both years
    assert len(result) == 2
    assert set(result['date'].dt.year.values) == {2024, 2025}

    # CDS should only be called once for 2025
    mock_cds.get_noon_series.assert_called_once()


def test_coordinator_retrieve_all_cached(tmp_path, monkeypatch):
    """RetrievalCoordinator skips CDS calls when all requested data is cached."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")

    # Pre-populate cache with all requested years
    df = pd.DataFrame({
        'date': ['2024-01-01', '2025-01-01'],
        'temp_C': [10.0, 15.0],
        'grid_lat': [40.0, 40.0],
        'grid_lon': [-73.0, -73.0],
    })
    data_cache_dir = tmp_path / "data_cache"
    data_cache_dir.mkdir()
    yaml_file = data_cache_dir / "Test.yaml"
    cache_store.save_data_file(df, yaml_file, loc)

    # Mock CDS to detect if it's called
    mock_cds = MagicMock()
    monkeypatch.setattr('geo_data.data_retrieval.TemperatureCDS', lambda cache_dir, progress_manager=None, config_path=None: mock_cds)

    # Request cached years only
    result = RetrievalCoordinator(cache_dir=tmp_path, data_cache_dir=data_cache_dir).retrieve(
        [loc], 2024, 2025
    )

    # Should have data
    assert len(result) == 2

    # CDS should NOT be called at all
    mock_cds.get_noon_series.assert_not_called()


def test_save_data_file_key_normalization(tmp_path):
    """Test that YAML keys are normalized to integers during merge."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")
    yaml_file = tmp_path / "test.yaml"

    # Create initial file with integer keys
    df_2024 = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    cache_store.save_data_file(df_2024, yaml_file, loc)

    # Manually inject string keys (simulating old bug)
    with open(yaml_file, 'r') as f:
        data = yaml.safe_load(f)
    # Add a year as string (simulating the bug)
    data[DATA_KEY][NOON_TEMP_VAR]['2023'] = {1: {1: 5.0}}
    with open(yaml_file, 'w') as f:
        yaml.dump(data, f)

    # Append new data (should normalize all keys)
    df_2025 = pd.DataFrame({
        'date': ['2025-01-01'],
        'temp_C': [15.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    cache_store.save_data_file(df_2025, yaml_file, loc, append=True)

    # Verify all years are detected (including the string key)
    cached_years = cache_store.get_cached_years(yaml_file)
    assert cached_years == {2023, 2024, 2025}


def test_read_data_file_legacy_temperatures_key(tmp_path):
    """Unversioned legacy cache documents should be rejected."""
    yaml_file = tmp_path / "legacy.yaml"
    legacy = {
        'place': {
            'name': 'Legacy Place',
            'lat': 40.0,
            'lon': -73.0,
            'timezone': 'America/New_York',
            'grid_lat': 40.0,
            'grid_lon': -73.0,
        },
        'temperatures': {
            2024: {
                1: {1: 12.5}
            }
        }
    }
    with open(yaml_file, 'w') as f:
        yaml.safe_dump(legacy, f)

    with pytest.raises(ValueError, match='unversioned cache documents are no longer supported'):
        cache_store.read_data_file(yaml_file)


def test_read_data_file_schema_v1_auto_migrates(tmp_path):
    """Test one-way migration from schema v1 to current schema."""
    yaml_file = tmp_path / "v1.yaml"
    v1_doc = {
        'schema_version': 1,
        'place': {
            'name': 'V1 Place',
            'lat': 40.0,
            'lon': -73.0,
            'timezone': 'America/New_York',
            'grid_lat': 40.0,
            'grid_lon': -73.0,
        },
        'noon_temperatures': {
            2024: {
                1: {1: 14.25}
            }
        }
    }
    with open(yaml_file, 'w') as f:
        yaml.safe_dump(v1_doc, f)

    df = cache_store.read_data_file(yaml_file)
    assert len(df) == 1
    assert df['temp_C'].iloc[0] == 14.25

    with open(yaml_file, 'r') as f:
        migrated = yaml.safe_load(f)
    assert migrated['schema_version'] == SCHEMA_VERSION
    assert DATA_KEY in migrated and NOON_TEMP_VAR in migrated[DATA_KEY]


def test_read_data_file_schema_v1_uses_field_mapping_candidates(tmp_path):
    """Test v1->v2 migration using schema-defined source_candidates mapping."""
    yaml_file = tmp_path / "v1_mapping.yaml"
    v1_doc = {
        'schema_version': 1,
        'place': {
            'name': 'V1 Mapping Place',
            'lat': 40.0,
            'lon': -73.0,
            'timezone': 'America/New_York',
            'grid_lat': 40.0,
            'grid_lon': -73.0,
        },
        'temp_map_v1': {
            2024: {
                1: {1: 9.5}
            }
        }
    }
    with open(yaml_file, 'w') as f:
        yaml.safe_dump(v1_doc, f)

    df = cache_store.read_data_file(yaml_file)
    assert len(df) == 1
    assert df['temp_C'].iloc[0] == 9.5

    with open(yaml_file, 'r') as f:
        migrated = yaml.safe_load(f)
    assert migrated['schema_version'] == SCHEMA_VERSION
    assert DATA_KEY in migrated and NOON_TEMP_VAR in migrated[DATA_KEY]


def test_read_data_file_rejects_newer_schema_version(tmp_path):
    """Test that cache files with unsupported newer schema versions are rejected."""
    yaml_file = tmp_path / "future.yaml"
    future_doc = {
        'schema_version': SCHEMA_VERSION + 1,
        'place': {
            'name': 'Future Place',
            'lat': 40.0,
            'lon': -73.0,
            'timezone': 'America/New_York',
            'grid_lat': 40.0,
            'grid_lon': -73.0,
        },
        'data': {'noon_temp_C': {2024: {1: {1: 10.0}}}},
    }
    with open(yaml_file, 'w') as f:
        yaml.safe_dump(future_doc, f)

    with pytest.raises(ValueError, match='newer schema_version'):
        cache_store.read_data_file(yaml_file)


def test_read_data_file_rejects_unsupported_older_schema_version(tmp_path):
    """Unknown older schema versions should be rejected, not guessed."""
    yaml_file = tmp_path / "v0.yaml"
    v0_doc = {
        'schema_version': 0,
        'place': {
            'name': 'Old Place',
            'lat': 40.0,
            'lon': -73.0,
            'timezone': 'America/New_York',
            'grid_lat': 40.0,
            'grid_lon': -73.0,
        },
        'temperatures': {2024: {1: {1: 10.0}}},
    }
    with open(yaml_file, 'w') as f:
        yaml.safe_dump(v0_doc, f)

    with pytest.raises(ValueError, match='unsupported schema_version'):
        cache_store.read_data_file(yaml_file)


def test_schema_loader_rejects_invalid_required_list_type(tmp_path):
    """Schema loader should fail if required_* fields are malformed."""
    schema_file = tmp_path / "schema.yaml"
    schema_file.write_text(
        """
current_version: 2
versions:
  1:
    required: place
  2:
    data_key: data
    variables_key: variables
    primary_variable: noon_temp_C
""".strip()
    )

    with pytest.raises(ValueError, match="required"):
        Schema.load_registry(schema_file)


def test_measure_value_columns_can_be_loaded_from_schema(monkeypatch):
    """measure_value_columns should be loaded from schema when configured."""
    fake_schema = SimpleNamespace(current={
        'measure_value_columns': {
            'noon_temperature': 'temp_c_custom',
            'daily_precipitation': 'rain_mm_custom',
            'daily_solar_radiation_energy': 'solar_custom',
        }
    })
    monkeypatch.setattr(measure_mapping, 'DEFAULT_SCHEMA', fake_schema)

    loaded = MeasureRegistry._load_measure_to_value_column_mapping()
    assert loaded['noon_temperature'] == 'temp_c_custom'
    assert loaded['daily_precipitation'] == 'rain_mm_custom'
    assert loaded['daily_solar_radiation_energy'] == 'solar_custom'


def test_measure_value_columns_rejects_missing_required_measure(monkeypatch):
    """measure_value_columns should fail validation when required measures are missing."""
    fake_schema = SimpleNamespace(current={
        'measure_value_columns': {
            'noon_temperature': 'temp_c_custom',
        }
    })
    monkeypatch.setattr(measure_mapping, 'DEFAULT_SCHEMA', fake_schema)

    with pytest.raises(ValueError, match='measure_value_columns'):
        MeasureRegistry._load_measure_to_value_column_mapping()


def test_measure_registry_builds_from_schema(monkeypatch):
    """MeasureRegistry should build both mappings from schema metadata."""
    fake_schema = SimpleNamespace(
        primary_variable='noon_temp_custom',
        current={
            'measure_cache_vars': {
                'noon_temperature': 'noon_temp_custom',
                'daily_precipitation': 'daily_precip_custom',
                'daily_solar_radiation_energy': 'daily_solar_custom',
            },
            'measure_value_columns': {
                'noon_temperature': 'temp_custom',
                'daily_precipitation': 'precip_custom',
                'daily_solar_radiation_energy': 'solar_custom',
            },
        },
    )
    monkeypatch.setattr(measure_mapping, 'DEFAULT_SCHEMA', fake_schema)

    registry = MeasureRegistry.from_schema()
    assert registry.get_cache_var('noon_temperature') == 'noon_temp_custom'
    assert registry.get_value_column('daily_precipitation') == 'precip_custom'
    assert registry.get_value_column('daily_solar_radiation_energy') == 'solar_custom'


def test_read_data_file_schema_v1_missing_required_place_field_fails(tmp_path):
    """Schema v1 documents missing required place fields should fail migration."""
    yaml_file = tmp_path / "v1_missing_place_field.yaml"
    v1_doc = {
        'schema_version': 1,
        'place': {
            'name': 'V1 Place',
            'lat': 40.0,
            'lon': -73.0,
            'timezone': 'America/New_York',
            'grid_lat': 40.0,
        },
        'noon_temperatures': {
            2024: {
                1: {1: 14.25}
            }
        }
    }
    with open(yaml_file, 'w') as f:
        yaml.safe_dump(v1_doc, f)

    with pytest.raises(ValueError, match='missing required path'):
        cache_store.read_data_file(yaml_file)


def test_coordinator_retrieve_prints_cds_summary(tmp_path, monkeypatch, capsys):
    """RetrievalCoordinator prints a CDS retrieval summary for uncached places."""
    loc1 = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    loc2 = Location(name="City B", lat=51.5, lon=-0.1, tz="Europe/London")

    mock_cds = MagicMock()
    mock_df = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'place_name': ['Test'],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    mock_cds.get_noon_series.return_value = mock_df

    def mock_cds_init(cache_dir, progress_manager=None, config_path=None):
        return mock_cds

    monkeypatch.setattr('geo_data.data_retrieval.TemperatureCDS', mock_cds_init)

    # Call with fresh locations (no cache)
    RetrievalCoordinator(cache_dir=tmp_path, data_cache_dir=tmp_path).retrieve([loc1, loc2], 2024, 2024)

    captured = capsys.readouterr()

    # Check for summary message
    assert "CDS Retrieval Required: 2 place(s)" in captured.out
    assert "City A" in captured.out
    assert "City B" in captured.out
    assert "=" in captured.out  # Separator lines


def test_coordinator_retrieve_prints_all_cached_message(tmp_path, monkeypatch, capsys):
    """RetrievalCoordinator prints an all-cached message when no CDS retrieval is needed."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")

    # Pre-populate cache
    df = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    data_cache_dir = tmp_path / "data_cache"
    data_cache_dir.mkdir()
    yaml_file = data_cache_dir / "Test.yaml"
    cache_store.save_data_file(df, yaml_file, loc)

    mock_cds = MagicMock()
    monkeypatch.setattr('geo_data.data_retrieval.TemperatureCDS', lambda cache_dir, progress_manager=None, config_path=None: mock_cds)

    # Call with cached data
    RetrievalCoordinator(cache_dir=tmp_path, data_cache_dir=data_cache_dir).retrieve([loc], 2024, 2024)

    captured = capsys.readouterr()

    # Check for "all cached" message
    assert "All data already cached - no CDS retrieval needed" in captured.out
    assert "=" in captured.out  # Separator lines

    # Verify CDS was not called
    mock_cds.get_noon_series.assert_not_called()
