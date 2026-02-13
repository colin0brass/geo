"""
Tests for data module (data retrieval and I/O operations).
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock
import yaml
from data import (
    DATA_KEY,
    NOON_TEMP_VAR,
    SCHEMA_VERSION,
    _load_cache_schema_registry,
    get_cached_years,
    read_data_file,
    retrieve_and_concat_data,
    save_data_file,
)
from cds import Location


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
    save_data_file(df, out_file, loc)

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
    df2 = read_data_file(out_file)
    assert not df2.empty
    assert len(df2) == 2
    assert df2['date'].iloc[0] == pd.Timestamp('2025-01-01')
    assert df2['temp_C'].iloc[0] == 10.0


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

    save_data_file(df, nested_path, loc)
    assert nested_path.exists()
    assert nested_path.parent.is_dir()


def test_read_data_file_missing_file(tmp_path):
    """Test reading a non-existent file raises appropriate error."""
    missing_file = tmp_path / "missing.yaml"
    with pytest.raises(FileNotFoundError):
        read_data_file(missing_file)


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
    save_data_file(df, out_file, loc)

    df2 = read_data_file(out_file)
    assert pd.api.types.is_datetime64_any_dtype(df2['date'])
    assert df2['date'].iloc[0].year == 2025
    assert df2['date'].iloc[0].month == 1
    assert df2['date'].iloc[0].day == 15


def test_retrieve_and_concat_data_single_location(tmp_path, monkeypatch):
    """Test retrieving data for a single location."""
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

    def mock_cds_init(cache_dir, progress_manager=None):
        return mock_cds

    monkeypatch.setattr('data.CDS', mock_cds_init)

    result = retrieve_and_concat_data([loc], 2024, 2024, tmp_path, tmp_path)

    assert not result.empty
    assert 'Test City' in result['place_name'].values
    mock_cds.get_noon_series.assert_called_once()


def test_retrieve_and_concat_data_multiple_locations(tmp_path, monkeypatch):
    """Test retrieving and concatenating data for multiple locations."""
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

    def mock_cds_init(cache_dir, progress_manager=None):
        return mock_cds

    monkeypatch.setattr('data.CDS', mock_cds_init)

    result = retrieve_and_concat_data([loc1, loc2], 2024, 2024, tmp_path, tmp_path)

    assert not result.empty
    assert len(result) == 2
    assert 'City A' in result['place_name'].values
    assert 'City B' in result['place_name'].values
    assert mock_cds.get_noon_series.call_count == 2


def test_retrieve_and_concat_data_caches_to_yaml(tmp_path, monkeypatch):
    """Test that data is saved to YAML cache file."""
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

    monkeypatch.setattr('data.CDS', lambda cache_dir, progress_manager=None: mock_cds)

    data_cache_dir = tmp_path / "data_cache"
    retrieve_and_concat_data([loc], 2024, 2024, tmp_path, data_cache_dir)

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
    save_data_file(df, yaml_file, loc)

    cached_years = get_cached_years(yaml_file)
    assert cached_years == {2024, 2025}


def test_get_cached_years_with_nonexistent_file(tmp_path):
    """Test get_cached_years with non-existent file returns empty set."""
    yaml_file = tmp_path / "nonexistent.yaml"
    cached_years = get_cached_years(yaml_file)
    assert cached_years == set()


def test_get_cached_years_with_corrupted_yaml(tmp_path):
    """Test get_cached_years handles corrupted YAML gracefully."""
    yaml_file = tmp_path / "corrupted.yaml"
    with open(yaml_file, 'w') as f:
        f.write("this is not valid: yaml: syntax: [[[")

    cached_years = get_cached_years(yaml_file)
    assert cached_years == set()


def test_get_cached_years_with_missing_temperatures_key(tmp_path):
    """Test get_cached_years with missing data key."""
    yaml_file = tmp_path / "no_temps.yaml"
    with open(yaml_file, 'w') as f:
        yaml.dump({'place': {'name': 'Test'}}, f)

    cached_years = get_cached_years(yaml_file)
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
    save_data_file(df_2024, yaml_file, loc, append=False)

    # Append 2025 data
    df_2025 = pd.DataFrame({
        'date': ['2025-01-01', '2025-01-02'],
        'temp_C': [15.0, 16.0],
        'grid_lat': [40.0, 40.0],
        'grid_lon': [-73.0, -73.0],
    })
    save_data_file(df_2025, yaml_file, loc, append=True)

    # Verify both years are present
    cached_years = get_cached_years(yaml_file)
    assert cached_years == {2024, 2025}

    # Read back and verify all data
    df_result = read_data_file(yaml_file)
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
    save_data_file(df, yaml_file, loc, append=True)

    # Verify file is now valid
    df_result = read_data_file(yaml_file)
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
    save_data_file(df_initial, yaml_file, loc)

    # Append with same date but different temperature
    df_update = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [20.0],
        'grid_lat': [40.0],
        'grid_lon': [-73.0],
    })
    save_data_file(df_update, yaml_file, loc, append=True)

    # Verify temperature was updated
    df_result = read_data_file(yaml_file)
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
    save_data_file(df, yaml_file, loc)

    # Test start_year filter
    df_filtered = read_data_file(yaml_file, start_year=2024)
    assert len(df_filtered) == 3
    assert df_filtered['date'].dt.year.min() == 2024

    # Test end_year filter
    df_filtered = read_data_file(yaml_file, end_year=2025)
    assert len(df_filtered) == 3
    assert df_filtered['date'].dt.year.max() == 2025

    # Test both filters
    df_filtered = read_data_file(yaml_file, start_year=2024, end_year=2025)
    assert len(df_filtered) == 2
    assert set(df_filtered['date'].dt.year.values) == {2024, 2025}


def test_retrieve_and_concat_data_uses_cached_years(tmp_path, monkeypatch):
    """Test that retrieve_and_concat_data uses cached data when available."""
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
    save_data_file(df_2024, yaml_file, loc)

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
    monkeypatch.setattr('data.CDS', lambda cache_dir, progress_manager=None: mock_cds)

    # Request 2024-2025, should only fetch 2025
    result = retrieve_and_concat_data([loc], 2024, 2025, tmp_path, data_cache_dir)

    # Should have both years
    assert len(result) == 2
    assert set(result['date'].dt.year.values) == {2024, 2025}

    # CDS should only be called once for 2025
    mock_cds.get_noon_series.assert_called_once()


def test_retrieve_and_concat_data_all_cached(tmp_path, monkeypatch):
    """Test that no CDS calls are made when all data is cached."""
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
    save_data_file(df, yaml_file, loc)

    # Mock CDS to detect if it's called
    mock_cds = MagicMock()
    monkeypatch.setattr('data.CDS', lambda cache_dir, progress_manager=None: mock_cds)

    # Request cached years only
    result = retrieve_and_concat_data([loc], 2024, 2025, tmp_path, data_cache_dir)

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
    save_data_file(df_2024, yaml_file, loc)

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
    save_data_file(df_2025, yaml_file, loc, append=True)

    # Verify all years are detected (including the string key)
    cached_years = get_cached_years(yaml_file)
    assert cached_years == {2023, 2024, 2025}


def test_read_data_file_legacy_temperatures_key(tmp_path):
    """Test one-way migration from legacy 'temperatures' key to v2 schema."""
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

    df = read_data_file(yaml_file)
    assert len(df) == 1
    assert df['temp_C'].iloc[0] == 12.5
    assert get_cached_years(yaml_file) == {2024}

    with open(yaml_file, 'r') as f:
        migrated = yaml.safe_load(f)
    assert migrated['schema_version'] == SCHEMA_VERSION
    assert DATA_KEY in migrated and NOON_TEMP_VAR in migrated[DATA_KEY]


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

    df = read_data_file(yaml_file)
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

    df = read_data_file(yaml_file)
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
        read_data_file(yaml_file)


def test_schema_registry_rejects_invalid_required_list_type(tmp_path):
    """Schema registry should fail if required_* fields are malformed."""
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
        _load_cache_schema_registry(schema_file)


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
        read_data_file(yaml_file)


def test_retrieve_and_concat_data_prints_cds_summary(tmp_path, monkeypatch, capsys):
    """Test that retrieve_and_concat_data prints CDS retrieval summary."""
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

    def mock_cds_init(cache_dir, progress_manager=None):
        return mock_cds

    monkeypatch.setattr('data.CDS', mock_cds_init)

    # Call with fresh locations (no cache)
    retrieve_and_concat_data([loc1, loc2], 2024, 2024, tmp_path, tmp_path)

    captured = capsys.readouterr()

    # Check for summary message
    assert "CDS Retrieval Required: 2 place(s)" in captured.out
    assert "City A" in captured.out
    assert "City B" in captured.out
    assert "=" in captured.out  # Separator lines


def test_retrieve_and_concat_data_prints_all_cached_message(tmp_path, monkeypatch, capsys):
    """Test that retrieve_and_concat_data prints message when all data is cached."""
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
    save_data_file(df, yaml_file, loc)

    mock_cds = MagicMock()
    monkeypatch.setattr('data.CDS', lambda cache_dir, progress_manager=None: mock_cds)

    # Call with cached data
    retrieve_and_concat_data([loc], 2024, 2024, tmp_path, data_cache_dir)

    captured = capsys.readouterr()

    # Check for "all cached" message
    assert "All data already cached - no CDS retrieval needed" in captured.out
    assert "=" in captured.out  # Separator lines

    # Verify CDS was not called
    mock_cds.get_noon_series.assert_not_called()
