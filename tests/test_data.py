"""
Tests for data module (data retrieval and I/O operations).
"""
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from data import retrieve_and_concat_data, read_data_file, save_data_file
from cds import Location


def test_read_and_save_data_file(tmp_path):
    """Test reading and saving CSV data files."""
    df = pd.DataFrame({
        'date': ['2025-01-01', '2025-01-02'],
        'utc_time_used': ['2025-01-01T18:00:00', '2025-01-02T18:00:00'],
        'local_noon': ['2025-01-01T12:00:00', '2025-01-02T12:00:00'],
        'temp_C': [10.0, 12.0],
        'temp_F': [50.0, 53.6],
        'grid_lat': [1.0, 1.0],
        'grid_lon': [2.0, 2.0],
        'place_name': ['Test', 'Test'],
    })
    out_file = tmp_path / "test.csv"
    save_data_file(df, out_file)
    
    # Verify file was created
    assert out_file.exists()
    
    # Read it back
    df2 = read_data_file(out_file)
    assert not df2.empty
    assert len(df2) == 2
    assert df2['date'].iloc[0] == pd.Timestamp('2025-01-01')
    assert df2['temp_C'].iloc[0] == 10.0


def test_save_data_file_creates_directory(tmp_path):
    """Test that save_data_file creates output directory if needed."""
    df = pd.DataFrame({
        'date': ['2025-01-01'],
        'temp_C': [10.0],
    })
    nested_path = tmp_path / "nested" / "dir" / "test.csv"
    
    save_data_file(df, nested_path)
    assert nested_path.exists()
    assert nested_path.parent.is_dir()


def test_read_data_file_missing_file(tmp_path):
    """Test reading a non-existent file raises appropriate error."""
    missing_file = tmp_path / "missing.csv"
    with pytest.raises(FileNotFoundError):
        read_data_file(missing_file)


def test_read_data_file_date_parsing(tmp_path):
    """Test that date column is properly parsed as datetime."""
    df = pd.DataFrame({
        'date': ['2025-01-15', '2025-02-20'],
        'temp_C': [15.0, 18.0],
        'utc_time_used': ['2025-01-15T18:00:00', '2025-02-20T18:00:00'],
        'local_noon': ['2025-01-15T12:00:00', '2025-02-20T12:00:00'],
    })
    out_file = tmp_path / "dates.csv"
    save_data_file(df, out_file)
    
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
    })
    mock_cds.get_noon_series.return_value = mock_df
    
    def mock_cds_init(cache_dir):
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
    
    def mock_get_noon_series(loc, start_d, end_d):
        return pd.DataFrame({
            'date': ['2024-01-01'],
            'temp_C': [10.0],
            'place_name': [loc.name],
        })
    
    mock_cds.get_noon_series.side_effect = mock_get_noon_series
    
    def mock_cds_init(cache_dir):
        return mock_cds
    
    monkeypatch.setattr('data.CDS', mock_cds_init)
    
    result = retrieve_and_concat_data([loc1, loc2], 2024, 2024, tmp_path, tmp_path)
    
    assert not result.empty
    assert len(result) == 2
    assert 'City A' in result['place_name'].values
    assert 'City B' in result['place_name'].values
    assert mock_cds.get_noon_series.call_count == 2


def test_retrieve_and_concat_data_caches_to_csv(tmp_path, monkeypatch):
    """Test that data is saved to CSV cache file."""
    loc = Location(name="Test", lat=40.0, lon=-73.0, tz="America/New_York")
    
    mock_cds = MagicMock()
    mock_df = pd.DataFrame({
        'date': ['2024-01-01'],
        'temp_C': [10.0],
        'place_name': ['Test'],
    })
    mock_cds.get_noon_series.return_value = mock_df
    
    monkeypatch.setattr('data.CDS', lambda cache_dir: mock_cds)
    
    out_dir = tmp_path / "output"
    retrieve_and_concat_data([loc], 2024, 2024, tmp_path, out_dir)
    
    # Check that CSV file was created
    csv_files = list(out_dir.glob("*.csv"))
    assert len(csv_files) > 0
    assert any('Test' in str(f) for f in csv_files)
