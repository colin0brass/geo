# Test CLI utilities
import pytest
import pandas as pd
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch
from cli import parse_args, parse_years, get_place_list, calculate_grid_layout, parse_grid, load_places, load_grid_settings
from cds import Location


def test_cli_help():
    result = subprocess.run([sys.executable, '-m', 'geo_temp', '--help'], capture_output=True, text=True)
    assert result.returncode == 0
    assert 'usage' in result.stdout.lower() or 'help' in result.stdout.lower()


# Test parse_years function
def test_parse_years_single_year():
    start, end = parse_years("2024")
    assert start == 2024
    assert end == 2024


def test_parse_years_range():
    start, end = parse_years("2020-2024")
    assert start == 2020
    assert end == 2024


def test_parse_years_invalid_format():
    with pytest.raises(SystemExit):
        parse_years("not-a-year")


def test_parse_years_invalid_range():
    with pytest.raises(SystemExit):
        parse_years("2020-2024-2025")


# Test parse_args function
def test_parse_args_default():
    with patch('sys.argv', ['geo_temp.py']):
        args = parse_args()
        assert args.place is None
        assert args.place_list is None
        assert args.all is False
        assert args.show is False


def test_parse_args_with_place():
    with patch('sys.argv', ['geo_temp.py', '--place', 'Austin, TX']):
        args = parse_args()
        assert args.place == 'Austin, TX'
        assert args.place_list is None
        assert args.all is False


def test_parse_args_with_place_list():
    with patch('sys.argv', ['geo_temp.py', '--list', 'preferred']):
        args = parse_args()
        assert args.place_list == 'preferred'
        assert args.place is None
        assert args.all is False


def test_parse_args_with_all():
    with patch('sys.argv', ['geo_temp.py', '--all']):
        args = parse_args()
        assert args.all is True
        assert args.place is None
        assert args.place_list is None


def test_parse_args_with_custom_location():
    with patch('sys.argv', ['geo_temp.py', '--place', 'Custom', '--lat', '40.0', '--lon', '-73.0', '--tz', 'America/New_York']):
        args = parse_args()
        assert args.place == 'Custom'
        assert args.lat == 40.0
        assert args.lon == -73.0
        assert args.tz == 'America/New_York'


def test_parse_args_with_years():
    with patch('sys.argv', ['geo_temp.py', '--years', '2020-2024']):
        args = parse_args()
        assert args.years == '2020-2024'


def test_parse_args_with_show():
    with patch('sys.argv', ['geo_temp.py', '--show']):
        args = parse_args()
        assert args.show is True


# Test get_place_list function
def test_get_place_list_default():
    places = {
        'Austin, TX': Location(name='Austin, TX', lat=30.27, lon=-97.74, tz='America/Chicago'),
        'Cambridge, MA': Location(name='Cambridge, MA', lat=42.37, lon=-71.11, tz='America/New_York'),
    }
    default_place = 'Austin, TX'
    place_lists = {}
    
    # Mock args with no place selection
    class Args:
        all = False
        place_list = None
        place = None
        lat = None
        lon = None
        tz = None
    
    result, list_name = get_place_list(Args(), places, default_place, place_lists)
    assert len(result) == 1
    assert result[0].name == 'Austin, TX'
    assert list_name is None


def test_get_place_list_all():
    places = {
        'Austin, TX': Location(name='Austin, TX', lat=30.27, lon=-97.74, tz='America/Chicago'),
        'Cambridge, MA': Location(name='Cambridge, MA', lat=42.37, lon=-71.11, tz='America/New_York'),
    }
    default_place = 'Austin, TX'
    place_lists = {}
    
    class Args:
        all = True
        place_list = None
        place = None
        lat = None
        lon = None
        tz = None
    
    result, list_name = get_place_list(Args(), places, default_place, place_lists)
    assert len(result) == 2
    assert set(p.name for p in result) == {'Austin, TX', 'Cambridge, MA'}
    assert list_name == 'all'


def test_get_place_list_place_list():
    places = {
        'Austin, TX': Location(name='Austin, TX', lat=30.27, lon=-97.74, tz='America/Chicago'),
        'Cambridge, MA': Location(name='Cambridge, MA', lat=42.37, lon=-71.11, tz='America/New_York'),
        'Bangalore': Location(name='Bangalore', lat=12.97, lon=77.59, tz='Asia/Kolkata'),
    }
    default_place = 'Austin, TX'
    place_lists = {
        'preferred': ['Austin, TX', 'Bangalore']
    }
    
    class Args:
        all = False
        place_list = 'preferred'
        place = None
        lat = None
        lon = None
        tz = None
    
    result, list_name = get_place_list(Args(), places, default_place, place_lists)
    assert len(result) == 2
    assert set(p.name for p in result) == {'Austin, TX', 'Bangalore'}
    assert list_name == 'preferred'


def test_get_place_list_single_place():
    places = {
        'Austin, TX': Location(name='Austin, TX', lat=30.27, lon=-97.74, tz='America/Chicago'),
        'Cambridge, MA': Location(name='Cambridge, MA', lat=42.37, lon=-71.11, tz='America/New_York'),
    }
    default_place = 'Austin, TX'
    place_lists = {}
    
    class Args:
        all = False
        place_list = None
        place = 'Cambridge, MA'
        lat = None
        lon = None
        tz = None
    
    result, list_name = get_place_list(Args(), places, default_place, place_lists)
    assert len(result) == 1
    assert result[0].name == 'Cambridge, MA'
    assert list_name is None


def test_get_place_list_custom_location():
    places = {}
    default_place = 'Austin, TX'
    place_lists = {}
    
    class Args:
        all = False
        place_list = None
        place = 'Custom City'
        lat = 40.0
        lon = -73.0
        tz = 'America/New_York'
    
    result, list_name = get_place_list(Args(), places, default_place, place_lists)
    assert len(result) == 1
    assert result[0].name == 'Custom City'
    assert result[0].lat == 40.0
    assert result[0].lon == -73.0
    assert result[0].tz == 'America/New_York'
    assert list_name is None


def test_get_place_list_invalid_place_list():
    places = {'Austin, TX': Location(name='Austin, TX', lat=30.27, lon=-97.74, tz='America/Chicago')}
    default_place = 'Austin, TX'
    place_lists = {'preferred': ['Austin, TX']}
    
    class Args:
        all = False
        place_list = 'nonexistent'
        place = None
        lat = None
        lon = None
        tz = None
    
    with pytest.raises(SystemExit):
        get_place_list(Args(), places, default_place, place_lists)


def test_get_place_list_invalid_place_no_coords():
    places = {'Austin, TX': Location(name='Austin, TX', lat=30.27, lon=-97.74, tz='America/Chicago')}
    default_place = 'Austin, TX'
    place_lists = {}
    
    class Args:
        all = False
        place_list = None
        place = 'Unknown Place'
        lat = None
        lon = None
        tz = None
    
    with pytest.raises(SystemExit):
        get_place_list(Args(), places, default_place, place_lists)


# Test calculate_grid_layout function
def test_calculate_grid_layout_single():
    # With default 4x6 max
    assert calculate_grid_layout(1, 4, 6) == (1, 1)


def test_calculate_grid_layout_two():
    # With default 4x6 max
    assert calculate_grid_layout(2, 4, 6) == (1, 2)


def test_calculate_grid_layout_four():
    # With default 4x6 max
    assert calculate_grid_layout(4, 4, 6) == (2, 2)


def test_calculate_grid_layout_six():
    # With default 4x6 max
    assert calculate_grid_layout(6, 4, 6) == (2, 3)


def test_calculate_grid_layout_eight():
    # Should be 3×3 (9 cells, 1 empty) for better aspect ratio
    # With default 4x6 max
    rows, cols = calculate_grid_layout(8, 4, 6)
    assert rows == 3 and cols == 3


def test_calculate_grid_layout_ten():
    # Should be 3×4 (12 cells, 2 empty) instead of 2×5
    # With default 4x6 max
    rows, cols = calculate_grid_layout(10, 4, 6)
    assert rows == 3 and cols == 4


def test_calculate_grid_layout_twelve():
    # With default 4x6 max
    assert calculate_grid_layout(12, 4, 6) == (3, 4)


def test_calculate_grid_layout_sixteen():
    # Should be 4×4 instead of 2×8
    # With default 4x6 max
    assert calculate_grid_layout(16, 4, 6) == (4, 4)


def test_calculate_grid_layout_twenty():
    # With 4x6 max grid (24 places max), should be 4×5 for 20 places
    rows, cols = calculate_grid_layout(20, 4, 6)
    assert rows == 4 and cols == 5


def test_calculate_grid_layout_zero():
    # With default 4x6 max
    assert calculate_grid_layout(0, 4, 6) == (1, 1)


def test_calculate_grid_layout_custom_max_cols():
    # Test with custom max_cols=3 and max_rows=5
    rows, cols = calculate_grid_layout(10, max_rows=5, max_cols=3)
    assert cols <= 3
    assert rows * cols >= 10


# Test parse_grid function
def test_parse_grid_valid():
    assert parse_grid("4x3") == (3, 4)  # 4 cols, 3 rows


def test_parse_grid_valid_uppercase():
    assert parse_grid("5X4") == (4, 5)  # 5 cols, 4 rows


def test_parse_grid_none():
    assert parse_grid(None) is None


def test_parse_grid_invalid_no_x():
    with pytest.raises(SystemExit):
        parse_grid("43")


def test_parse_grid_invalid_too_many_parts():
    with pytest.raises(SystemExit):
        parse_grid("4x3x2")


def test_parse_grid_invalid_non_numeric():
    with pytest.raises(SystemExit):
        parse_grid("axb")


def test_parse_grid_invalid_zero():
    with pytest.raises(SystemExit):
        parse_grid("0x3")


def test_parse_grid_invalid_negative():
    with pytest.raises(SystemExit):
        parse_grid("4x-3")


# Test CLI with --grid argument
def test_parse_args_with_grid():
    with patch('sys.argv', ['geo_temp.py', '--grid', '4x3']):
        args = parse_args()
        assert args.grid == '4x3'


def test_parse_args_grid_default():
    with patch('sys.argv', ['geo_temp.py']):
        args = parse_args()
        assert args.grid is None


# Helper function for testing year range condensing logic
def _condense_year_ranges(years):
    """Helper to test year range condensing logic."""
    if not years:
        return ""
    ranges = []
    start = years[0]
    end = years[0]
    for i in range(1, len(years)):
        if years[i] == end + 1:
            end = years[i]
        else:
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = years[i]
            end = years[i]
    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")
    return ", ".join(ranges)


def test_condense_year_ranges_single_year():
    """Test condensing a single year."""
    assert _condense_year_ranges([2025]) == "2025"


def test_condense_year_ranges_contiguous():
    """Test condensing contiguous year ranges."""
    assert _condense_year_ranges([1990, 1991, 1992, 1993, 1994, 1995]) == "1990-1995"
    assert _condense_year_ranges([2020, 2021, 2022, 2023, 2024, 2025]) == "2020-2025"


def test_condense_year_ranges_with_gaps():
    """Test condensing years with gaps."""
    assert _condense_year_ranges([1990, 1991, 1995, 2000, 2001, 2002]) == "1990-1991, 1995, 2000-2002"
    assert _condense_year_ranges([2020, 2022, 2024]) == "2020, 2022, 2024"


def test_condense_year_ranges_empty():
    """Test condensing empty list."""
    assert _condense_year_ranges([]) == ""


def test_condense_year_ranges_two_years():
    """Test condensing two contiguous years."""
    assert _condense_year_ranges([2024, 2025]) == "2024-2025"


def test_parse_args_with_list_years():
    """Test parsing --list-years argument."""
    with patch('sys.argv', ['geo_temp.py', '-ly']):
        args = parse_args()
        assert args.list_years is True


def test_parse_args_list_years_long_form():
    """Test parsing --list-years with long form."""
    with patch('sys.argv', ['geo_temp.py', '--list-years']):
        args = parse_args()
        assert args.list_years is True


def test_parse_args_list_years_default():
    """Test that list_years is False by default."""
    with patch('sys.argv', ['geo_temp.py']):
        args = parse_args()
        assert args.list_years is False


# Test load_grid_settings function
def test_load_grid_settings_valid(tmp_path):
    """Test loading grid settings from valid YAML file."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("grid:\n  max_auto_rows: 5\n  max_auto_cols: 8\n")
    
    max_rows, max_cols = load_grid_settings(config_file)
    assert max_rows == 5
    assert max_cols == 8


def test_load_grid_settings_defaults(tmp_path):
    """Test that defaults are returned when grid section is missing."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("other_section:\n  key: value\n")
    
    max_rows, max_cols = load_grid_settings(config_file)
    assert max_rows == 4
    assert max_cols == 6


def test_load_grid_settings_partial(tmp_path):
    """Test partial grid settings with only one value specified."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("grid:\n  max_auto_rows: 3\n")
    
    max_rows, max_cols = load_grid_settings(config_file)
    assert max_rows == 3
    assert max_cols == 6  # default


def test_load_grid_settings_missing_file(tmp_path):
    """Test that defaults are returned when config file doesn't exist."""
    config_file = tmp_path / "nonexistent.yaml"
    
    max_rows, max_cols = load_grid_settings(config_file)
    assert max_rows == 4
    assert max_cols == 6
