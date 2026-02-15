# Test CLI utilities
import pytest
import subprocess
import sys
from unittest.mock import patch
from cli import (
    CLIError,
    get_place_list,
    parse_args,
    parse_measure_selection,
    parse_grid,
    parse_years,
    validate_measures_support,
    validate_measure_support,
)
from geo_data.cds_base import Location


def test_cli_help():
    result = subprocess.run([sys.executable, '-m', 'geo', '--help'], capture_output=True, text=True)
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
    with pytest.raises(CLIError) as exc_info:
        parse_years("not-a-year")
    assert "Invalid --years format" in str(exc_info.value)


def test_parse_years_invalid_range():
    with pytest.raises(CLIError):
        parse_years("2020-2024-2025")


def test_parse_years_reversed_range():
    with pytest.raises(CLIError):
        parse_years("2025-2020")


# Test parse_args function
def test_parse_args_default():
    with patch('sys.argv', ['geo.py']):
        args = parse_args()
        assert args.place is None
        assert args.place_list is None
        assert args.all is False
        assert args.show is False
        assert args.measure == 'noon_temperature'
        assert args.download_by == 'config'
        assert args.update_cache is False


def test_parse_args_with_place():
    with patch('sys.argv', ['geo.py', '--place', 'Austin, TX']):
        args = parse_args()
        assert args.place == 'Austin, TX'
        assert args.place_list is None
        assert args.all is False


def test_parse_args_with_place_list():
    with patch('sys.argv', ['geo.py', '--list', 'preferred']):
        args = parse_args()
        assert args.place_list == 'preferred'
        assert args.place is None
        assert args.all is False


def test_parse_args_with_place_list_short_option():
    with patch('sys.argv', ['geo.py', '-l', 'preferred']):
        args = parse_args()
        assert args.place_list == 'preferred'
        assert args.place is None
        assert args.all is False


def test_parse_args_with_list_all_alias():
    with patch('sys.argv', ['geo.py', '--list', 'all']):
        args = parse_args()
        assert args.place_list == 'all'
        assert args.all is False


def test_parse_args_with_list_places_short_option():
    with patch('sys.argv', ['geo.py', '-L']):
        args = parse_args()
        assert args.list_places is True


def test_parse_args_with_cache_summary_flag():
    with patch('sys.argv', ['geo.py', '--cache-summary']):
        args = parse_args()
        assert args.cache_summary is True
        assert args.rebuild_cache_summary is False


def test_parse_args_with_rebuild_cache_summary_flag():
    with patch('sys.argv', ['geo.py', '--rebuild-cache-summary']):
        args = parse_args()
        assert args.rebuild_cache_summary is True


def test_parse_args_with_all():
    with patch('sys.argv', ['geo.py', '--all']):
        args = parse_args()
        assert args.all is True
        assert args.place is None
        assert args.place_list is None


def test_parse_args_with_custom_location():
    with patch('sys.argv', ['geo.py', '--place', 'Custom', '--lat', '40.0', '--lon', '-73.0', '--tz', 'America/New_York']):
        args = parse_args()
        assert args.place == 'Custom'
        assert args.lat == 40.0
        assert args.lon == -73.0
        assert args.tz == 'America/New_York'


def test_parse_args_with_years():
    with patch('sys.argv', ['geo.py', '--years', '2020-2024']):
        args = parse_args()
        assert args.years == '2020-2024'


def test_parse_args_with_show():
    with patch('sys.argv', ['geo.py', '--show']):
        args = parse_args()
        assert args.show is True


def test_parse_args_with_measure():
    with patch('sys.argv', ['geo.py', '--measure', 'daily_precipitation']):
        args = parse_args()
        assert args.measure == 'daily_precipitation'


def test_parse_args_with_measure_short_option():
    with patch('sys.argv', ['geo.py', '-m', 'daily_precipitation']):
        args = parse_args()
        assert args.measure == 'daily_precipitation'


def test_parse_args_with_solar_measure():
    with patch('sys.argv', ['geo.py', '--measure', 'daily_solar_radiation_energy']):
        args = parse_args()
        assert args.measure == 'daily_solar_radiation_energy'


def test_parse_args_with_temp_alias():
    with patch('sys.argv', ['geo.py', '--measure', 'temp']):
        args = parse_args()
        assert args.measure == 'noon_temperature'


def test_parse_args_with_temperature_alias():
    with patch('sys.argv', ['geo.py', '--measure', 'temperature']):
        args = parse_args()
        assert args.measure == 'noon_temperature'


def test_parse_args_with_precipitation_alias():
    with patch('sys.argv', ['geo.py', '--measure', 'precipitation']):
        args = parse_args()
        assert args.measure == 'daily_precipitation'


def test_parse_args_with_solar_alias():
    with patch('sys.argv', ['geo.py', '--measure', 'solar']):
        args = parse_args()
        assert args.measure == 'daily_solar_radiation_energy'


def test_parse_args_with_measure_all():
    with patch('sys.argv', ['geo.py', '-m', 'all']):
        args = parse_args()
        assert args.measures == [
            'noon_temperature',
            'daily_precipitation',
            'daily_solar_radiation_energy',
        ]
        assert args.measure == 'noon_temperature'


def test_parse_args_with_multiple_measures_csv():
    with patch('sys.argv', ['geo.py', '-m', 'temp,solar']):
        args = parse_args()
        assert args.measures == ['noon_temperature', 'daily_solar_radiation_energy']


def test_parse_args_with_measure_all_mixed_raises():
    with patch('sys.argv', ['geo.py', '-m', 'all,temp']):
        with pytest.raises(CLIError):
            parse_args()


def test_parse_args_with_download_by_month():
    with patch('sys.argv', ['geo.py', '--download-by', 'month']):
        args = parse_args()
        assert args.download_by == 'month'


def test_parse_args_with_download_by_compare():
    with patch('sys.argv', ['geo.py', '--download-by', 'compare']):
        args = parse_args()
        assert args.download_by == 'compare'


def test_parse_args_with_update_cache_long_form():
    with patch('sys.argv', ['geo.py', '--update-cache']):
        args = parse_args()
        assert args.update_cache is True


def test_parse_args_with_update_cache_short_form():
    with patch('sys.argv', ['geo.py', '-u']):
        args = parse_args()
        assert args.update_cache is True


def test_parse_args_runtime_paths_from_custom_config(tmp_path):
    config_file = tmp_path / "custom.yaml"
    config_file.write_text(
        "runtime_paths:\n"
        "  cache_dir: alt_cache\n"
        "  data_cache_dir: alt_data\n"
        "  out_dir: alt_out\n"
        "  settings_file: alt/settings.yaml\n"
    )

    with patch('sys.argv', ['geo.py', '--config', str(config_file)]):
        args = parse_args()
        assert str(args.cache_dir) == 'alt_cache'
        assert str(args.data_cache_dir) == 'alt_data'
        assert str(args.out_dir) == 'alt_out'
        assert str(args.settings) == 'alt/settings.yaml'


def test_validate_measure_support_default_ok():
    validate_measure_support('noon_temperature')


def test_validate_measure_support_precipitation_ok():
    validate_measure_support('daily_precipitation')


def test_validate_measure_support_solar_ok():
    validate_measure_support('daily_solar_radiation_energy')


def test_parse_measure_selection_deduplicates():
    measures = parse_measure_selection('temp,noon_temperature,solar,solar')
    assert measures == ['noon_temperature', 'daily_solar_radiation_energy']


def test_parse_measure_selection_temperature_alias():
    measures = parse_measure_selection('temperature')
    assert measures == ['noon_temperature']


def test_validate_measures_support_ok():
    validate_measures_support(['noon_temperature', 'daily_precipitation'])


def test_validate_measures_support_invalid_raises():
    with pytest.raises(CLIError):
        validate_measures_support(['noon_temperature', 'invalid_measure'])


def test_parse_args_with_colour_mode():
    with patch('sys.argv', ['geo.py', '--colour-mode', 'year']):
        args = parse_args()
        assert args.colour_mode == 'year'


def test_parse_args_with_color_mode_alias():
    with patch('sys.argv', ['geo.py', '--color-mode', 'year']):
        args = parse_args()
        assert args.colour_mode == 'year'


def test_parse_args_invalid_argument_hint_for_start_year():
    with patch('sys.argv', ['geo.py', '--start-year', '2025']):
        with pytest.raises(CLIError) as exc_info:
            parse_args()
    assert "--years" in str(exc_info.value)


def test_parse_args_rejects_legacy_colour_mode_temperature():
    with patch('sys.argv', ['geo.py', '--colour-mode', 'temperature']):
        with pytest.raises(CLIError) as exc_info:
            parse_args()
    assert "invalid choice" in str(exc_info.value)


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


def test_get_place_list_all_alias_from_list():
    places = {
        'Austin, TX': Location(name='Austin, TX', lat=30.27, lon=-97.74, tz='America/Chicago'),
        'Cambridge, MA': Location(name='Cambridge, MA', lat=42.37, lon=-71.11, tz='America/New_York'),
    }
    default_place = 'Austin, TX'
    place_lists = {'default': ['Austin, TX']}

    class Args:
        all = False
        place_list = 'all'
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

    with pytest.raises(CLIError) as exc_info:
        get_place_list(Args(), places, default_place, place_lists)
    assert "Unknown place list" in str(exc_info.value)


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

    with pytest.raises(CLIError) as exc_info:
        get_place_list(Args(), places, default_place, place_lists)
    assert "Unknown place" in str(exc_info.value)


# Test parse_grid function
def test_parse_grid_valid():
    assert parse_grid("4x3") == (3, 4)  # 4 cols, 3 rows


def test_parse_grid_valid_uppercase():
    assert parse_grid("5X4") == (4, 5)  # 5 cols, 4 rows


def test_parse_grid_none():
    assert parse_grid(None) is None


def test_parse_grid_invalid_no_x():
    with pytest.raises(CLIError):
        parse_grid("43")


def test_parse_grid_invalid_too_many_parts():
    with pytest.raises(CLIError):
        parse_grid("4x3x2")


def test_parse_grid_invalid_non_numeric():
    with pytest.raises(CLIError):
        parse_grid("axb")


def test_parse_grid_invalid_zero():
    with pytest.raises(CLIError):
        parse_grid("0x3")


def test_parse_grid_invalid_negative():
    with pytest.raises(CLIError):
        parse_grid("4x-3")


# Test CLI with --grid argument
def test_parse_args_with_grid():
    with patch('sys.argv', ['geo.py', '--grid', '4x3']):
        args = parse_args()
        assert args.grid == '4x3'


def test_parse_args_grid_default():
    with patch('sys.argv', ['geo.py']):
        args = parse_args()
        assert args.grid is None


def test_parse_args_rejects_list_years_short_option():
    """Removed -ly shorthand should no longer map to a list-years flag."""
    with patch('sys.argv', ['geo.py', '-ly']):
        args = parse_args()
        assert args.place_list == 'y'


def test_parse_args_rejects_list_years_long_option():
    """Removed --list-years option should now raise a CLI error."""
    with patch('sys.argv', ['geo.py', '--list-years']):
        with pytest.raises(CLIError):
            parse_args()
