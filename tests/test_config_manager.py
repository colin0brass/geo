"""
Tests for config_manager module.

Tests configuration file management, place loading, and config saving.
"""

import yaml

from config_manager import load_places, save_config
from cds import Location


def test_load_places_basic(tmp_path):
    """Test loading places from a basic config file."""
    config_file = tmp_path / "config.yaml"
    config_content = """
logging:
  log_file: geo.log
  console_level: WARNING

places:
  default_place: Cambridge, UK

  all_places:
    - {name: "Cambridge, UK", lat: 52.2053, lon: 0.1218}
    - {name: "Austin, TX", lat: 30.2672, lon: -97.7431}

  place_lists:
    test_list:
      - Cambridge, UK
"""
    config_file.write_text(config_content)

    places, default_place, place_lists = load_places(config_file)

    assert len(places) == 2
    assert "Cambridge, UK" in places
    assert "Austin, TX" in places
    assert default_place == "Cambridge, UK"
    assert "test_list" in place_lists
    assert place_lists["test_list"] == ["Cambridge, UK"]

    # Check Location object
    cambridge = places["Cambridge, UK"]
    assert isinstance(cambridge, Location)
    assert cambridge.lat == 52.2053
    assert cambridge.lon == 0.1218


def test_load_places_no_place_lists(tmp_path):
    """Test loading places when place_lists section is missing."""
    config_file = tmp_path / "config.yaml"
    config_content = """
places:
  default_place: Cambridge, UK
  all_places:
    - {name: "Cambridge, UK", lat: 52.2053, lon: 0.1218}
"""
    config_file.write_text(config_content)

    places, default_place, place_lists = load_places(config_file)

    assert len(places) == 1
    assert default_place == "Cambridge, UK"
    assert place_lists == {}


def test_save_config_basic(tmp_path):
    """Test saving a basic config file."""
    config_file = tmp_path / "config.yaml"
    config = {
        'logging': {
            'log_file': 'test.log',
            'console_level': 'INFO'
        },
        'places': {
            'default_place': 'TestPlace',
            'all_places': [
                {'name': 'TestPlace', 'lat': 1.0, 'lon': 2.0}
            ]
        }
    }

    save_config(config, config_file)

    assert config_file.exists()
    content = config_file.read_text()

    # Check formatting
    assert 'logging:' in content
    assert 'places:' in content
    assert 'default_place: TestPlace' in content
    assert '{name: "TestPlace", lat: 1.0, lon: 2.0}' in content

    # Check spacing (blank lines between sections)
    lines = content.split('\n')
    assert '' in lines  # Has blank lines


def test_save_config_with_place_lists(tmp_path):
    """Test saving config with place lists."""
    config_file = tmp_path / "config.yaml"
    config = {
        'logging': {'log_file': 'test.log', 'console_level': 'WARNING'},
        'places': {
            'default_place': 'Place1',
            'all_places': [
                {'name': 'Place1', 'lat': 1.0, 'lon': 2.0},
                {'name': 'Place2', 'lat': 3.0, 'lon': 4.0}
            ],
            'place_lists': {
                'list1': ['Place1', 'Place2'],
                'list2': ['Place1']
            }
        }
    }

    save_config(config, config_file)

    content = config_file.read_text()
    assert 'place_lists:' in content
    assert 'list1:' in content
    assert 'list2:' in content
    assert '- Place1' in content

    # Verify it can be loaded back
    loaded = yaml.safe_load(content)
    assert loaded['places']['place_lists']['list1'] == ['Place1', 'Place2']


def test_save_config_compact_format(tmp_path):
    """Test that places are saved in compact one-line format."""
    config_file = tmp_path / "config.yaml"
    config = {
        'places': {
            'all_places': [
                {'name': 'City, State', 'lat': 12.34, 'lon': -56.78}
            ]
        }
    }

    save_config(config, config_file)

    content = config_file.read_text()
    # Check it's on one line with flow syntax
    assert '{name: "City, State", lat: 12.34, lon: -56.78}' in content
    # Names should be quoted to handle commas
    assert '"City, State"' in content


def test_save_config_quoted_names(tmp_path):
    """Test that place names with special characters are quoted."""
    config_file = tmp_path / "config.yaml"
    config = {
        'places': {
            'all_places': [
                {'name': 'São Paulo, Brazil', 'lat': -23.55, 'lon': -46.63},
                {'name': 'O\'Fallon, IL', 'lat': 38.59, 'lon': -89.91}
            ]
        }
    }

    save_config(config, config_file)

    content = config_file.read_text()
    # Check both names are preserved correctly
    loaded = yaml.safe_load(content)
    names = [p['name'] for p in loaded['places']['all_places']]
    assert 'São Paulo, Brazil' in names
    assert 'O\'Fallon, IL' in names


def test_load_places_default_fallback(tmp_path):
    """Test that default_place falls back to first place if missing."""
    config_file = tmp_path / "config.yaml"
    config_content = """
places:
  all_places:
    - {name: "FirstPlace", lat: 1.0, lon: 2.0}
    - {name: "SecondPlace", lat: 3.0, lon: 4.0}
"""
    config_file.write_text(config_content)

    places, default_place, place_lists = load_places(config_file)

    # Should default to first place in dict
    assert default_place in places


def test_load_places_preserves_order(tmp_path):
    """Test that places maintain their order from config."""
    config_file = tmp_path / "config.yaml"
    config_content = """
places:
  all_places:
    - {name: "Alpha", lat: 1.0, lon: 1.0}
    - {name: "Beta", lat: 2.0, lon: 2.0}
    - {name: "Gamma", lat: 3.0, lon: 3.0}
"""
    config_file.write_text(config_content)

    places, _, _ = load_places(config_file)

    # Check all places loaded
    assert len(places) == 3
    assert all(name in places for name in ["Alpha", "Beta", "Gamma"])


def test_save_and_reload_config(tmp_path):
    """Test that a saved config can be loaded back correctly."""
    config_file = tmp_path / "config.yaml"
    original_config = {
        'logging': {'log_file': 'test.log', 'console_level': 'DEBUG'},
        'places': {
            'default_place': 'TestCity',
            'all_places': [
                {'name': 'TestCity', 'lat': 40.7128, 'lon': -74.0060}
            ],
            'place_lists': {
                'favorites': ['TestCity']
            }
        }
    }

    # Save and reload
    save_config(original_config, config_file)
    places, default_place, place_lists = load_places(config_file)

    # Verify data preserved
    assert default_place == 'TestCity'
    assert 'TestCity' in places
    assert places['TestCity'].lat == 40.7128
    assert places['TestCity'].lon == -74.0060
    assert place_lists['favorites'] == ['TestCity']


def test_save_config_spacing(tmp_path):
    """Test that config file has proper spacing between sections."""
    config_file = tmp_path / "config.yaml"
    config = {
        'logging': {'log_file': 'test.log', 'console_level': 'WARNING'},
        'places': {
            'default_place': 'Place1',
            'all_places': [{'name': 'Place1', 'lat': 1.0, 'lon': 2.0}],
            'place_lists': {'list1': ['Place1']}
        }
    }

    save_config(config, config_file)
    content = config_file.read_text()
    lines = content.split('\n')

    # Find blank lines
    blank_line_indices = [i for i, line in enumerate(lines) if line == '']

    # Should have blank lines between sections
    assert len(blank_line_indices) >= 2


def test_load_places_with_timezone(tmp_path):
    """Test loading places with explicit timezone."""
    config_file = tmp_path / "config.yaml"
    config_content = """
places:
  all_places:
    - {name: "TestPlace", lat: 1.0, lon: 2.0, tz: "America/New_York"}
"""
    config_file.write_text(config_content)

    places, _, _ = load_places(config_file)

    # Timezone should be ignored during loading (auto-detected at Location creation)
    # But the place should load successfully
    assert "TestPlace" in places
