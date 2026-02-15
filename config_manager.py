"""
Configuration file management for geo.

Handles loading and saving of config.yaml with proper formatting.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder

from geo_data.cds_base import Location
from geo_core.config import (
    extract_places_config,
    find_place_by_name,
    render_config_yaml,
)


def _resolve_places_file_path(config: dict, config_path: Path) -> Path:
    places_file = config.get('places_file', 'places.yaml')
    if not isinstance(places_file, str) or not places_file.strip():
        raise ValueError("places_file must be a non-empty string when places are externalized")
    candidate = Path(places_file)
    if not candidate.is_absolute():
        candidate = config_path.parent / candidate
    return candidate


def load_places(yaml_path: Path = Path("config.yaml")) -> tuple[dict, str, dict]:
    """
    Load places configuration from YAML.

    Args:
        yaml_path: Path to the configuration YAML file.

    Returns:
        tuple: (places_dict, default_place_name, place_lists_dict)
    """
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f) or {}

    if isinstance(config.get('places'), dict):
        places_source = {'places': config['places']}
    else:
        places_file_path = _resolve_places_file_path(config, yaml_path)
        if not places_file_path.exists():
            raise FileNotFoundError(f"Places file not found: {places_file_path}")
        with open(places_file_path, "r") as places_file:
            places_source = yaml.safe_load(places_file) or {}

    all_places, default_place, place_lists = extract_places_config(places_source)
    places_dict = {p['name']: Location(**p) for p in all_places}

    if not default_place and places_dict:
        default_place = next(iter(places_dict.keys()))

    return places_dict, default_place, place_lists


def save_config(config: dict, config_path: Path = Path("config.yaml")) -> None:
    """
    Save configuration to YAML file with proper formatting.

    Uses compact flow style for places (one line per place) and ensures
    spacing between sections. Note: This overwrites the file and does not
    preserve user-added comments.

    Args:
        config: Configuration dictionary to save
        config_path: Path to the configuration YAML file
    """
    rendered_yaml = render_config_yaml(config)

    with open(config_path, "w") as f:
        f.write(rendered_yaml)


def add_place_to_config(place_name: str, config_path: Path = Path("config.yaml")) -> None:
    """
    Look up coordinates for a place and add it to the configuration file.

    Args:
        place_name: Name of the place to add (e.g., "Seattle, WA")
        config_path: Path to the configuration YAML file
    """
    # Geocode the place
    print(f"Looking up coordinates for '{place_name}'...")
    geolocator = Nominatim(user_agent="geo")

    try:
        location = geolocator.geocode(place_name)
        if location is None:
            print(f"ERROR: Could not find coordinates for '{place_name}'")
            print("Try being more specific (e.g., 'Seattle, WA, USA' instead of 'Seattle')")
            sys.exit(1)

        lat = round(location.latitude, 2)
        lon = round(location.longitude, 2)

        print(f"Found: {location.address}")
        print(f"Coordinates: {lat}, {lon}")

        # Auto-detect timezone
        tf = TimezoneFinder()
        tz = tf.timezone_at(lat=lat, lng=lon)
        if tz:
            print(f"Timezone: {tz}")

        # Load existing config
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}

        places_file_path = None
        if isinstance(config.get('places'), dict):
            places_payload = config['places']
            save_to_main_config = True
        else:
            places_file_path = _resolve_places_file_path(config, config_path)
            if places_file_path.exists():
                with open(places_file_path, "r") as places_file:
                    loaded_places = yaml.safe_load(places_file) or {}
            else:
                loaded_places = {}
            if isinstance(loaded_places.get('places'), dict):
                places_payload = loaded_places['places']
            else:
                places_payload = loaded_places
            save_to_main_config = False

        # Check if place already exists
        all_places = places_payload.get('all_places', [])
        if not isinstance(all_places, list):
            all_places = []
            places_payload['all_places'] = all_places

        existing_place = find_place_by_name(all_places, place_name)
        if existing_place is not None:
            print(f"\nWARNING: '{place_name}' already exists in config with coordinates {existing_place['lat']}, {existing_place['lon']}")
            response = input("Overwrite? (y/n): ")
            if response.lower() != 'y':
                print("Cancelled.")
                sys.exit(0)
            all_places.remove(existing_place)

        # Add new place
        new_place = {'name': place_name, 'lat': lat, 'lon': lon}
        all_places.append(new_place)
        if not places_payload.get('default_place'):
            places_payload['default_place'] = place_name

        # Save config with proper formatting
        if save_to_main_config:
            config['places'] = places_payload
            save_config(config, config_path)
            saved_path = config_path
        else:
            places_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(places_file_path, "w") as places_file:
                yaml.safe_dump(places_payload, places_file, default_flow_style=False, sort_keys=False, allow_unicode=True)
            saved_path = places_file_path

        print(f"\n✓ Added '{place_name}' to {saved_path}")
        print(f"  Coordinates: {lat}, {lon}")
        if tz:
            print(f"  Timezone will be auto-detected as: {tz}")

    except Exception as e:
        print(f"ERROR: Failed to look up or add place: {e}")
        sys.exit(1)
