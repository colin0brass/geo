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

from cds import Location


def load_plot_text_config(config_path: Path = Path("config.yaml")) -> dict:
    """
    Load plot text configuration from config file.

    Args:
        config_path: Path to the configuration YAML file.

    Returns:
        dict: Plot text configuration with default fallbacks.
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('plot_text', {})
    except Exception:
        return {}


def load_measure_labels_config(config_path: Path = Path("config.yaml")) -> dict[str, dict[str, str]]:
    """
    Load measure label/unit mappings from config.

    Args:
        config_path: Path to the configuration YAML file.

    Returns:
        dict[str, dict[str, str]]: Mapping of measure key to label/unit dict.
    """
    defaults = {
        "noon_temperature": {
            "label": "Mid-Day Temperature",
            "unit": "°C",
            "y_value_column": "temp_C",
            "range_text": "{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)",
        },
        "daily_precipitation": {
            "label": "Daily Precipitation",
            "unit": "mm",
            "y_value_column": "precip_mm",
            "range_text": "{measure_label}: {min_value:.1f} to {max_value:.1f} {measure_unit}",
        },
    }

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f) or {}

        configured = config.get('plotting', {}).get('measure_labels', {})
        if not isinstance(configured, dict):
            return defaults

        merged = {key: value.copy() for key, value in defaults.items()}
        for measure_key, metadata in configured.items():
            if not isinstance(metadata, dict):
                continue
            label = metadata.get('label')
            unit = metadata.get('unit')
            y_value_column = metadata.get('y_value_column')
            range_text = metadata.get('range_text')
            merged_label = label if isinstance(label, str) else measure_key.replace('_', ' ').title()
            merged_unit = unit if isinstance(unit, str) else ''
            merged[measure_key] = {
                'label': merged_label,
                'unit': merged_unit,
                'y_value_column': y_value_column if isinstance(y_value_column, str) else 'temp_C',
                'range_text': range_text if isinstance(range_text, str) else '',
            }
        return merged
    except Exception:
        return defaults


def get_plot_text(config: dict, key: str, **kwargs) -> str:
    """
    Get formatted plot text from configuration.

    Args:
        config: Plot text configuration dictionary.
        key: Configuration key to retrieve.
        **kwargs: Format parameters for string substitution.

    Returns:
        str: Formatted text string.
    """
    # Default patterns if config is missing
    defaults = {
        'single_plot_title': "{location} {measure_label} ({start_year}-{end_year})",
        'subplot_title': "{measure_label} ({start_year}-{end_year})",
        'subplot_title_with_batch': "{measure_label} ({start_year}-{end_year}) - Part {batch}/{total_batches}",
        'range_text': "{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)",
        'single_plot_filename': "{location}_{measure_key}_{start_year}_{end_year}.png",
        'subplot_filename': "{list_name}_{measure_key}_{start_year}_{end_year}.png",
        'subplot_filename_with_batch': "{list_name}_{measure_key}_{start_year}_{end_year}_part{batch}of{total_batches}.png",
        'credit': "Climate Data Analysis & Visualisation by Colin Osborne",
        'data_source': "Data from: ERA5 via CDS",
        'single_plot_credit': "Analysis & visualisation by Colin Osborne"
    }

    pattern = config.get(key, defaults.get(key, ""))

    # Sanitize location name for filenames if present
    if 'location' in kwargs and ('filename' in key):
        kwargs['location'] = kwargs['location'].replace(' ', '_').replace(',', '')

    try:
        return pattern.format(**kwargs)
    except KeyError:
        # Missing format parameter - return pattern as-is
        return pattern


def load_places(yaml_path: Path = Path("config.yaml")) -> tuple[dict, str, dict]:
    """
    Load places configuration from YAML.

    Args:
        yaml_path: Path to the configuration YAML file.

    Returns:
        tuple: (places_dict, default_place_name, place_lists_dict)
    """
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)

    # Extract places section from config
    places_config = config.get('places', {})
    places_dict = {p['name']: Location(**p) for p in places_config['all_places']}
    default_place = places_config.get('default_place', list(places_dict.keys())[0])
    place_lists = places_config.get('place_lists', {})

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
    # Build YAML string manually for better formatting control
    lines = []

    # File header comment
    lines.append("# geo configuration file")
    lines.append("# Programmatic updates (e.g., --add-place) will reformat this file")
    lines.append("")

    # Track which sections we've explicitly handled
    handled_sections = set()

    # Logging section
    if 'logging' in config:
        handled_sections.add('logging')
        lines.append("# Logging configuration")
        lines.append("logging:")
        for key, value in config['logging'].items():
            lines.append(f"  {key}: {value}")
        lines.append("")  # Blank line after section

    # Grid section
    if 'grid' in config:
        handled_sections.add('grid')
        lines.append("# Grid layout configuration")
        lines.append("grid:")
        grid_config = config['grid']
        lines.append("  # Maximum automatic grid size (rows x columns)")
        lines.append("  # Used when no --grid option is specified")
        lines.append("  # Locations exceeding this will be batched into multiple images")
        for key, value in grid_config.items():
            lines.append(f"  {key}: {value}")
        lines.append("")  # Blank line after section

    # Places section
    if 'places' in config:
        handled_sections.add('places')
        lines.append("# Places configuration")
        lines.append("places:")
        places_config = config['places']

        # Default place
        if 'default_place' in places_config:
            lines.append("  # Default place used when no location is specified")
            lines.append(f"  default_place: {places_config['default_place']}")
            lines.append("")  # Blank line before all_places

        # All places (compact format)
        if 'all_places' in places_config:
            lines.append("  # All available places (name, latitude, longitude)")
            lines.append("  # Timezone is auto-detected from coordinates")
            lines.append("  all_places:")
            for place in places_config['all_places']:
                # Use flow style (one line) for each place
                # Quote the name to handle commas and special characters
                name = place['name']
                lat = place['lat']
                lon = place['lon']
                lines.append(f"    - {{name: \"{name}\", lat: {lat}, lon: {lon}}}")
            lines.append("")  # Blank line after all_places

        # Place lists
        if 'place_lists' in places_config:
            lines.append("  # Predefined place lists (use with --list/-L)")
            lines.append("  place_lists:")
            for list_name, places in places_config['place_lists'].items():
                lines.append(f"    {list_name}:")
                for place_name in places:
                    lines.append(f"      - {place_name}")
                if list_name != list(places_config['place_lists'].keys())[-1]:
                    lines.append("")  # Blank line between lists

    # Fallback: write any other sections using standard YAML formatting
    # This preserves sections we don't explicitly handle above
    for section_name, section_data in config.items():
        if section_name not in handled_sections:
            lines.append(f"# {section_name.replace('_', ' ').title()} section")
            lines.append(f"{section_name}:")
            # Use standard YAML dump for unknown sections
            section_yaml = yaml.dump(section_data, default_flow_style=False, sort_keys=False)
            for line in section_yaml.rstrip().split('\n'):
                lines.append(f"  {line}")
            lines.append("")  # Blank line after section

    # Write to file
    with open(config_path, "w") as f:
        f.write("\n".join(lines))
        if lines and not lines[-1] == "":
            f.write("\n")  # Ensure file ends with newline


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
            config = yaml.safe_load(f)

        # Check if place already exists
        places_config = config.get('places', {})
        all_places = places_config.get('all_places', [])

        for place in all_places:
            if place['name'] == place_name:
                print(f"\nWARNING: '{place_name}' already exists in config with coordinates {place['lat']}, {place['lon']}")
                response = input("Overwrite? (y/n): ")
                if response.lower() != 'y':
                    print("Cancelled.")
                    sys.exit(0)
                all_places.remove(place)
                break

        # Add new place
        new_place = {'name': place_name, 'lat': lat, 'lon': lon}
        all_places.append(new_place)

        # Save config with proper formatting
        save_config(config, config_path)

        print(f"\n✓ Added '{place_name}' to {config_path}")
        print(f"  Coordinates: {lat}, {lon}")
        if tz:
            print(f"  Timezone will be auto-detected as: {tz}")

    except Exception as e:
        print(f"ERROR: Failed to look up or add place: {e}")
        sys.exit(1)
