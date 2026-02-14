"""Configuration helpers shared across CLI and plotting layers."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from matplotlib import colormaps as mpl_colormaps

from .constants import (
    DEFAULT_COLOUR_MODE,
    DEFAULT_COLORMAP,
    DEFAULT_GRID_SETTINGS,
    DEFAULT_RETRIEVAL_SETTINGS,
    DEFAULT_RUNTIME_PATHS,
    REQUIRED_MEASURE_LABEL_KEYS,
    REQUIRED_PLOT_TEXT_KEYS,
    VALID_COLOUR_MODES,
)

logger = logging.getLogger("geo")


class CoreConfigService:
    """Stateful access wrapper for core config helpers."""

    def __init__(self, config_file: Path = Path("config.yaml")) -> None:
        self.config_file = config_file

    def load_grid_settings(self) -> tuple[int, int]:
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f) or {}

        grid_config = config.get('grid', {})
        if not isinstance(grid_config, dict):
            raise ValueError(f"Invalid grid section in {self.config_file}; expected mapping.")

        max_rows = grid_config.get('max_auto_rows', DEFAULT_GRID_SETTINGS['max_auto_rows'])
        max_cols = grid_config.get('max_auto_cols', DEFAULT_GRID_SETTINGS['max_auto_cols'])

        if not isinstance(max_rows, int) or max_rows <= 0:
            raise ValueError("grid.max_auto_rows must be a positive integer")
        if not isinstance(max_cols, int) or max_cols <= 0:
            raise ValueError("grid.max_auto_cols must be a positive integer")

        return (max_rows, max_cols)

    def load_colour_mode(self, cli_colour_mode: str | None = None) -> str:
        if cli_colour_mode is not None:
            return cli_colour_mode

        default_mode = DEFAULT_COLOUR_MODE
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f) or {}

        plotting = config.get('plotting', {})
        if not isinstance(plotting, dict):
            raise ValueError(f"Invalid plotting section in {self.config_file}; expected mapping.")

        config_mode = plotting.get('colour_mode', default_mode)
        if config_mode not in VALID_COLOUR_MODES:
            allowed = ', '.join(VALID_COLOUR_MODES)
            raise ValueError(
                f"Invalid plotting.colour_mode '{config_mode}' in {self.config_file}. Use one of: {allowed}."
            )

        return config_mode

    def load_colormap(self) -> str:
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f) or {}

        plotting_config = config.get('plotting', {})
        if not isinstance(plotting_config, dict):
            raise ValueError(f"Invalid plotting section in {self.config_file}; expected mapping.")

        configured_valid_colormaps = plotting_config.get('valid_colormaps')
        valid_colormaps: list[str] | None = None
        if configured_valid_colormaps is not None:
            if not isinstance(configured_valid_colormaps, (list, tuple)) or not configured_valid_colormaps:
                raise ValueError("plotting.valid_colormaps must be a non-empty list of valid colormap names")

            parsed_colormaps: list[str] = []
            for cmap_name in configured_valid_colormaps:
                if not isinstance(cmap_name, str) or not cmap_name.strip():
                    raise ValueError("plotting.valid_colormaps entries must be non-empty strings")
                stripped_name = cmap_name.strip()
                if stripped_name not in mpl_colormaps:
                    raise ValueError(f"Unknown colormap '{stripped_name}' in plotting.valid_colormaps")
                parsed_colormaps.append(stripped_name)

            valid_colormaps = parsed_colormaps

        default_colormap = valid_colormaps[0] if valid_colormaps else DEFAULT_COLORMAP
        colormap = plotting_config.get('colormap', default_colormap)
        if not isinstance(colormap, str) or not colormap.strip():
            raise ValueError("plotting.colormap must be a non-empty string")

        colormap = colormap.strip()
        if colormap not in mpl_colormaps:
            raise ValueError(f"Unknown plotting.colormap '{colormap}'")
        if valid_colormaps and colormap not in valid_colormaps:
            raise ValueError(
                f"plotting.colormap '{colormap}' is not in plotting.valid_colormaps: {', '.join(valid_colormaps)}"
            )

        return colormap

    def load_plot_text_config(self) -> dict:
        return load_plot_text_config(self.config_file)

    def load_runtime_paths(self) -> dict[str, str]:
        return load_runtime_paths(self.config_file)

    def load_retrieval_settings(self) -> dict[str, float | int | dict[str, str]]:
        return load_retrieval_settings(self.config_file)

    def load_measure_labels_config(self) -> dict[str, dict[str, object]]:
        return load_measure_labels_config(self.config_file)

    def get_plot_text(self, key: str, **kwargs) -> str:
        return get_plot_text(self.load_plot_text_config(), key, **kwargs)

    @staticmethod
    def extract_places_config(config: dict) -> tuple[list[dict], str, dict]:
        return extract_places_config(config)

    @staticmethod
    def find_place_by_name(all_places: list[dict], place_name: str) -> dict | None:
        return find_place_by_name(all_places, place_name)

    @staticmethod
    def render_config_yaml(config: dict) -> str:
        return render_config_yaml(config)


def load_plot_text_config(config_path: Path = Path("config.yaml")) -> dict:
    """Load and validate required plot text templates from config file."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f) or {}

    plot_text = config.get('plot_text')
    if not isinstance(plot_text, dict):
        raise ValueError(f"Invalid plot_text section in {config_path}; expected mapping.")

    missing_keys = [key for key in REQUIRED_PLOT_TEXT_KEYS if key not in plot_text]
    if missing_keys:
        missing = ', '.join(missing_keys)
        raise ValueError(f"Missing required plot_text keys in {config_path}: {missing}")

    for key in REQUIRED_PLOT_TEXT_KEYS:
        value = plot_text[key]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"plot_text.{key} must be a non-empty string")

    return {key: plot_text[key] for key in REQUIRED_PLOT_TEXT_KEYS}


def load_runtime_paths(config_file: Path = Path("config.yaml")) -> dict[str, str]:
    """Load runtime path defaults from config YAML.

    Paths are read from top-level ``runtime_paths`` and merged with minimal
    defaults when keys are missing.
    """
    paths = DEFAULT_RUNTIME_PATHS.copy()

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f) or {}

    runtime_paths = config.get('runtime_paths', {})
    if not isinstance(runtime_paths, dict):
        raise ValueError(f"Invalid runtime_paths section in {config_file}; expected mapping.")

    for key in DEFAULT_RUNTIME_PATHS:
        if key in runtime_paths:
            value = runtime_paths[key]
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"runtime_paths.{key} must be a non-empty string")
            paths[key] = value.strip()

    return paths


def load_retrieval_settings(config_file: Path = Path("config.yaml")) -> dict[str, float | int | dict[str, str]]:
    """Load retrieval tuning settings from config YAML.

    Settings are expected in the top-level ``retrieval`` section.
    Minimal defaults are applied when the section or keys are missing.
    """
    settings = DEFAULT_RETRIEVAL_SETTINGS.copy()

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f) or {}

    retrieval = config.get('retrieval', {})
    if not isinstance(retrieval, dict):
        raise ValueError(f"Invalid retrieval section in {config_file}; expected mapping.")

    if 'half_box_deg' in retrieval:
        settings['half_box_deg'] = float(retrieval['half_box_deg'])
    if settings['half_box_deg'] <= 0:
        raise ValueError("retrieval.half_box_deg must be > 0")

    if 'max_nearest_time_delta_minutes' in retrieval:
        settings['max_nearest_time_delta_minutes'] = int(retrieval['max_nearest_time_delta_minutes'])
    if settings['max_nearest_time_delta_minutes'] <= 0:
        raise ValueError("retrieval.max_nearest_time_delta_minutes must be > 0")

    if 'month_fetch_day_span_threshold' in retrieval:
        settings['month_fetch_day_span_threshold'] = int(retrieval['month_fetch_day_span_threshold'])
    if settings['month_fetch_day_span_threshold'] <= 0:
        raise ValueError("retrieval.month_fetch_day_span_threshold must be > 0")

    valid_fetch_modes = {'monthly', 'yearly', 'auto'}

    configured_fetch_mode = retrieval.get('fetch_mode')
    if configured_fetch_mode is not None:
        if not isinstance(configured_fetch_mode, dict):
            raise ValueError("retrieval.fetch_mode must be a mapping")

        merged_fetch_mode = settings['fetch_mode'].copy()
        canonical_measures = (
            'noon_temperature',
            'daily_precipitation',
            'daily_solar_radiation_energy',
        )
        for metric_name in canonical_measures:
            if metric_name in configured_fetch_mode:
                mode_value = configured_fetch_mode[metric_name]
                if not isinstance(mode_value, str) or not mode_value.strip():
                    raise ValueError(f"retrieval.fetch_mode.{metric_name} must be a non-empty string")
                merged_fetch_mode[metric_name] = mode_value.strip().lower()

        nested_legacy_key_map = {
            'temp': 'noon_temperature',
            'precipitation': 'daily_precipitation',
            'solar': 'daily_solar_radiation_energy',
        }
        for legacy_key, canonical_key in nested_legacy_key_map.items():
            if legacy_key in configured_fetch_mode:
                mode_value = configured_fetch_mode[legacy_key]
                if not isinstance(mode_value, str) or not mode_value.strip():
                    raise ValueError(f"retrieval.fetch_mode.{legacy_key} must be a non-empty string")
                merged_fetch_mode[canonical_key] = mode_value.strip().lower()

        settings['fetch_mode'] = merged_fetch_mode

    legacy_fetch_mode_map = {
        'temp_fetch_mode': 'noon_temperature',
        'precipitation_fetch_mode': 'daily_precipitation',
        'solar_fetch_mode': 'daily_solar_radiation_energy',
    }
    for legacy_key, metric_name in legacy_fetch_mode_map.items():
        if legacy_key in retrieval:
            legacy_value = retrieval[legacy_key]
            if not isinstance(legacy_value, str) or not legacy_value.strip():
                raise ValueError(f"retrieval.{legacy_key} must be a non-empty string")
            settings['fetch_mode'][metric_name] = legacy_value.strip().lower()

    for metric_name, mode_value in settings['fetch_mode'].items():
        if mode_value not in valid_fetch_modes:
            allowed = ', '.join(sorted(valid_fetch_modes))
            raise ValueError(f"retrieval.fetch_mode.{metric_name} must be one of: {allowed}")

    valid_daily_sources_by_measure = {
        'noon_temperature': {'hourly'},
        'daily_precipitation': {'hourly', 'daily_statistics'},
        'daily_solar_radiation_energy': {'hourly'},
    }

    configured_daily_source = retrieval.get('daily_source')
    if configured_daily_source is not None:
        if not isinstance(configured_daily_source, dict):
            raise ValueError("retrieval.daily_source must be a mapping")

        merged_daily_source = settings['daily_source'].copy()
        for metric_name in valid_daily_sources_by_measure:
            if metric_name in configured_daily_source:
                source_value = configured_daily_source[metric_name]
                if not isinstance(source_value, str) or not source_value.strip():
                    raise ValueError(f"retrieval.daily_source.{metric_name} must be a non-empty string")
                merged_daily_source[metric_name] = source_value.strip().lower()
        settings['daily_source'] = merged_daily_source

    for metric_name, source_value in settings['daily_source'].items():
        allowed_values = valid_daily_sources_by_measure[metric_name]
        if source_value not in allowed_values:
            allowed = ', '.join(sorted(allowed_values))
            raise ValueError(f"retrieval.daily_source.{metric_name} must be one of: {allowed}")

    return settings


def load_measure_labels_config(config_path: Path = Path("config.yaml")) -> dict[str, dict[str, object]]:
    """Load and validate measure label/unit mappings from config."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f) or {}

    plotting = config.get('plotting')
    if not isinstance(plotting, dict):
        raise ValueError(f"Invalid plotting section in {config_path}; expected mapping.")

    configured = plotting.get('measure_labels')
    if not isinstance(configured, dict) or not configured:
        raise ValueError(f"Invalid plotting.measure_labels section in {config_path}; expected non-empty mapping.")

    validated: dict[str, dict[str, object]] = {}
    for measure_key, metadata in configured.items():
        if not isinstance(measure_key, str) or not measure_key.strip():
            raise ValueError("plotting.measure_labels keys must be non-empty strings")
        if not isinstance(metadata, dict):
            raise ValueError(f"plotting.measure_labels.{measure_key} must be a mapping")

        entry: dict[str, object] = {}
        for field_name in REQUIRED_MEASURE_LABEL_KEYS:
            value = metadata.get(field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"plotting.measure_labels.{measure_key}.{field_name} must be a non-empty string"
                )
            entry[field_name] = value

        for numeric_field in ('y_min', 'y_max', 'y_step'):
            if numeric_field in metadata and metadata[numeric_field] is not None:
                try:
                    entry[numeric_field] = float(metadata[numeric_field])
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"plotting.measure_labels.{measure_key}.{numeric_field} must be numeric"
                    ) from exc

        y_min = entry.get('y_min')
        y_max = entry.get('y_max')
        y_step = entry.get('y_step')
        if y_step is not None and y_step <= 0:
            raise ValueError(f"plotting.measure_labels.{measure_key}.y_step must be > 0")
        if y_min is not None and y_max is not None and y_min >= y_max:
            raise ValueError(f"plotting.measure_labels.{measure_key}.y_min must be < y_max")

        validated[measure_key] = entry

    return validated


def get_plot_text(config: dict, key: str, **kwargs) -> str:
    """Get formatted plot text from configuration."""
    if key not in config:
        raise KeyError(f"Missing plot_text key '{key}'")

    pattern = config[key]
    if not isinstance(pattern, str) or not pattern.strip():
        raise ValueError(f"plot_text.{key} must be a non-empty string")

    if 'location' in kwargs and ('filename' in key):
        kwargs['location'] = kwargs['location'].replace(' ', '_').replace(',', '')

    try:
        return pattern.format(**kwargs)
    except KeyError as exc:
        missing = exc.args[0]
        raise ValueError(f"Missing format placeholder '{missing}' for plot_text.{key}") from exc


def extract_places_config(config: dict) -> tuple[list[dict], str, dict]:
    """Extract places payload from full config mapping.

    Returns:
        tuple[list[dict], str, dict]: (all_places, default_place, place_lists)
    """
    places_config = config.get('places', {}) if isinstance(config, dict) else {}
    all_places = places_config.get('all_places', []) if isinstance(places_config, dict) else []
    place_lists = places_config.get('place_lists', {}) if isinstance(places_config, dict) else {}

    if not isinstance(all_places, list):
        all_places = []
    if not isinstance(place_lists, dict):
        place_lists = {}

    default_place = ""
    if isinstance(places_config, dict):
        configured_default = places_config.get('default_place')
        if isinstance(configured_default, str):
            default_place = configured_default

    if not default_place and all_places:
        first_place = all_places[0]
        if isinstance(first_place, dict):
            first_name = first_place.get('name')
            if isinstance(first_name, str):
                default_place = first_name

    return all_places, default_place, place_lists


def find_place_by_name(all_places: list[dict], place_name: str) -> dict | None:
    """Return the first place entry matching a name, or None."""
    for place in all_places:
        if isinstance(place, dict) and place.get('name') == place_name:
            return place
    return None


def render_config_yaml(config: dict) -> str:
    """Render config mapping to the project's canonical YAML format."""
    lines = []

    lines.append("# geo configuration file")
    lines.append("# Programmatic updates (e.g., --add-place) will reformat this file")
    lines.append("")

    handled_sections = set()

    if 'logging' in config:
        handled_sections.add('logging')
        lines.append("# Logging configuration")
        lines.append("logging:")
        for key, value in config['logging'].items():
            lines.append(f"  {key}: {value}")
        lines.append("")

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
        lines.append("")

    if 'places' in config:
        handled_sections.add('places')
        lines.append("# Places configuration")
        lines.append("places:")
        places_config = config['places']

        if 'default_place' in places_config:
            lines.append("  # Default place used when no location is specified")
            lines.append(f"  default_place: {places_config['default_place']}")
            lines.append("")

        if 'all_places' in places_config:
            lines.append("  # All available places (name, latitude, longitude)")
            lines.append("  # Timezone is auto-detected from coordinates")
            lines.append("  all_places:")
            for place in places_config['all_places']:
                name = place['name']
                lat = place['lat']
                lon = place['lon']
                lines.append(f"    - {{name: \"{name}\", lat: {lat}, lon: {lon}}}")
            lines.append("")

        if 'place_lists' in places_config:
            lines.append("  # Predefined place lists (use with --list/-L)")
            lines.append("  place_lists:")
            for list_name, places in places_config['place_lists'].items():
                lines.append(f"    {list_name}:")
                for place_name in places:
                    lines.append(f"      - {place_name}")
                if list_name != list(places_config['place_lists'].keys())[-1]:
                    lines.append("")

    for section_name, section_data in config.items():
        if section_name not in handled_sections:
            lines.append(f"# {section_name.replace('_', ' ').title()} section")
            lines.append(f"{section_name}:")
            section_yaml = yaml.dump(section_data, default_flow_style=False, sort_keys=False)
            for line in section_yaml.rstrip().split('\n'):
                lines.append(f"  {line}")
            lines.append("")

    output = "\n".join(lines)
    if lines and lines[-1] != "":
        output += "\n"
    return output
