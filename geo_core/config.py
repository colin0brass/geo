"""Configuration helpers shared across CLI and plotting layers."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from matplotlib import colormaps as mpl_colormaps

logger = logging.getLogger("geo")

VALID_COLOUR_MODES = ("y_value", "year")
DEFAULT_COLOUR_MODE = VALID_COLOUR_MODES[0]
DEFAULT_COLORMAP = "turbo"


def load_grid_settings(config_file: Path) -> tuple[int, int]:
    """Load grid maximum dimensions from config YAML file."""
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f) or {}
            grid_config = config.get('grid', {})
            max_rows = grid_config.get('max_auto_rows', 4)
            max_cols = grid_config.get('max_auto_cols', 6)
            return (max_rows, max_cols)
    except Exception as exc:
        logger.warning(f"Failed to load grid settings from {config_file}: {exc}. Using defaults (4x6).")
        return (4, 6)


def load_colour_mode(config_file: Path, cli_colour_mode: str | None = None) -> str:
    """Resolve colour mode from CLI override or config file."""
    if cli_colour_mode is not None:
        return cli_colour_mode

    default_mode = DEFAULT_COLOUR_MODE
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f) or {}

        config_mode = config.get('plotting', {}).get('colour_mode', default_mode)
        if config_mode not in VALID_COLOUR_MODES:
            allowed = ', '.join(VALID_COLOUR_MODES)
            raise ValueError(f"Invalid plotting.colour_mode '{config_mode}' in {config_file}. Use one of: {allowed}.")

        return config_mode
    except ValueError:
        raise
    except Exception as exc:
        logger.warning(f"Failed to load colour mode from {config_file}: {exc}. Using default '{default_mode}'.")
        return default_mode


def load_colormap(config_file: Path) -> str:
    """Resolve plotting colormap from config YAML."""
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f) or {}

        plotting_config = config.get('plotting', {})

        configured_valid_colormaps = plotting_config.get('valid_colormaps')
        valid_colormaps = None
        if configured_valid_colormaps is not None:
            parsed_colormaps = []
            if isinstance(configured_valid_colormaps, (list, tuple)):
                for cmap_name in configured_valid_colormaps:
                    if isinstance(cmap_name, str):
                        cmap_name = cmap_name.strip()
                        if cmap_name and cmap_name in mpl_colormaps:
                            parsed_colormaps.append(cmap_name)

            if parsed_colormaps:
                valid_colormaps = parsed_colormaps
            else:
                logger.warning(
                    f"Invalid plotting.valid_colormaps in {config_file}. "
                    "Ignoring this setting and using matplotlib colormap validation."
                )

        default_colormap = valid_colormaps[0] if valid_colormaps else DEFAULT_COLORMAP

        colormap = plotting_config.get('colormap', default_colormap)
        if not isinstance(colormap, str) or not colormap.strip():
            logger.warning(
                f"Invalid plotting.colormap '{colormap}' in {config_file}. "
                f"Using default '{default_colormap}'."
            )
            return default_colormap

        colormap = colormap.strip()
        if valid_colormaps and colormap not in valid_colormaps:
            logger.warning(
                f"Unknown plotting.colormap '{colormap}' in {config_file}. "
                f"Allowed values: {', '.join(valid_colormaps)}. Using default '{default_colormap}'."
            )
            return default_colormap

        return colormap
    except Exception as exc:
        logger.warning(f"Failed to load colormap from {config_file}: {exc}. Using default '{DEFAULT_COLORMAP}'.")
        return DEFAULT_COLORMAP


def load_plot_text_config(config_path: Path = Path("config.yaml")) -> dict:
    """Load plot text configuration from config file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f) or {}
            return config.get('plot_text', {})
    except Exception:
        return {}


def load_measure_labels_config(config_path: Path = Path("config.yaml")) -> dict[str, dict[str, str]]:
    """Load measure label/unit mappings from config."""
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
    """Get formatted plot text from configuration."""
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
        'single_plot_credit': "Analysis & visualisation by Colin Osborne",
    }

    pattern = config.get(key, defaults.get(key, ""))

    if 'location' in kwargs and ('filename' in key):
        kwargs['location'] = kwargs['location'].replace(' ', '_').replace(',', '')

    try:
        return pattern.format(**kwargs)
    except KeyError:
        return pattern
