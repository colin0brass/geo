"""
CLI and configuration utilities for geo.

Handles command-line argument parsing and grid layout calculations.
"""

from __future__ import annotations

import argparse
import difflib
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml

from geo_data.cds_base import Location
from config_manager import load_places
from geo_core.config import (
    CoreConfigService,
    VALID_COLOUR_MODES,
    load_runtime_paths as core_load_runtime_paths,
)
from geo_core.formatting import condense_year_ranges
from geo_core.grid import calculate_grid_layout as core_calculate_grid_layout

logger = logging.getLogger("geo")

__version__ = "1.0.0"
VALID_MEASURES = (
    "noon_temperature",
    "daily_precipitation",
    "daily_solar_radiation_energy",
)
MEASURE_ALIASES = {
    "temp": "noon_temperature",
    "precipitation": "daily_precipitation",
    "solar": "daily_solar_radiation_energy",
}
ALL_MEASURES_TOKEN = "all"
MEASURE_CHOICES = tuple(VALID_MEASURES) + tuple(MEASURE_ALIASES.keys()) + (ALL_MEASURES_TOKEN,)
DEFAULT_COLOUR_MODE = VALID_COLOUR_MODES[0]
DEFAULT_MEASURE = VALID_MEASURES[0]
DEFAULT_COLORMAP = "turbo"
VALID_DOWNLOAD_BY = ("config", "month", "year", "compare")


def _resolve_runtime_path_defaults() -> tuple[Path, dict[str, str]]:
    """Resolve config path and runtime path defaults from CLI pre-parse."""
    config_probe = argparse.ArgumentParser(add_help=False)
    config_probe.add_argument("--config", type=Path, default=Path("config.yaml"))
    probe_args, _ = config_probe.parse_known_args()
    config_path = probe_args.config

    if "--help" in sys.argv or "-h" in sys.argv:
        return config_path, {
            "cache_dir": "era5_cache",
            "data_cache_dir": "data_cache",
            "out_dir": "output",
            "settings_file": "geo_plot/settings.yaml",
        }

    try:
        runtime_paths = core_load_runtime_paths(config_path)
    except Exception as exc:
        raise CLIError(
            f"Failed to load runtime_paths from {config_path}: {exc}",
            "Fix config.yaml runtime_paths values or provide a valid --config path.",
        )

    return config_path, runtime_paths


class CLIError(ValueError):
    """User-facing CLI validation error with optional hint text."""

    def __init__(self, message: str, hint: str | None = None):
        self.message = message
        self.hint = hint
        super().__init__(self.__str__())

    def __str__(self) -> str:
        if self.hint:
            return f"{self.message}\nHint: {self.hint}"
        return self.message


class FriendlyArgumentParser(argparse.ArgumentParser):
    """ArgumentParser that raises CLIError instead of exiting immediately."""

    def error(self, message: str) -> None:
        hint = None
        if "unrecognized arguments" in message:
            if "--start-year" in message or "--end-year" in message:
                hint = "Use --years YYYY or --years YYYY-YYYY (for example: --years 2020-2025)."
            elif "--colour_mode" in message:
                hint = "Use --colour-mode (or --color-mode), not --colour_mode."
        usage = self.format_usage().strip()
        raise CLIError(f"Argument error: {message}\n{usage}", hint=hint)


def _suggest_values(value: str, options: list[str], max_suggestions: int = 5) -> str | None:
    """Return a short suggestion string from close matches."""
    matches = difflib.get_close_matches(value, options, n=max_suggestions, cutoff=0.5)
    if not matches:
        return None
    return ", ".join(matches)


def normalize_measure(measure: str) -> str:
    """Normalize CLI measure aliases to canonical internal measure keys."""
    return MEASURE_ALIASES.get(measure, measure)


def parse_measure_selection(measure_arg: str) -> list[str]:
    """Parse comma-separated CLI measure selection into canonical measure keys."""
    if not isinstance(measure_arg, str) or not measure_arg.strip():
        raise CLIError(
            "Measure value cannot be empty.",
            "Use --measure noon_temperature, --measure daily_precipitation, or --measure all.",
        )

    raw_items = [item.strip().lower() for item in measure_arg.split(",") if item.strip()]
    if not raw_items:
        raise CLIError(
            "Measure value cannot be empty.",
            "Use --measure noon_temperature, --measure daily_precipitation, or --measure all.",
        )

    if ALL_MEASURES_TOKEN in raw_items:
        if len(raw_items) > 1:
            raise CLIError(
                "'all' cannot be combined with other measure values.",
                "Use either --measure all, or provide a comma-separated list without 'all'.",
            )
        return list(VALID_MEASURES)

    resolved: list[str] = []
    for item in raw_items:
        canonical = normalize_measure(item)
        if canonical not in VALID_MEASURES:
            allowed = ", ".join(MEASURE_CHOICES)
            raise CLIError(
                f"Unknown measure '{item}'.",
                f"Allowed values: {allowed}. You can also use comma-separated values.",
            )
        if canonical not in resolved:
            resolved.append(canonical)

    return resolved


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for geo.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    config_path_default, runtime_paths = _resolve_runtime_path_defaults()

    parser = FriendlyArgumentParser(
        description="Generate geographic temperature plots from ERA5 data.",
        epilog="""
Examples:
  %(prog)s --years 2024 --show                    # Default place, show all plots
  %(prog)s --place "Austin, TX" --years 2020-2025  # Specific place
  %(prog)s --lat 30.27 --lon -97.74 --years 2024  # Custom coordinates
  %(prog)s --all --years 2024                     # All configured places
  %(prog)s --list all --years 2024                # Alias for --all
  %(prog)s --list-places                          # List available places
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    # Location selection (mutually exclusive)
    location_group = parser.add_argument_group("location selection (choose one)")
    location_exclusive = location_group.add_mutually_exclusive_group()
    location_exclusive.add_argument(
        "-p", "--place",
        type=str,
        default=None,
        help="Name of a configured place, or custom place name with --lat/--lon"
    )
    location_exclusive.add_argument(
        "-l", "--list",
        dest="place_list",
        type=str,
        nargs='?',
        const='default',
        default=None,
        help="Name of a predefined place list (e.g., 'default', 'us_cities'). If no list name is provided, uses 'default'. Use 'all' as an alias for --all."
    )
    location_exclusive.add_argument(
        "-a", "--all",
        action="store_true",
        help="Retrieve data for all configured locations"
    )

    # Custom location coordinates
    coord_group = parser.add_argument_group("custom location (use with --place)")
    coord_group.add_argument("--lat", type=float, help="Latitude of custom location")
    coord_group.add_argument("--lon", type=float, help="Longitude of custom location")
    coord_group.add_argument("--tz", type=str, help="Timezone (optional, auto-detected from coordinates if omitted)")

    # Information
    info_group = parser.add_argument_group("information")
    info_group.add_argument(
        "-L", "--list-places",
        action="store_true",
        help="List all available places and place lists, then exit"
    )
    info_group.add_argument(
        "-ly", "--list-years",
        action="store_true",
        help="List all places with their cached years from data cache, then exit"
    )
    info_group.add_argument(
        "--add-place",
        type=str,
        metavar="NAME",
        help="Add a new place to config (looks up coordinates online). Usage: --add-place 'Seattle, WA'"
    )

    # Time period
    time_group = parser.add_argument_group("time period")
    current_year = datetime.now().year
    time_group.add_argument(
        "-y", "--years",
        type=str,
        default=str(current_year - 1),
        help=f"Year or year range (e.g., 2025 or 2020-2025). Default: {current_year - 1}"
    )

    # Output options
    output_group = parser.add_argument_group("output options")
    output_group.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(runtime_paths["cache_dir"]),
        help=f"Cache directory for NetCDF files (default: {runtime_paths['cache_dir']})"
    )
    output_group.add_argument(
        "--data-cache-dir",
        type=Path,
        default=Path(runtime_paths["data_cache_dir"]),
        help=f"Cache directory for CSV data files (default: {runtime_paths['data_cache_dir']})"
    )
    output_group.add_argument(
        "--out-dir",
        type=Path,
        default=Path(runtime_paths["out_dir"]),
        help=f"Output directory for plots (default: {runtime_paths['out_dir']})"
    )
    output_group.add_argument(
        "--config",
        type=Path,
        default=config_path_default,
        help=f"Path to config YAML file (default: {config_path_default})"
    )
    output_group.add_argument(
        "--settings",
        type=Path,
        default=Path(runtime_paths["settings_file"]),
        help=f"Path to plot settings YAML file (default: {runtime_paths['settings_file']})"
    )
    output_group.add_argument(
        "-m", "--measure",
        type=str,
        default=DEFAULT_MEASURE,
        help=(
            "Data measure to use: 'noon_temperature', "
            "'daily_precipitation', 'daily_solar_radiation_energy' "
            "(aliases: 'temp', 'precipitation', 'solar'). "
            "Use comma-separated values to run multiple measures, or 'all'."
        )
    )

    # Display options
    display_group = parser.add_argument_group("display options")
    display_group.add_argument(
        "-s", "--show",
        action="store_true",
        help="Show plots interactively after generation"
    )
    display_group.add_argument(
        "-g", "--grid",
        type=str,
        default=None,
        help="Grid dimensions as COLSxROWS (e.g., 4x3 for 4 columns × 3 rows)"
    )
    display_group.add_argument(
        "--colour-mode", "--color-mode",
        dest="colour_mode",
        choices=VALID_COLOUR_MODES,
        default=None,
        help="Colour mapping mode: 'y_value' (default) or 'year' for trend-over-time colouring"
    )

    # Advanced options
    advanced_group = parser.add_argument_group("advanced options")
    advanced_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview operations without downloading data or creating plots"
    )
    advanced_group.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress console output except errors (log file unaffected)"
    )
    advanced_group.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose console output (DEBUG level, log file always at DEBUG)"
    )
    advanced_group.add_argument(
        "--download-by",
        choices=VALID_DOWNLOAD_BY,
        default="config",
        help=(
            "Download chunking override: 'config' (use config.yaml), 'month', 'year', "
            "or 'compare' (benchmark month vs year for one selected year)."
        ),
    )
    advanced_group.add_argument(
        "-u", "--update-cache",
        dest="update_cache",
        action="store_true",
        help=(
            "Overwrite existing values for matching dates when writing to data cache. "
            "Default preserves existing cached values."
        ),
    )

    args = parser.parse_args()
    args.measures = parse_measure_selection(args.measure)
    args.measure = args.measures[0]
    return args


def parse_years(years_str: str) -> tuple[int, int]:
    """
    Parse year string into start and end years.

    Args:
        years_str: Year string in format "YYYY" or "YYYY-YYYY".

    Returns:
        tuple[int, int]: (start_year, end_year)

    Raises:
        CLIError: If the year format is invalid.
    """
    if not years_str:
        raise CLIError(
            "--years argument is required.",
            "Use --years 2025 or --years 2020-2025."
        )
    if '-' in years_str:
        try:
            if years_str.count('-') != 1:
                raise ValueError()
            start, end = years_str.split('-')
            start_year = int(start)
            end_year = int(end)
            if start_year > end_year:
                raise ValueError()
        except ValueError:
            raise CLIError(
                f"Invalid --years format: '{years_str}'.",
                "Use YYYY or YYYY-YYYY with start <= end (for example: --years 2020-2025)."
            )
    else:
        try:
            year = int(years_str)
            start_year = year
            end_year = year
        except Exception:
            raise CLIError(
                f"Invalid --years format: '{years_str}'.",
                "Use YYYY or YYYY-YYYY (for example: --years 2025 or --years 2020-2025)."
            )
    return start_year, end_year


def get_place_list(args: argparse.Namespace, places: dict[str, Location], default_place: str, place_lists: dict[str, list[str]]) -> tuple[list[Location], str | None]:
    """
    Determine the list of places to process based on command-line arguments.

    Args:
        args: Parsed command-line arguments.
        places: Dictionary mapping place names to Location objects.
        default_place: Name of the default place to use when no arguments provided.
        place_lists: Dictionary of predefined place lists.

    Returns:
        tuple: (List of Location objects to process, list name or None for single places)

    Raises:
        CLIError: If invalid place or place list is specified.
    """
    # --all takes precedence
    if args.all:
        return list(places.values()), "all"

    # --list all acts as an alias for --all
    if args.place_list == "all":
        return list(places.values()), "all"

    # --place-list uses a named list
    if args.place_list:
        if args.place_list not in place_lists:
            available_lists = sorted(place_lists.keys())
            suggestions = _suggest_values(args.place_list, available_lists)
            hint = "Use --list-places to see all available place lists."
            if suggestions:
                hint = f"Did you mean one of: {suggestions}?"
            raise CLIError(
                f"Unknown place list '{args.place_list}'.",
                hint
            )
        place_names = place_lists[args.place_list]
        return [places[name] for name in place_names if name in places], args.place_list

    # --place uses a specific place
    if args.place:
        if args.place in places:
            # Use configured place
            return [places[args.place]], None
        # Custom place - require coordinates
        if args.lat is None or args.lon is None:
            available_places = sorted(places.keys())
            suggestions = _suggest_values(args.place, available_places)
            if suggestions:
                hint = f"Did you mean: {suggestions}?"
            else:
                hint = "Provide --lat and --lon for a custom place, or run --list-places to see configured names."
            raise CLIError(
                f"Unknown place '{args.place}'.",
                hint
            )
        # Create custom location (tz will auto-detect if not provided)
        if args.tz:
            return [Location(name=args.place, lat=args.lat, lon=args.lon, tz=args.tz)], None
        else:
            return [Location(name=args.place, lat=args.lat, lon=args.lon)], None

    # No arguments: use default place
    if default_place in places:
        return [places[default_place]], None
    else:
        raise CLIError(
            f"Default place '{default_place}' not found in config.yaml.",
            "Set places.default_place to a valid configured place name."
        )


def parse_grid(grid_str: str | None) -> tuple[int, int] | None:
    """
    Parse grid dimension string into (rows, cols).

    Args:
        grid_str: Grid string in format "COLSxROWS" (e.g., "4x3" = 4 columns, 3 rows) or None.

    Returns:
        tuple[int, int] | None: (rows, cols) or None if grid_str is None.

    Raises:
        CLIError: If the grid format is invalid.
    """
    if grid_str is None:
        return None

    if 'x' not in grid_str.lower():
        raise CLIError(
            f"Invalid grid format '{grid_str}'.",
            "Use COLSxROWS (e.g., --grid 4x3 for 4 columns by 3 rows)."
        )

    try:
        parts = grid_str.lower().split('x')
        if len(parts) != 2:
            raise ValueError()
        cols = int(parts[0])  # X = horizontal = columns
        rows = int(parts[1])  # Y = vertical = rows
        if rows <= 0 or cols <= 0:
            raise ValueError()
        return (rows, cols)
    except ValueError:
        raise CLIError(
            f"Invalid grid format '{grid_str}'.",
            "Use COLSxROWS with positive integers (e.g., --grid 4x3)."
        )


def load_grid_settings(config_file: Path) -> tuple[int, int]:
    """
    Load grid maximum dimensions from config YAML file.

    Args:
        config_file: Path to config YAML file.

    Returns:
        tuple[int, int]: (max_rows, max_cols) from config, or defaults (4, 6).
    """
    try:
        return CoreConfigService(config_file).load_grid_settings()
    except (ValueError, OSError, yaml.YAMLError) as exc:
        raise CLIError(str(exc)) from exc


def load_colour_mode(config_file: Path, cli_colour_mode: str | None = None) -> str:
    """
    Resolve the colour mode from CLI override or config YAML.

    Priority:
    1. CLI --colour-mode value (if provided)
    2. config.yaml plotting.colour_mode
    3. "y_value" default

    Args:
        config_file: Path to config YAML file.
        cli_colour_mode: Optional CLI override value.

    Returns:
        str: Resolved colour mode ('y_value' or 'year').

    Raises:
        CLIError: If plotting.colour_mode is configured with an invalid value.
    """
    try:
        return CoreConfigService(config_file).load_colour_mode(cli_colour_mode)
    except (ValueError, OSError, yaml.YAMLError) as exc:
        raise CLIError(str(exc)) from exc


def load_colormap(config_file: Path) -> str:
    """
    Resolve plotting colormap from config YAML.

    Args:
        config_file: Path to config YAML file.

    Returns:
        str: Valid configured colormap name.
    """
    try:
        return CoreConfigService(config_file).load_colormap()
    except (ValueError, OSError, yaml.YAMLError) as exc:
        raise CLIError(str(exc)) from exc


def validate_measure_support(measure: str) -> None:
    """Validate that the selected measure is currently implemented."""
    if measure in VALID_MEASURES:
        return

    raise CLIError(
        f"Unknown measure '{measure}'.",
        f"Allowed values: {', '.join(VALID_MEASURES)}"
    )


def validate_measures_support(measures: list[str]) -> None:
    """Validate that all selected measures are currently implemented."""
    if not measures:
        raise CLIError("At least one measure must be selected.")
    for measure in measures:
        validate_measure_support(measure)


def calculate_grid_layout(num_places: int, max_rows: int = 4, max_cols: int = 6) -> tuple[int, int]:
    """
    Calculate optimal grid layout (rows, columns) for subplot arrangement.

    Prioritizes balanced aspect ratio while limiting maximum grid size for readability.
    If places exceed max capacity, they should be batched into multiple images.

    Args:
        num_places: Number of subplots to arrange.
        max_rows: Maximum number of rows allowed (default 4).
        max_cols: Maximum number of columns allowed (default 6).

    Returns:
        tuple[int, int]: (num_rows, num_cols) for the grid layout.

    Examples:
        >>> calculate_grid_layout(1, 4, 6)  # 1×1
        (1, 1)
        >>> calculate_grid_layout(6, 4, 6)  # 2×3
        (2, 3)
        >>> calculate_grid_layout(8, 4, 6)  # 2×4
        (2, 4)
        >>> calculate_grid_layout(10, 4, 6)  # 3×4 (instead of 2×5)
        (3, 4)
        >>> calculate_grid_layout(24, 4, 6)  # 4×6 (max grid size)
        (4, 6)
        >>> calculate_grid_layout(30, 4, 6)  # 4×6 (capped at max, needs batching)
        (4, 6)
    """
    return core_calculate_grid_layout(num_places, max_rows, max_cols)


def build_places_report() -> str:
    """Build a formatted report of available places and place lists."""
    places, default_place, place_lists = load_places()
    lines: list[str] = []

    lines.append("\n=== Available Places ===")
    lines.append(f"Default place: {default_place}\n")
    lines.append(f"Total places: {len(places)}\n")

    for place_name in sorted(places.keys()):
        loc = places[place_name]
        lines.append(f"  • {place_name:30s}  ({loc.lat:7.4f}, {loc.lon:8.4f})  {loc.tz}")

    if place_lists:
        lines.append("\n=== Place Lists ===")
        for list_name, places_in_list in sorted(place_lists.items()):
            lines.append(f"\n  {list_name}:")
            for place in places_in_list:
                lines.append(f"    - {place}")

    lines.append("")
    return "\n".join(lines)


def list_places_and_exit() -> None:
    """
    Print all available places and place lists, then exit.
    """
    print(build_places_report())
    raise SystemExit(0)


def build_cached_years_report(data_cache_dir: Path = Path("data_cache")) -> str:
    """Build a formatted report of cached years by place."""
    from geo_data.cache_store import CacheStore

    places, _default_place, _place_lists = load_places()
    cache_store = CacheStore()
    lines: list[str] = []

    lines.append("\n=== Cached Years by Place ===")
    lines.append(f"Data cache directory: {data_cache_dir}\n")

    places_with_cache = 0
    places_without_cache = 0

    for place_name in sorted(places.keys()):
        yaml_file = cache_store.cache_yaml_path_for_place(data_cache_dir, place_name)
        cached_years = cache_store.get_cached_years(yaml_file)

        if cached_years:
            places_with_cache += 1
            year_list = sorted(cached_years)
            year_str = condense_year_ranges(year_list)
            lines.append(f"  • {place_name:30s}  Years: {year_str}")
        else:
            places_without_cache += 1
            lines.append(f"  • {place_name:30s}  (no cached data)")

    lines.append(f"\n{'='*60}")
    lines.append(f"Total places: {len(places)}")
    lines.append(f"Places with cached data: {places_with_cache}")
    lines.append(f"Places without cached data: {places_without_cache}")
    lines.append(f"{'='*60}\n")

    return "\n".join(lines)


def list_years_and_exit(data_cache_dir: Path = Path("data_cache")) -> None:
    """
    Print all places with their cached years from the data cache directory, then exit.

    Args:
        data_cache_dir: Directory containing cached YAML data files.
    """
    print(build_cached_years_report(data_cache_dir))
    raise SystemExit(0)
