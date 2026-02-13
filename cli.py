"""
CLI and configuration utilities for geo_temp.

Handles command-line argument parsing and grid layout calculations.
"""

from __future__ import annotations

import argparse
import difflib
import logging
import math
import sys
from datetime import datetime
from pathlib import Path

import yaml
from matplotlib import colormaps as mpl_colormaps

from cds import Location
from config_manager import load_places

logger = logging.getLogger("geo_temp")

__version__ = "1.0.0"
VALID_COLOUR_MODES = ("temperature", "year")
VALID_MEASURES = ("noon_temperature", "daily_precipitation")
DEFAULT_COLOUR_MODE = VALID_COLOUR_MODES[0]
DEFAULT_MEASURE = VALID_MEASURES[0]
DEFAULT_COLORMAP = "turbo"


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


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for geo_temp.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
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
        "-L", "--list",
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
        "-l", "--list-places",
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
        default=Path("era5_cache"),
        help="Cache directory for NetCDF files (default: era5_cache)"
    )
    output_group.add_argument(
        "--data-cache-dir",
        type=Path,
        default=Path("data_cache"),
        help="Cache directory for CSV data files (default: data_cache)"
    )
    output_group.add_argument(
        "--out-dir",
        type=Path,
        default=Path("output"),
        help="Output directory for plots (default: output)"
    )
    output_group.add_argument(
        "--config",
        type=Path,
        default=Path("config.yaml"),
        help="Path to config YAML file (default: config.yaml)"
    )
    output_group.add_argument(
        "--settings",
        type=Path,
        default=Path("settings.yaml"),
        help="Path to plot settings YAML file (default: settings.yaml)"
    )
    output_group.add_argument(
        "--measure",
        choices=VALID_MEASURES,
        default=DEFAULT_MEASURE,
        help="Data measure to use: 'noon_temperature' (implemented) or 'daily_precipitation' (planned)"
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
        help="Colour mapping mode: 'temperature' (default) or 'year' for trend-over-time colouring"
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
    
    return parser.parse_args()


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
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
            grid_config = config.get('grid', {})
            max_rows = grid_config.get('max_auto_rows', 4)
            max_cols = grid_config.get('max_auto_cols', 6)
            return (max_rows, max_cols)
    except Exception as e:
        logger.warning(f"Failed to load grid settings from {config_file}: {e}. Using defaults (4x6).")
        return (4, 6)


def load_colour_mode(config_file: Path, cli_colour_mode: str | None = None) -> str:
    """
    Resolve the colour mode from CLI override or config YAML.

    Priority:
    1. CLI --colour-mode value (if provided)
    2. config.yaml plotting.colour_mode
    3. "temperature" default

    Args:
        config_file: Path to config YAML file.
        cli_colour_mode: Optional CLI override value.

    Returns:
        str: Resolved colour mode ('temperature' or 'year').
    """
    if cli_colour_mode is not None:
        return cli_colour_mode

    default_mode = DEFAULT_COLOUR_MODE
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f) or {}

        config_mode = config.get('plotting', {}).get('colour_mode', default_mode)
        if config_mode not in VALID_COLOUR_MODES:
            logger.warning(
                f"Invalid plotting.colour_mode '{config_mode}' in {config_file}. "
                f"Using default '{default_mode}'."
            )
            return default_mode

        return config_mode
    except Exception as e:
        logger.warning(f"Failed to load colour mode from {config_file}: {e}. Using default '{default_mode}'.")
        return default_mode


def load_colormap(config_file: Path) -> str:
    """
    Resolve plotting colormap from config YAML.

    Args:
        config_file: Path to config YAML file.

    Returns:
        str: Valid configured colormap name.
    """
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
    except Exception as e:
        logger.warning(f"Failed to load colormap from {config_file}: {e}. Using default '{DEFAULT_COLORMAP}'.")
        return DEFAULT_COLORMAP


def validate_measure_support(measure: str) -> None:
    """Validate that the selected measure is currently implemented."""
    if measure == "noon_temperature":
        return

    if measure == "daily_precipitation":
        raise CLIError(
            "Measure 'daily_precipitation' is not implemented yet.",
            "Use --measure noon_temperature for now. Precipitation support will be added next."
        )

    raise CLIError(
        f"Unknown measure '{measure}'.",
        f"Allowed values: {', '.join(VALID_MEASURES)}"
    )


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
    if num_places == 0:
        return (1, 1)
    
    # Cap at maximum grid size
    max_grid_size = max_rows * max_cols
    places_to_fit = min(num_places, max_grid_size)
    
    # For small numbers, use simple logic
    if places_to_fit <= 2:
        return (1, places_to_fit)
    if places_to_fit <= 4:
        return (2, 2)
    
    # For larger numbers, find best balanced layout within constraints
    # Start with square-ish layout
    num_cols = min(max_cols, math.ceil(math.sqrt(places_to_fit)))
    num_rows = math.ceil(places_to_fit / num_cols)
    
    # Cap rows at maximum
    if num_rows > max_rows:
        num_rows = max_rows
        num_cols = min(max_cols, math.ceil(places_to_fit / num_rows))
    
    # Optimize: try to reduce empty spaces while maintaining good aspect ratio
    # Check if we can reduce columns and still fit everything
    for cols in range(num_cols, 0, -1):
        rows = math.ceil(places_to_fit / cols)
        
        # Skip if exceeds row limit
        if rows > max_rows:
            continue
            
        empty_spaces = (rows * cols) - places_to_fit
        
        # Accept layout if it doesn't waste too many spaces
        # and maintains reasonable aspect ratio (rows ≤ cols + 1)
        if empty_spaces <= cols and rows <= cols + 1:
            return (rows, cols)
    
    return (num_rows, num_cols)


def list_places_and_exit() -> None:
    """
    Print all available places and place lists, then exit.
    """
    places, default_place, place_lists = load_places()
    
    print("\n=== Available Places ===")
    print(f"Default place: {default_place}\n")
    print(f"Total places: {len(places)}\n")
    
    # Sort places alphabetically for display
    for place_name in sorted(places.keys()):
        loc = places[place_name]
        print(f"  • {place_name:30s}  ({loc.lat:7.4f}, {loc.lon:8.4f})  {loc.tz}")
    
    if place_lists:
        print("\n=== Place Lists ===")
        for list_name, places_in_list in sorted(place_lists.items()):
            print(f"\n  {list_name}:")
            for place in places_in_list:
                print(f"    - {place}")
    
    print()
    exit(0)


def list_years_and_exit(data_cache_dir: Path = Path("data_cache")) -> None:
    """
    Print all places with their cached years from the data cache directory, then exit.
    
    Args:
        data_cache_dir: Directory containing cached YAML data files.
    """
    from data import cache_yaml_path_for_place, get_cached_years
    
    def condense_year_ranges(years: list[int]) -> str:
        """
        Condense contiguous year ranges into readable format.
        
        Args:
            years: Sorted list of years.
            
        Returns:
            String representation with ranges (e.g., "1990-2000, 2005, 2010-2015").
        """
        if not years:
            return ""
        
        ranges = []
        start = years[0]
        end = years[0]
        
        for i in range(1, len(years)):
            if years[i] == end + 1:
                # Contiguous year
                end = years[i]
            else:
                # Gap found, save previous range
                if start == end:
                    ranges.append(str(start))
                else:
                    ranges.append(f"{start}-{end}")
                start = years[i]
                end = years[i]
        
        # Add the last range
        if start == end:
            ranges.append(str(start))
        else:
            ranges.append(f"{start}-{end}")
        
        return ", ".join(ranges)
    
    places, default_place, place_lists = load_places()
    
    print("\n=== Cached Years by Place ===")
    print(f"Data cache directory: {data_cache_dir}\n")
    
    # Track statistics
    places_with_cache = 0
    places_without_cache = 0
    
    # Sort places alphabetically for display
    for place_name in sorted(places.keys()):
        yaml_file = cache_yaml_path_for_place(data_cache_dir, place_name)
        
        # Check for cached years
        cached_years = get_cached_years(yaml_file)
        
        if cached_years:
            places_with_cache += 1
            year_list = sorted(cached_years)
            year_str = condense_year_ranges(year_list)
            print(f"  • {place_name:30s}  Years: {year_str}")
        else:
            places_without_cache += 1
            print(f"  • {place_name:30s}  (no cached data)")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Total places: {len(places)}")
    print(f"Places with cached data: {places_with_cache}")
    print(f"Places without cached data: {places_without_cache}")
    print(f"{'='*60}\n")
    
    exit(0)
