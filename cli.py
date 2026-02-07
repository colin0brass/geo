"""
CLI and configuration utilities for geo_temp.

Handles command-line argument parsing and grid layout calculations.
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
from datetime import datetime
from pathlib import Path

import yaml

from cds import Location
from config_manager import load_places

logger = logging.getLogger("geo_temp")

__version__ = "1.0.0"


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for geo_temp.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Generate geographic temperature plots from ERA5 data.",
        epilog="""
Examples:
  %(prog)s --years 2024 --show                    # Default place, show all plots
  %(prog)s --place "Austin, TX" --years 2020-2025  # Specific place
  %(prog)s --lat 30.27 --lon -97.74 --years 2024  # Custom coordinates
  %(prog)s --all --years 2024                     # All configured places
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
        help="Name of a predefined place list (e.g., 'default', 'us_cities'). If no list name provided, uses 'default' list."
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
        "--settings",
        type=Path,
        default=Path("settings.yaml"),
        help="Path to plot settings YAML file (default: settings.yaml)"
    )
    
    # Display options
    display_group = parser.add_argument_group("display options")
    display_group.add_argument(
        "-s", "--show",
        type=str,
        nargs='?',
        const="main",
        choices=["none", "main", "all"],
        default="none",
        help="Show plots interactively: 'none' (default), 'main' (combined plot only), 'all' (all plots). Use -s without argument for 'main'"
    )
    display_group.add_argument(
        "--grid",
        type=str,
        default=None,
        help="Grid dimensions as COLSxROWS (e.g., 4x3 for 4 columns × 3 rows)"
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
        SystemExit: If the year format is invalid.
    """
    if not years_str:
        logger.error("--years argument is required (e.g. --years 2025 or --years 2020-2025)")
        sys.exit(1)
    if '-' in years_str:
        try:
            start, end = years_str.split('-')
            start_year = int(start)
            end_year = int(end)
        except Exception:
            logger.error(f"Invalid --years format: {years_str}. Use YYYY or YYYY-YYYY.")
            sys.exit(1)
    else:
        try:
            year = int(years_str)
            start_year = year
            end_year = year
        except Exception:
            logger.error(f"Invalid --years format: {years_str}. Use YYYY or YYYY-YYYY.")
            sys.exit(1)
    return start_year, end_year


def get_place_list(args: argparse.Namespace, places: dict[str, Location], default_place: str, place_lists: dict[str, list[str]]) -> list[Location]:
    """
    Determine the list of places to process based on command-line arguments.
    
    Args:
        args: Parsed command-line arguments.
        places: Dictionary mapping place names to Location objects.
        default_place: Name of the default place to use when no arguments provided.
        place_lists: Dictionary of predefined place lists.
        
    Returns:
        list[Location]: List of Location objects to process.
        
    Raises:
        SystemExit: If invalid place or place list is specified.
    """
    # --all takes precedence
    if args.all:
        return list(places.values())
    
    # --place-list uses a named list
    if args.place_list:
        if args.place_list not in place_lists:
            logger.error(f"Unknown place list '{args.place_list}'. Available lists: {list(place_lists.keys())}")
            sys.exit(1)
        place_names = place_lists[args.place_list]
        return [places[name] for name in place_names if name in places]
    
    # --place uses a specific place
    if args.place:
        if args.place in places:
            # Use configured place
            return [places[args.place]]
        # Custom place - require coordinates
        if args.lat is None or args.lon is None:
            logger.error(f"Unknown place '{args.place}'. Please provide --lat and --lon for custom locations.")
            sys.exit(1)
        # Create custom location (tz will auto-detect if not provided)
        if args.tz:
            return [Location(name=args.place, lat=args.lat, lon=args.lon, tz=args.tz)]
        else:
            return [Location(name=args.place, lat=args.lat, lon=args.lon)]
    
    # No arguments: use default place
    if default_place in places:
        return [places[default_place]]
    else:
        logger.error(f"Default place '{default_place}' not found in config.yaml")
        sys.exit(1)


def parse_grid(grid_str: str | None) -> tuple[int, int] | None:
    """
    Parse grid dimension string into (rows, cols).
    
    Args:
        grid_str: Grid string in format "COLSxROWS" (e.g., "4x3" = 4 columns, 3 rows) or None.
        
    Returns:
        tuple[int, int] | None: (rows, cols) or None if grid_str is None.
        
    Raises:
        SystemExit: If the grid format is invalid.
    """
    if grid_str is None:
        return None
        
    if 'x' not in grid_str.lower():
        logger.error(f"Invalid grid format '{grid_str}'. Use COLSxROWS (e.g., '4x3' for 4 columns by 3 rows)")
        sys.exit(1)
        
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
        logger.error(f"Invalid grid format '{grid_str}'. Use COLSxROWS (e.g., '4x3' for 4 columns by 3 rows)")
        sys.exit(1)


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
    from data import get_cached_years
    
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
        # Generate expected cache file name
        base_name = f"{place_name.replace(' ', '_').replace(',', '')}_noon_temps"
        yaml_file = data_cache_dir / f"{base_name}.yaml"
        
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
