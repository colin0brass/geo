"""
CLI and configuration utilities for geo_temp.

Handles command-line argument parsing, configuration loading,
and grid layout calculations.
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

logger = logging.getLogger("geo_temp")


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


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for geo_temp.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Generate geographic temperature plots from ERA5 data.")
    parser.add_argument("--place", type=str, default=None, help="Name of the place to retrieve data for.")
    parser.add_argument("--lat", type=float, required=False, help="Latitude of the location.")
    parser.add_argument("--lon", type=float, required=False, help="Longitude of the location.")
    parser.add_argument("--tz", type=str, required=False, help="Timezone of the location.")
    parser.add_argument("--place-list", type=str, default=None, help="Name of a predefined place list (e.g., 'preferred', 'us_cities').")
    parser.add_argument("--all", action="store_true", help="Retrieve data for all locations.")
    parser.add_argument(
        "--years",
        type=str,
        required=False,
        default=str(datetime.now().year - 1),
        help="Year or year range (e.g. 2025 or 2020-2025). Overrides --start-year and --end-year if provided."
    )
    parser.add_argument("--cache-dir", type=Path, default=Path("era5_cache"), help="Cache directory for NetCDF files.")
    parser.add_argument("--out-dir", type=Path, default=Path("output"), help="Output directory for plots.")
    parser.add_argument("--settings", type=Path, default=Path("settings.yaml"), help="Path to plot settings YAML file.")
    parser.add_argument(
        "--show",
        type=str,
        choices=["none", "main", "all"],
        default="none",
        help="Which plots to display: 'none' (default, show no plots), 'main' (show only the main/all subplot), 'all' (show all individual and main plots)"
    )
    parser.add_argument(
        "--scale-height",
        action="store_true",
        default=True,
        help="Scale figure height for 3+ rows (default: enabled). Use --no-scale-height to disable."
    )
    parser.add_argument(
        "--no-scale-height",
        dest="scale_height",
        action="store_false",
        help="Disable figure height scaling for 3+ rows"
    )
    parser.add_argument(
        "--grid",
        type=str,
        default=None,
        help="Specify grid dimensions as COLSxROWS (e.g., '4x3' for 4 columns by 3 rows). If places exceed grid capacity, multiple images will be generated."
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
        if args.place not in places and (args.lat is None or args.lon is None or args.tz is None):
            logger.error(f"Unknown place '{args.place}'. Please provide lat, lon, and tz.")
            sys.exit(1)
        if args.place in places:
            return [places[args.place]]
        return [Location(name=args.place, lat=args.lat, lon=args.lon, tz=args.tz)]
    
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


def calculate_grid_layout(num_places: int, max_cols: int = 4) -> tuple[int, int]:
    """
    Calculate optimal grid layout (rows, columns) for subplot arrangement.
    
    Prioritizes balanced aspect ratio while limiting maximum columns for readability.
    
    Args:
        num_places: Number of subplots to arrange.
        max_cols: Maximum number of columns allowed (default 4).
        
    Returns:
        tuple[int, int]: (num_rows, num_cols) for the grid layout.
        
    Examples:
        >>> calculate_grid_layout(1)  # 1×1
        (1, 1)
        >>> calculate_grid_layout(6)  # 2×3
        (2, 3)
        >>> calculate_grid_layout(8)  # 2×4
        (2, 4)
        >>> calculate_grid_layout(10)  # 3×4 instead of 2×5
        (3, 4)
        >>> calculate_grid_layout(16)  # 4×4 instead of 2×8
        (4, 4)
    """
    if num_places == 0:
        return (1, 1)
    
    # For small numbers, use simple logic
    if num_places <= 2:
        return (1, num_places)
    if num_places <= 4:
        return (2, 2)
    
    # For larger numbers, find best balanced layout within max_cols constraint
    # Start with square-ish layout
    num_cols = min(max_cols, math.ceil(math.sqrt(num_places)))
    num_rows = math.ceil(num_places / num_cols)
    
    # Optimize: try to reduce empty spaces while maintaining good aspect ratio
    # Check if we can reduce columns and still fit everything
    for cols in range(num_cols, 0, -1):
        rows = math.ceil(num_places / cols)
        empty_spaces = (rows * cols) - num_places
        
        # Accept layout if it doesn't waste too many spaces
        # and maintains reasonable aspect ratio (rows ≤ cols + 1)
        if empty_spaces <= cols and rows <= cols + 1:
            return (rows, cols)
    
    return (num_rows, num_cols)
