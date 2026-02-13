"""
geo: ERA5 temperature data analysis and visualization.

Main entry point for the geo application. Orchestrates data retrieval,
processing, and visualization of ERA5 temperature data.
"""

import logging
import sys

from cli import parse_args, parse_grid, parse_years, get_place_list, list_places_and_exit, list_years_and_exit, load_colour_mode, load_colormap, validate_measure_support, CLIError
from config_manager import load_places, add_place_to_config
from geo_data.data import retrieve_and_concat_data
from orchestrator import plot_all
from logging_config import setup_logging, get_logger
from progress import get_progress_manager, ConsoleProgressHandler


def main() -> int:
    """
    Main entry point for geo.

    Parses command-line arguments, loads places configuration, retrieves temperature data,
    and generates visualizations according to the specified options.
    """
    try:
        args = parse_args()
    except CLIError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    # Initialize logging
    setup_logging()
    logger = get_logger("geo")  # Use explicit name, not __name__

    # Register progress handler for console output
    progress_manager = get_progress_manager()
    progress_manager.register_handler(ConsoleProgressHandler())

    # Handle verbose/quiet flags for console output
    geo_logger = logging.getLogger("geo")
    if args.verbose:
        # Set console handler to DEBUG level for verbose output
        for handler in geo_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                handler.setLevel(logging.DEBUG)
        logger.debug("Verbose mode enabled (console output at DEBUG level)")
    elif args.quiet:
        # Set console handler to ERROR level for quiet output
        for handler in geo_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                handler.setLevel(logging.ERROR)
    elif args.dry_run:
        # Dry-run needs at least INFO level to show preview output
        for handler in geo_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                current_level = handler.level
                if current_level > logging.INFO:
                    handler.setLevel(logging.INFO)

    # Handle --list-years flag (exits after listing)
    if args.list_years:
        list_years_and_exit(args.data_cache_dir)

    # Handle --list-places flag (exits after listing)
    if args.list_places:
        list_places_and_exit()

    # Handle --add-place flag (exits after adding)
    if args.add_place:
        add_place_to_config(args.add_place)
        return 0

    try:
        start_year, end_year = parse_years(args.years)
        grid = parse_grid(args.grid)
        validate_measure_support(args.measure)
        colour_mode = load_colour_mode(args.config, args.colour_mode)
        colormap_name = load_colormap(args.config)
        places, default_place, place_lists = load_places()
        place_list, list_name = get_place_list(args, places, default_place, place_lists)
    except CLIError as e:
        logger.error(str(e))
        return 2

    # Dry-run mode: show what would be done without executing
    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be downloaded or plots created")
        logger.info(f"Places to process: {[loc.name for loc in place_list]}")
        logger.info(f"Years: {start_year}-{end_year}")
        logger.info(f"Measure: {args.measure}")
        logger.info(f"Grid: {grid if grid else 'auto'}")
        logger.info(f"Colour mode: {colour_mode}")
        logger.info(f"Colormap: {colormap_name}")
        logger.info(f"Output directory: {args.out_dir}")
        logger.info(f"NetCDF cache directory: {args.cache_dir}")
        logger.info(f"Data cache directory: {args.data_cache_dir}")
        logger.info(f"Show plots: {args.show}")
        return 0

    df_overall = retrieve_and_concat_data(
        place_list,
        start_year,
        end_year,
        args.cache_dir,
        args.data_cache_dir,
        measure=args.measure,
    )
    plot_all(
        df_overall,
        place_list,
        start_year,
        end_year,
        args.out_dir,
        args.config,
        args.settings,
        args.show,
        args.show,
        grid,
        list_name,
        args.measure,
        colour_mode,
        colormap_name
    )
    return 0


if __name__ == "__main__":
    exit_code = main()
    in_debugger = (
        sys.gettrace() is not None
        or "debugpy" in sys.modules
        or "pydevd" in sys.modules
    )
    if not in_debugger:
        raise SystemExit(exit_code)
