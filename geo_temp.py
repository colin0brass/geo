"""
geo_temp: ERA5 temperature data analysis and visualization.

Main entry point for the geo_temp application. Orchestrates data retrieval,
processing, and visualization of ERA5 temperature data.
"""

import logging

from cli import parse_args, parse_grid, parse_years, get_place_list, list_places_and_exit, list_years_and_exit
from config_manager import load_places, add_place_to_config
from data import retrieve_and_concat_data
from orchestrator import plot_all
from logging_config import setup_logging, get_logger
from progress import get_progress_manager, ConsoleProgressHandler


def main() -> None:
    """
    Main entry point for geo_temp.
    
    Parses command-line arguments, loads places configuration, retrieves temperature data,
    and generates visualizations according to the specified options.
    """
    args = parse_args()
    
    # Initialize logging
    setup_logging()
    logger = get_logger("geo_temp")  # Use explicit name, not __name__
    
    # Register progress handler for console output
    progress_manager = get_progress_manager()
    progress_manager.register_handler(ConsoleProgressHandler())
    
    # Handle verbose/quiet flags for console output
    geo_logger = logging.getLogger("geo_temp")
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
        return
    
    start_year, end_year = parse_years(args.years)
    grid = parse_grid(args.grid)
    places, default_place, place_lists = load_places()
    place_list = get_place_list(args, places, default_place, place_lists)
    
    # Dry-run mode: show what would be done without executing
    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be downloaded or plots created")
        logger.info(f"Places to process: {[loc.name for loc in place_list]}")
        logger.info(f"Years: {start_year}-{end_year}")
        logger.info(f"Grid: {grid if grid else 'auto'}")
        logger.info(f"Output directory: {args.out_dir}")
        logger.info(f"NetCDF cache directory: {args.cache_dir}")
        logger.info(f"Data cache directory: {args.data_cache_dir}")
        logger.info(f"Show plots: {args.show}")
        return
    
    df_overall = retrieve_and_concat_data(place_list, start_year, end_year, args.cache_dir, args.data_cache_dir)
    show_main = args.show.lower() in ("main", "all")
    show_individual = args.show.lower() == "all"
    plot_all(df_overall, place_list, start_year, end_year, args.out_dir, args.config, args.settings, show_main, show_individual, True, grid)


if __name__ == "__main__":
    main()

