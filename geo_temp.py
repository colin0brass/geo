"""
geo_temp: ERA5 temperature data analysis and visualization.

Main entry point for the geo_temp application. Orchestrates data retrieval,
processing, and visualization of ERA5 temperature data.
"""

import logging

from cli import load_places, parse_args, parse_grid, parse_years, get_place_list, list_places_and_exit
from data import retrieve_and_concat_data
from orchestrator import plot_all
from logging_config import setup_logging, get_logger

logger = get_logger(__name__)


def main() -> None:
    """
    Main entry point for geo_temp.
    
    Parses command-line arguments, loads places configuration, retrieves temperature data,
    and generates visualizations according to the specified options.
    """
    args = parse_args()
    
    # Initialize logging with appropriate level based on flags
    setup_logging()
    
    # Handle verbose/quiet flags
    if args.verbose:
        logging.getLogger("geo_temp").setLevel(logging.DEBUG)
        logger.debug("Verbose mode enabled")
    elif args.quiet:
        logging.getLogger("geo_temp").setLevel(logging.ERROR)
    
    # Handle --list-places flag (exits after listing)
    if args.list_places:
        list_places_and_exit()
    
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
        logger.info(f"Cache directory: {args.cache_dir}")
        logger.info(f"Show plots: {args.show}")
        return
    
    df_overall = retrieve_and_concat_data(place_list, start_year, end_year, args.cache_dir, args.out_dir)
    show_main = args.show.lower() in ("main", "all")
    show_individual = args.show.lower() == "all"
    plot_all(df_overall, place_list, start_year, end_year, args.out_dir, args.settings, show_main, show_individual, True, grid)


if __name__ == "__main__":
    main()

