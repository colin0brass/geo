"""
geo_temp: ERA5 temperature data analysis and visualization.

Main entry point for the geo_temp application. Orchestrates data retrieval,
processing, and visualization of ERA5 temperature data.
"""

from cli import load_places, parse_args, parse_grid, parse_years, get_place_list
from data import retrieve_and_concat_data
from orchestrator import plot_all
from logging_config import setup_logging


def main() -> None:
    """
    Main entry point for geo_temp.
    
    Parses command-line arguments, loads places configuration, retrieves temperature data,
    and generates visualizations according to the specified options.
    """
    # Initialize logging
    setup_logging()
    
    args = parse_args()
    start_year, end_year = parse_years(args.years)
    grid = parse_grid(args.grid)
    places, default_place, place_lists = load_places()
    place_list = get_place_list(args, places, default_place, place_lists)
    df_overall = retrieve_and_concat_data(place_list, start_year, end_year, args.cache_dir, args.out_dir)
    show_main = args.show.lower() in ("main", "all")
    show_individual = args.show.lower() == "all"
    plot_all(df_overall, place_list, start_year, end_year, args.out_dir, args.settings, show_main, show_individual, args.scale_height, grid)


if __name__ == "__main__":
    main()

