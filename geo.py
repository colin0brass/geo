"""
geo: ERA5 temperature data analysis and visualization.

Main entry point for the geo application. Orchestrates data retrieval,
processing, and visualization of ERA5 temperature data.
"""

import logging
import sys
import tempfile
import time
from pathlib import Path

from cli import parse_args, parse_grid, parse_years, get_place_list, list_places_and_exit, list_years_and_exit, load_colour_mode, load_colormap, validate_measure_support, CLIError
from config_manager import load_places, add_place_to_config
from geo_data.data_retrieval import RetrievalCoordinator
from geo_core.progress import get_progress_manager
from geo_plot.orchestrator import plot_all
from logging_config import setup_logging, get_logger
from progress import ConsoleProgressHandler


def _configure_console_logging(args, logger) -> None:
    """Apply console verbosity rules based on CLI flags."""
    geo_logger = logging.getLogger("geo")
    if args.verbose:
        for handler in geo_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                handler.setLevel(logging.DEBUG)
        logger.debug("Verbose mode enabled (console output at DEBUG level)")
    elif args.quiet:
        for handler in geo_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                handler.setLevel(logging.ERROR)
    elif args.dry_run:
        for handler in geo_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                current_level = handler.level
                if current_level > logging.INFO:
                    handler.setLevel(logging.INFO)


def _resolve_run_context(args):
    """Resolve parsed CLI options into validated run-time context values."""
    start_year, end_year = parse_years(args.years)
    grid = parse_grid(args.grid)
    validate_measure_support(args.measure)
    if args.download_by == 'compare' and start_year != end_year:
        raise CLIError(
            "--download-by compare requires exactly one year.",
            "Use --years YYYY when benchmarking month vs year download chunking.",
        )
    colour_mode = load_colour_mode(args.config, args.colour_mode)
    colormap_name = load_colormap(args.config)
    places, default_place, place_lists = load_places()
    place_list, list_name = get_place_list(args, places, default_place, place_lists)
    return {
        'start_year': start_year,
        'end_year': end_year,
        'grid': grid,
        'colour_mode': colour_mode,
        'colormap_name': colormap_name,
        'place_list': place_list,
        'list_name': list_name,
    }


def _handle_dry_run(args, ctx, logger) -> int:
    """Render dry-run summary and exit early."""
    logger.info("DRY RUN MODE - No data will be downloaded or plots created")
    logger.info(f"Places to process: {[loc.name for loc in ctx['place_list']]}")
    logger.info(f"Years: {ctx['start_year']}-{ctx['end_year']}")
    logger.info(f"Measure: {args.measure}")
    logger.info(f"Grid: {ctx['grid'] if ctx['grid'] else 'auto'}")
    logger.info(f"Download strategy: {args.download_by}")
    logger.info(f"Colour mode: {ctx['colour_mode']}")
    logger.info(f"Colormap: {ctx['colormap_name']}")
    logger.info(f"Output directory: {args.out_dir}")
    logger.info(f"NetCDF cache directory: {args.cache_dir}")
    logger.info(f"Data cache directory: {args.data_cache_dir}")
    logger.info(f"Show plots: {args.show}")
    return 0


def _run_compare_benchmark(args, ctx, logger) -> int:
    """Benchmark month vs year download chunking for one selected year."""
    compare_year = ctx['start_year']
    benchmark_results = {}
    for mode in ('month', 'year'):
        with tempfile.TemporaryDirectory(prefix=f"geo_compare_{mode}_") as temp_root:
            temp_root_path = Path(temp_root)
            retrieval = RetrievalCoordinator(
                cache_dir=temp_root_path / 'era5_cache',
                data_cache_dir=temp_root_path / 'data_cache',
                config_path=args.config,
                fetch_mode_override=mode,
                status_reporter=None,
            )
            logger.info(
                "Benchmarking %s chunking for %s (%d place(s), %d)...",
                mode,
                args.measure,
                len(ctx['place_list']),
                compare_year,
            )
            start_time = time.perf_counter()
            retrieval.retrieve(
                ctx['place_list'],
                compare_year,
                compare_year,
                measure=args.measure,
            )
            elapsed = time.perf_counter() - start_time
            benchmark_results[mode] = elapsed

    month_seconds = benchmark_results['month']
    year_seconds = benchmark_results['year']
    faster_mode = 'month' if month_seconds < year_seconds else 'year'
    speedup = max(month_seconds, year_seconds) / max(min(month_seconds, year_seconds), 1e-9)
    logger.info("\nDownload benchmark for %s in %d", args.measure, compare_year)
    logger.info("- month chunking: %.2fs", month_seconds)
    logger.info("- year chunking:  %.2fs", year_seconds)
    logger.info("=> %s is faster (%.2fx)", faster_mode, speedup)
    return 0


def _run_standard_pipeline(args, ctx) -> int:
    """Run retrieval and plotting pipeline for non-compare execution paths."""
    fetch_mode_override = None if args.download_by == 'config' else args.download_by
    retrieval = RetrievalCoordinator(
        cache_dir=args.cache_dir,
        data_cache_dir=args.data_cache_dir,
        config_path=args.config,
        fetch_mode_override=fetch_mode_override,
    )
    df_overall = retrieval.retrieve(
        ctx['place_list'],
        ctx['start_year'],
        ctx['end_year'],
        measure=args.measure,
    )
    plot_all(
        df_overall,
        ctx['place_list'],
        ctx['start_year'],
        ctx['end_year'],
        args.out_dir,
        args.config,
        args.settings,
        args.show,
        args.show,
        ctx['grid'],
        ctx['list_name'],
        args.measure,
        ctx['colour_mode'],
        ctx['colormap_name']
    )
    return 0


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
    _configure_console_logging(args, logger)

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
        context = _resolve_run_context(args)
    except CLIError as e:
        logger.error(str(e))
        return 2

    # Dry-run mode: show what would be done without executing
    if args.dry_run:
        return _handle_dry_run(args, context, logger)

    if args.download_by == 'compare':
        return _run_compare_benchmark(args, context, logger)

    return _run_standard_pipeline(args, context)


if __name__ == "__main__":
    exit_code = main()
    in_debugger = (
        sys.gettrace() is not None
        or "debugpy" in sys.modules
        or "pydevd" in sys.modules
    )
    if not in_debugger:
        raise SystemExit(exit_code)
