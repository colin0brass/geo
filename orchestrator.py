"""
Plot orchestration for geo.

Coordinates the creation of main subplots and individual plots,
handling batching, grid layouts, and display logic.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from cds import Location
from cli import calculate_grid_layout, load_grid_settings
from config_manager import get_plot_text, load_measure_labels_config, load_plot_text_config
from plot import Visualizer

logger = logging.getLogger("geo")


def _measure_plot_context(measure: str, measure_labels: dict[str, dict[str, str]]) -> dict[str, str]:
    """Build template context values for plot text based on selected measure."""
    default_label = measure.replace('_', ' ').title()
    measure_metadata = measure_labels.get(measure, {})
    measure_label = measure_metadata.get('label', default_label)
    measure_unit = measure_metadata.get('unit', '')
    return {
        'measure': measure,
        'measure_key': measure,
        'measure_label': measure_label,
        'measure_unit': measure_unit,
        'y_value_label': measure_label,
    }


def calculate_grid_dimensions(num_places: int, grid: tuple[int, int] | None, config: Path) -> tuple[int, int, int]:
    """
    Calculate grid layout and maximum places per image.

    Args:
        num_places: Total number of places to plot.
        grid: Optional fixed grid dimensions (rows, cols).
        config: Path to config file for loading grid configuration.
    Returns:
        Tuple of (num_rows, num_cols, max_places_per_image).
    """
    if grid:
        num_rows, num_cols = grid
        max_places_per_image = num_rows * num_cols
        logger.info(f"Using fixed grid: {num_rows}×{num_cols} (max {max_places_per_image} places per image)")
    else:
        # Load grid settings from config YAML
        max_rows, max_cols = load_grid_settings(config)
        max_places_per_image = max_rows * max_cols

        # Calculate grid, capping at maximum size
        num_rows, num_cols = calculate_grid_layout(num_places, max_rows, max_cols)

        if num_places > max_places_per_image:
            logger.info(f"Auto-calculated grid: {num_rows}×{num_cols} (max {max_places_per_image} places per image, will batch {num_places} locations)")
        else:
            logger.info(f"Auto-calculated grid: {num_rows}×{num_cols} for {num_places} location(s)")

    return num_rows, num_cols, max_places_per_image


def create_batch_subplot(
    df_batch: pd.DataFrame,
    batch_places: list[Location],
    batch_idx: int,
    num_batches: int,
    batch_rows: int,
    batch_cols: int,
    start_year: int,
    end_year: int,
    out_dir: Path,
    config: Path,
    settings: Path,
    t_min_c: float,
    t_max_c: float,
    list_name: str | None = None,
    measure: str = "noon_temperature",
    colour_mode: str = "y_value",
    colormap_name: str = "turbo"
) -> str:
    """
    Create a single batch subplot and save to file.

    Args:
        df_batch: DataFrame for this batch.
        batch_places: List of Location objects in this batch.
        batch_idx: Current batch index (0-based).
        num_batches: Total number of batches.
        batch_rows: Number of rows in this batch's grid.
        batch_cols: Number of columns in this batch's grid.
        start_year: Start year for plot titles and filenames.
        end_year: End year for plot titles and filenames.
        out_dir: Directory for output plot files.
        config: Path to config YAML file (for text patterns).
        settings: Path to plot settings YAML file.
        t_min_c: Minimum temperature across all data.
        t_max_c: Maximum temperature across all data.
        list_name: Name of place list for filename (e.g., "all", "arctic"), None for default.
        measure: Data measure key (e.g., "noon_temperature", "daily_precipitation").
        colour_mode: Colour mapping mode ('y_value' or 'year').
        colormap_name: Matplotlib colormap name.
    Returns:
        Path to the saved plot file.
    """
    # Load plot text configuration
    plot_text_config = load_plot_text_config(config)
    measure_labels = load_measure_labels_config(config)

    # Use list name in filename if provided, otherwise use "Overall"
    filename_prefix = list_name if list_name else "Overall"
    measure_ctx = _measure_plot_context(measure, measure_labels)
    measure_meta = measure_labels.get(measure, {})
    range_text_template = measure_meta.get('range_text') or plot_text_config.get('range_text')
    y_value_column = measure_meta.get('y_value_column', 'temp_C')

    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)

    # Generate title and filename using configuration
    if num_batches > 1:
        logger.info(f"Generating batch {batch_idx + 1}/{num_batches}: {len(batch_places)} locations")
        title = get_plot_text(
            plot_text_config,
            'subplot_title_with_batch',
            start_year=start_year,
            end_year=end_year,
            batch=batch_idx + 1,
            total_batches=num_batches,
            **measure_ctx,
        )
        filename = get_plot_text(
            plot_text_config,
            'subplot_filename_with_batch',
            list_name=filename_prefix,
            start_year=start_year,
            end_year=end_year,
            batch=batch_idx + 1,
            total_batches=num_batches,
            **measure_ctx,
        )
    else:
        title = get_plot_text(
            plot_text_config,
            'subplot_title',
            start_year=start_year,
            end_year=end_year,
            **measure_ctx,
        )
        filename = get_plot_text(
            plot_text_config,
            'subplot_filename',
            list_name=filename_prefix,
            start_year=start_year,
            end_year=end_year,
            **measure_ctx,
        )

    plot_file = out_dir / filename
    credit = get_plot_text(plot_text_config, 'credit', **measure_ctx)
    data_source = get_plot_text(plot_text_config, 'data_source', **measure_ctx)

    vis = Visualizer(
        df_batch,
        out_dir=out_dir,
        t_min_c=t_min_c,
        t_max_c=t_max_c,
        settings_file=settings,
        y_value_column=y_value_column,
        range_text_template=range_text_template,
        range_text_context=measure_ctx,
        colour_mode=colour_mode,
        colormap_name=colormap_name
    )

    vis.plot_polar_subplots(
        title=title,
        subplot_field="place_name",
        num_rows=batch_rows,
        num_cols=batch_cols,
        credit=credit,
        data_source=data_source,
        save_file=str(plot_file),
        layout="polar_subplot",
        show_plot=False
    )

    logger.info(f"Saved overall plot to {plot_file}")
    return str(plot_file)


def create_main_plots(
    df_overall: pd.DataFrame,
    place_list: list[Location],
    start_year: int,
    end_year: int,
    out_dir: Path,
    config: Path,
    settings: Path,
    t_min_c: float,
    t_max_c: float,
    grid: tuple[int, int] | None,
    list_name: str | None = None,
    measure: str = "noon_temperature",
    colour_mode: str = "y_value",
    colormap_name: str = "turbo"
) -> list[str]:
    """
    Create all main subplot plots, potentially split into batches.

    Args:
        df_overall: DataFrame containing temperature data for all places.
        place_list: List of Location objects that were processed.
        start_year: Start year for plot titles and filenames.
        end_year: End year for plot titles and filenames.
        out_dir: Directory for output plot files.
        config: Path to config YAML file (for grid settings).
        settings: Path to plot settings YAML file (for styling).
        t_min_c: Minimum temperature across all data.
        t_max_c: Maximum temperature across all data.
        grid: Optional fixed grid dimensions (rows, cols).
        list_name: Name of place list for filename (e.g., "all", "arctic"), None for default.
        measure: Data measure key (e.g., "noon_temperature", "daily_precipitation").
        colour_mode: Colour mapping mode ('y_value' or 'year').
        colormap_name: Matplotlib colormap name.
    Returns:
        List of paths to saved plot files.
    """
    num_places = len(place_list)
    num_rows, num_cols, max_places_per_image = calculate_grid_dimensions(num_places, grid, config)
    num_batches = (num_places + max_places_per_image - 1) // max_places_per_image

    batch_plot_files = []
    for batch_idx in range(num_batches):
        start_idx = batch_idx * max_places_per_image
        end_idx = min(start_idx + max_places_per_image, num_places)
        batch_places = place_list[start_idx:end_idx]
        batch_size = len(batch_places)

        # Filter dataframe for this batch
        batch_place_names = [p.name for p in batch_places]
        df_batch = df_overall[df_overall['place_name'].isin(batch_place_names)]

        # Recalculate grid for this batch if not using fixed grid
        if grid:
            batch_rows, batch_cols = num_rows, num_cols
        else:
            max_rows, max_cols = load_grid_settings(config)
            batch_rows, batch_cols = calculate_grid_layout(batch_size, max_rows, max_cols)

        plot_file = create_batch_subplot(
            df_batch=df_batch,
            batch_places=batch_places,
            batch_idx=batch_idx,
            num_batches=num_batches,
            batch_rows=batch_rows,
            batch_cols=batch_cols,
            start_year=start_year,
            end_year=end_year,
            out_dir=out_dir,
            config=config,
            settings=settings,
            t_min_c=t_min_c,
            t_max_c=t_max_c,
            list_name=list_name,
            measure=measure,
            colour_mode=colour_mode,
            colormap_name=colormap_name
        )
        batch_plot_files.append(plot_file)

    return batch_plot_files


def create_individual_plot(
    loc: Location,
    df: pd.DataFrame,
    start_year: int,
    end_year: int,
    out_dir: Path,
    config: Path,
    settings: Path,
    t_min_c: float,
    t_max_c: float,
    measure: str = "noon_temperature",
    colour_mode: str = "y_value",
    colormap_name: str = "turbo"
) -> str:
    """
    Create a single individual location plot and save to file.

    Args:
        loc: Location object.
        df: DataFrame for this location.
        start_year: Start year for plot titles and filenames.
        end_year: End year for plot titles and filenames.
        out_dir: Directory for output plot files.
        config: Path to config YAML file (for text patterns).
        settings: Path to plot settings YAML file.
        t_min_c: Minimum temperature across all data.
        t_max_c: Maximum temperature across all data.
        measure: Data measure key (e.g., "noon_temperature", "daily_precipitation").
        colour_mode: Colour mapping mode ('y_value' or 'year').
        colormap_name: Matplotlib colormap name.
    Returns:
        Path to the saved plot file.
    """
    # Load plot text configuration
    plot_text_config = load_plot_text_config(config)
    measure_labels = load_measure_labels_config(config)
    measure_ctx = _measure_plot_context(measure, measure_labels)
    measure_meta = measure_labels.get(measure, {})
    range_text_template = measure_meta.get('range_text') or plot_text_config.get('range_text')
    y_value_column = measure_meta.get('y_value_column', 'temp_C')

    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)

    # Generate title and filename using configuration
    title = get_plot_text(
        plot_text_config,
        'single_plot_title',
        location=loc.name,
        start_year=start_year,
        end_year=end_year,
        **measure_ctx,
    )
    filename = get_plot_text(
        plot_text_config,
        'single_plot_filename',
        location=loc.name,
        start_year=start_year,
        end_year=end_year,
        **measure_ctx,
    )
    credit = get_plot_text(plot_text_config, 'single_plot_credit', **measure_ctx)
    data_source = get_plot_text(plot_text_config, 'data_source', **measure_ctx)

    plot_file = out_dir / filename
    vis = Visualizer(
        df,
        out_dir=out_dir,
        t_min_c=t_min_c,
        t_max_c=t_max_c,
        settings_file=settings,
        y_value_column=y_value_column,
        range_text_template=range_text_template,
        range_text_context=measure_ctx,
        colour_mode=colour_mode,
        colormap_name=colormap_name
    )

    vis.plot_polar(
        title=title,
        credit=credit,
        data_source=data_source,
        save_file=str(plot_file),
        layout="polar_single",
        show_plot=False
    )
    logger.info(f"Saved plot to {plot_file}")
    return str(plot_file)


def plot_all(
    df_overall: pd.DataFrame,
    place_list: list[Location],
    start_year: int,
    end_year: int,
    out_dir: Path,
    config: Path,
    settings: Path,
    show_main: bool,
    show_individual: bool,
    grid: tuple[int, int] | None = None,
    list_name: str | None = None,
    measure: str = "noon_temperature",
    colour_mode: str = "y_value",
    colormap_name: str = "turbo"
) -> None:
    """
    Generate all plots (overall subplot and individual plots) for the temperature data.

    Args:
        df_overall: DataFrame containing temperature data for all places.
        place_list: List of Location objects that were processed.
        start_year: Start year for plot titles and filenames.
        end_year: End year for plot titles and filenames.
        out_dir: Directory for output plot files.
        config: Path to config YAML file (for grid settings).
        settings: Path to plot settings YAML file (for styling).
        show_main: Whether to display the main subplot on screen.
        show_individual: Whether to display individual plots on screen.
        grid: Optional fixed grid dimensions (rows, cols). If None, auto-calculate.
        list_name: Name of place list (e.g., "all", "arctic"). Not currently used.
        measure: Data measure key (e.g., "noon_temperature", "daily_precipitation").
        colour_mode: Colour mapping mode ('y_value' or 'year').
        colormap_name: Matplotlib colormap name.
    """
    t_min_c = df_overall["temp_C"].min()
    t_max_c = df_overall["temp_C"].max()
    logger.info(f"Overall temperature range across all locations: {t_min_c:.2f} °C to {t_max_c:.2f} °C")

    num_places = len(place_list)

    # For single place: create individual plot only (combined would be redundant)
    # For multiple places: create combined plot only (too many individual files)
    if num_places == 1:
        logger.info("Creating individual plot for single location")
        loc = place_list[0]
        df = df_overall[df_overall['place_name'] == loc.name]
        plot_file = create_individual_plot(
            loc=loc,
            df=df,
            start_year=start_year,
            end_year=end_year,
            out_dir=out_dir,
            config=config,
            settings=settings,
            t_min_c=t_min_c,
            t_max_c=t_max_c,
            measure=measure,
            colour_mode=colour_mode,
            colormap_name=colormap_name
        )

        # Show plot if requested
        if show_main or show_individual:
            Visualizer.show_saved_plots([plot_file])
    else:
        logger.info(f"Creating combined plot for {num_places} locations")
        # Create main subplot plots (potentially split into batches)
        batch_plot_files = create_main_plots(
            df_overall=df_overall,
            place_list=place_list,
            start_year=start_year,
            end_year=end_year,
            out_dir=out_dir,
            config=config,
            settings=settings,
            t_min_c=t_min_c,
            t_max_c=t_max_c,
            grid=grid,
            list_name=list_name,
            measure=measure,
            colour_mode=colour_mode,
            colormap_name=colormap_name
        )

        # Show plots if requested
        if show_main or show_individual:
            Visualizer.show_saved_plots(batch_plot_files)
