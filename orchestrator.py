"""
Plot orchestration for geo_temp.

Coordinates the creation of main subplots and individual plots,
handling batching, grid layouts, and display logic.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from cds import Location
from cli import calculate_grid_layout, load_grid_settings
from plot import Visualizer

logger = logging.getLogger("geo_temp")


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
    settings: Path,
    t_min_c: float,
    t_max_c: float,
    scale_height: bool
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
        settings: Path to plot settings YAML file.
        t_min_c: Minimum temperature across all data.
        t_max_c: Maximum temperature across all data.
        scale_height: Whether to scale figure height for 3+ rows.
    Returns:
        Path to the saved plot file.
    """
    # Generate filename with batch number if multiple batches
    if num_batches > 1:
        batch_suffix = f"_part{batch_idx + 1}of{num_batches}"
        logger.info(f"Generating batch {batch_idx + 1}/{num_batches}: {len(batch_places)} locations")
    else:
        batch_suffix = ""
    
    vis = Visualizer(df_batch, out_dir=out_dir, t_min_c=t_min_c, t_max_c=t_max_c, settings_file=settings)
    plot_file = out_dir / f"Overall_noon_temps_polar_{start_year}_{end_year}{batch_suffix}.png"
    title = f"Mid-Day Temperatures ({start_year}-{end_year})"
    if num_batches > 1:
        title += f" - Part {batch_idx + 1}/{num_batches}"
    
    credit = "Mid-Day Temperature Analysis & Visualisation by Colin Osborne"
    data_source = "Data from: ERA5 via CDS"
    
    vis.plot_polar_subplots(
        title=title,
        subplot_field="place_name",
        num_rows=batch_rows,
        num_cols=batch_cols,
        credit=credit,
        data_source=data_source,
        save_file=str(plot_file),
        layout="polar_subplot",
        show_plot=False,
        scale_height=scale_height
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
    scale_height: bool
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
        scale_height: Whether to scale figure height for 3+ rows.
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
            settings=settings,
            t_min_c=t_min_c,
            t_max_c=t_max_c,
            scale_height=scale_height
        )
        batch_plot_files.append(plot_file)
    
    return batch_plot_files


def create_individual_plot(
    loc: Location,
    df: pd.DataFrame,
    start_year: int,
    end_year: int,
    out_dir: Path,
    settings: Path,
    t_min_c: float,
    t_max_c: float
) -> str:
    """
    Create a single individual location plot and save to file.
    
    Args:
        loc: Location object.
        df: DataFrame for this location.
        start_year: Start year for plot titles and filenames.
        end_year: End year for plot titles and filenames.
        out_dir: Directory for output plot files.
        settings: Path to plot settings YAML file.
        t_min_c: Minimum temperature across all data.
        t_max_c: Maximum temperature across all data.
    Returns:
        Path to the saved plot file.
    """
    vis = Visualizer(df, out_dir=out_dir, t_min_c=t_min_c, t_max_c=t_max_c, settings_file=settings)
    plot_file = out_dir / f"{loc.name.replace(' ', '_').replace(',', '')}_noon_temps_polar_{start_year}_{end_year}.png"
    title = f"{loc.name} Mid-Day Temperatures ({start_year}-{end_year})"
    credit = "Analysis & visualisation by Colin Osborne"
    data_source = "Data from: ERA5 via CDS"
    
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
    scale_height: bool = True,
    grid: tuple[int, int] | None = None
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
        scale_height: Whether to scale figure height for 3+ rows to prevent overlap.
        grid: Optional fixed grid dimensions (rows, cols). If None, auto-calculate.
    """
    t_min_c = df_overall["temp_C"].min()
    t_max_c = df_overall["temp_C"].max()
    logger.info(f"Overall temperature range across all locations: {t_min_c:.2f} °C to {t_max_c:.2f} °C")
    
    # Create main subplot plots (potentially split into batches)
    batch_plot_files = create_main_plots(
        df_overall=df_overall,
        place_list=place_list,
        start_year=start_year,
        end_year=end_year,
        out_dir=out_dir,        config=config,        settings=settings,
        t_min_c=t_min_c,
        t_max_c=t_max_c,
        grid=grid,
        scale_height=scale_height
    )
    
    # Create individual plots for each location
    individual_plot_files = []
    for loc in place_list:
        df = df_overall[df_overall['place_name'] == loc.name]
        plot_file = create_individual_plot(
            loc=loc,
            df=df,
            start_year=start_year,
            end_year=end_year,
            out_dir=out_dir,
            settings=settings,
            t_min_c=t_min_c,
            t_max_c=t_max_c
        )
        individual_plot_files.append(plot_file)
    
    # Show plots on screen if requested
    # Combine all plots to show them simultaneously
    plots_to_show = []
    if show_main:
        plots_to_show.extend(batch_plot_files)
    if show_individual:
        plots_to_show.extend(individual_plot_files)
    
    if plots_to_show:
        Visualizer.show_saved_plots(plots_to_show)
