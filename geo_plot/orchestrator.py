"""
Plot orchestration for geo.

Coordinates the creation of main subplots and individual plots,
handling batching, grid layouts, and display logic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from geo_data.cds_base import Location
from geo_core.config import CoreConfigService, get_plot_text, load_measures_config, load_plot_text_config
from geo_core.grid import calculate_grid_layout
from .visualizer import Visualizer

logger = logging.getLogger("geo")


@dataclass(frozen=True)
class PlotRunContext:
    """Execution context for plot naming and layout."""
    start_year: int
    end_year: int
    out_dir: Path
    t_min_c: float
    t_max_c: float


class PlotOrchestrator:
    """Encapsulate measure-aware plotting configuration and rendering helpers."""

    def __init__(
        self,
        config: Path,
        settings: Path,
        measure: str = "noon_temperature",
        colour_mode: str | None = None,
        colormap_name: str = "turbo",
    ) -> None:
        self.config = config
        self.settings = settings
        self.measure = measure
        self.colormap_name = colormap_name
        self.config_service = CoreConfigService(config)

        self._plot_text_config: dict | None = None
        self.measures = load_measures_config(config)
        self.measure_ctx = self._measure_plot_context(measure, self.measures)
        self.measure_meta = self.measures.get(measure, {})
        self.range_text_template = self.measure_meta['range_text']
        self.y_value_column = self.measure_meta['y_value_column']
        if colour_mode is not None:
            self.colour_mode = str(colour_mode)
        else:
            self.colour_mode = str(self.measure_meta.get('colour_mode', 'y_value'))

    @staticmethod
    def _measure_plot_context(measure: str, measures: dict[str, dict[str, object]]) -> dict[str, str]:
        """Build template context values for plot text based on selected measure."""
        default_label = measure.replace('_', ' ').title()
        measure_metadata = measures.get(measure, {})
        measure_label = measure_metadata.get('label', default_label)
        measure_unit = measure_metadata.get('unit', '')
        return {
            'measure': measure,
            'measure_key': measure,
            'measure_label': measure_label,
            'measure_unit': measure_unit,
            'y_value_label': measure_label,
        }

    @staticmethod
    def _resolve_measure_range(df: pd.DataFrame, measure_meta: dict[str, object], y_value_column: str) -> tuple[float, float]:
        """Resolve y-axis bounds using optional measure overrides and data values."""
        if y_value_column not in df.columns:
            raise KeyError(f"Missing measure value column '{y_value_column}' in overall DataFrame")

        data_min = float(df[y_value_column].min())
        data_max = float(df[y_value_column].max())

        y_min_override = measure_meta.get('y_min')
        y_max_override = measure_meta.get('y_max')

        t_min_c = float(y_min_override) if y_min_override is not None else data_min
        t_max_c = float(y_max_override) if y_max_override is not None else data_max
        if t_min_c > t_max_c:
            raise ValueError("Resolved y-axis range is invalid: min must be <= max")

        return t_min_c, t_max_c

    def _get_plot_text_config(self) -> dict:
        """Lazily load plot text templates only when file naming/text is needed."""
        if self._plot_text_config is None:
            self._plot_text_config = load_plot_text_config(self.config)
        return self._plot_text_config

    @staticmethod
    def _format_year_range(start_year: int, end_year: int) -> str:
        """Return compact year range text (single year when bounds are equal)."""
        return str(start_year) if start_year == end_year else f"{start_year}-{end_year}"

    def _resolve_overall_title(
        self,
        run_ctx: PlotRunContext,
        fallback_title: str,
        batch_idx: int,
        num_batches: int,
    ) -> str:
        """Resolve optional per-measure overall title template for combined plots."""
        configured_title = str(self.measure_meta.get('overall_title', '')).strip()
        if not configured_title:
            return fallback_title

        year_range = self._format_year_range(run_ctx.start_year, run_ctx.end_year)
        title_context = {
            **self.measure_ctx,
            'start_year': run_ctx.start_year,
            'end_year': run_ctx.end_year,
            'year_range': year_range,
            'batch': batch_idx + 1,
            'total_batches': num_batches,
        }

        try:
            return configured_title.format(**title_context)
        except KeyError as exc:
            missing = exc.args[0]
            raise ValueError(
                f"Missing placeholder '{missing}' for plotting.measures.{self.measure}.overall_title"
            ) from exc

    def resolve_measure_range(self, df: pd.DataFrame) -> tuple[float, float]:
        """Resolve y-axis bounds for the configured measure over a DataFrame."""
        return self._resolve_measure_range(df, self.measure_meta, self.y_value_column)

    def calculate_grid_dimensions(self, num_places: int, grid: tuple[int, int] | None) -> tuple[int, int, int]:
        """Calculate grid layout and maximum places per image."""
        if grid:
            num_rows, num_cols = grid
            max_places_per_image = num_rows * num_cols
            logger.info(f"Using fixed grid: {num_rows}×{num_cols} (max {max_places_per_image} places per image)")
        else:
            max_rows, max_cols = self.config_service.load_grid_settings()
            max_places_per_image = max_rows * max_cols

            num_rows, num_cols = calculate_grid_layout(num_places, max_rows, max_cols)

            if num_places > max_places_per_image:
                logger.info(
                    f"Auto-calculated grid: {num_rows}×{num_cols} "
                    f"(max {max_places_per_image} places per image, will batch {num_places} locations)"
                )
            else:
                logger.info(f"Auto-calculated grid: {num_rows}×{num_cols} for {num_places} location(s)")

        return num_rows, num_cols, max_places_per_image

    def _build_visualizer(self, df: pd.DataFrame, run_ctx: PlotRunContext) -> Visualizer:
        """Create a Visualizer with the orchestrator's measure and style config."""
        return Visualizer(
            df,
            out_dir=run_ctx.out_dir,
            t_min_c=run_ctx.t_min_c,
            t_max_c=run_ctx.t_max_c,
            y_step=self.measure_meta.get('y_step'),
            max_y_steps=self.measure_meta.get('max_y_steps'),
            settings_file=self.settings,
            y_value_column=self.y_value_column,
            colour_value_column=self.measure_meta.get('colour_value_column'),
            colourbar_title=self.measure_meta.get('colourbar_title'),
            range_text_template=self.range_text_template,
            range_text_context=self.measure_ctx,
            colour_mode=self.colour_mode,
            colormap_name=self.colormap_name,
            plot_format=str(self.measure_meta.get('plot_format', 'points')),
            wedge_width_scale=float(self.measure_meta.get('wedge_width_scale', 1.0)),
        )

    def create_batch_subplot(
        self,
        df_batch: pd.DataFrame,
        batch_places: list[Location],
        batch_idx: int,
        num_batches: int,
        batch_rows: int,
        batch_cols: int,
        run_ctx: PlotRunContext,
        list_name: str | None = None,
    ) -> str:
        """Create a single batch subplot and return saved file path."""
        filename_prefix = list_name if list_name else "Overall"
        plot_text_config = self._get_plot_text_config()

        run_ctx.out_dir.mkdir(parents=True, exist_ok=True)
        year_range = self._format_year_range(run_ctx.start_year, run_ctx.end_year)

        if num_batches > 1:
            logger.info(f"Generating batch {batch_idx + 1}/{num_batches}: {len(batch_places)} locations")
            title = get_plot_text(
                plot_text_config,
                'overall_title_with_batch',
                start_year=run_ctx.start_year,
                end_year=run_ctx.end_year,
                year_range=year_range,
                batch=batch_idx + 1,
                total_batches=num_batches,
                **self.measure_ctx,
            )
            filename = get_plot_text(
                plot_text_config,
                'subplot_filename_with_batch',
                list_name=filename_prefix,
                start_year=run_ctx.start_year,
                end_year=run_ctx.end_year,
                batch=batch_idx + 1,
                total_batches=num_batches,
                **self.measure_ctx,
            )
        else:
            title = get_plot_text(
                plot_text_config,
                'overall_title',
                start_year=run_ctx.start_year,
                end_year=run_ctx.end_year,
                year_range=year_range,
                **self.measure_ctx,
            )
            filename = get_plot_text(
                plot_text_config,
                'subplot_filename',
                list_name=filename_prefix,
                start_year=run_ctx.start_year,
                end_year=run_ctx.end_year,
                **self.measure_ctx,
            )

        plot_file = run_ctx.out_dir / filename
        credit = get_plot_text(plot_text_config, 'credit', **self.measure_ctx)
        data_source = get_plot_text(plot_text_config, 'data_source', **self.measure_ctx)
        overall_title = self._resolve_overall_title(run_ctx, title, batch_idx, num_batches)
        year_range = self._format_year_range(run_ctx.start_year, run_ctx.end_year)
        subplot_title_template = str(
            plot_text_config.get('subplot_title', '{location} ({year_range})')
        )
        subplot_title_context = {
            **self.measure_ctx,
            'start_year': run_ctx.start_year,
            'end_year': run_ctx.end_year,
            'year_range': year_range,
        }

        vis = self._build_visualizer(df_batch, run_ctx)

        vis.plot_polar_subplots(
            title=overall_title,
            subplot_field="place_name",
            subplot_title_template=subplot_title_template,
            subplot_title_context=subplot_title_context,
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

    def create_individual_plot(
        self,
        loc: Location,
        df: pd.DataFrame,
        run_ctx: PlotRunContext,
    ) -> str:
        """Create a single location plot and return saved file path."""
        plot_text_config = self._get_plot_text_config()
        run_ctx.out_dir.mkdir(parents=True, exist_ok=True)
        year_range = self._format_year_range(run_ctx.start_year, run_ctx.end_year)

        title = get_plot_text(
            plot_text_config,
            'single_plot_title',
            location=loc.name,
            start_year=run_ctx.start_year,
            end_year=run_ctx.end_year,
            year_range=year_range,
            **self.measure_ctx,
        )
        filename = get_plot_text(
            plot_text_config,
            'single_plot_filename',
            location=loc.name,
            start_year=run_ctx.start_year,
            end_year=run_ctx.end_year,
            **self.measure_ctx,
        )
        credit = get_plot_text(plot_text_config, 'single_plot_credit', **self.measure_ctx)
        data_source = get_plot_text(plot_text_config, 'data_source', **self.measure_ctx)

        plot_file = run_ctx.out_dir / filename
        vis = self._build_visualizer(df, run_ctx)

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

    def create_main_plots(
        self,
        df_overall: pd.DataFrame,
        place_list: list[Location],
        run_ctx: PlotRunContext,
        grid: tuple[int, int] | None,
        list_name: str | None = None,
    ) -> list[str]:
        """Create all main subplot plots, split into batches when required."""
        num_places = len(place_list)
        num_rows, num_cols, max_places_per_image = self.calculate_grid_dimensions(num_places, grid)
        num_batches = (num_places + max_places_per_image - 1) // max_places_per_image

        max_rows = max_cols = None
        if not grid:
            max_rows, max_cols = self.config_service.load_grid_settings()

        batch_plot_files = []
        for batch_idx in range(num_batches):
            start_idx = batch_idx * max_places_per_image
            end_idx = min(start_idx + max_places_per_image, num_places)
            batch_places = place_list[start_idx:end_idx]
            batch_size = len(batch_places)

            batch_place_names = [p.name for p in batch_places]
            df_batch = df_overall[df_overall['place_name'].isin(batch_place_names)]

            if grid:
                batch_rows, batch_cols = num_rows, num_cols
            else:
                assert max_rows is not None and max_cols is not None
                batch_rows, batch_cols = calculate_grid_layout(batch_size, max_rows, max_cols)

            plot_file = self.create_batch_subplot(
                df_batch=df_batch,
                batch_places=batch_places,
                batch_idx=batch_idx,
                num_batches=num_batches,
                batch_rows=batch_rows,
                batch_cols=batch_cols,
                run_ctx=run_ctx,
                list_name=list_name,
            )
            batch_plot_files.append(plot_file)

        return batch_plot_files


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
    colour_mode: str | None = None,
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
        colour_mode: Colour mapping mode ('y_value', 'colour_value', or 'year').
        colormap_name: Matplotlib colormap name.
    """
    orchestrator = PlotOrchestrator(
        config=config,
        settings=settings,
        measure=measure,
        colour_mode=colour_mode,
        colormap_name=colormap_name,
    )
    t_min_c, t_max_c = orchestrator.resolve_measure_range(df_overall)
    logger.info(
        f"Overall value range across all locations ({measure}, column={orchestrator.y_value_column}): "
        f"{t_min_c:.2f} to {t_max_c:.2f}"
    )

    num_places = len(place_list)

    # For single place: create individual plot only (combined would be redundant)
    # For multiple places: create combined plot only (too many individual files)
    if num_places == 1:
        logger.info("Creating individual plot for single location")
        loc = place_list[0]
        df = df_overall[df_overall['place_name'] == loc.name]
        run_ctx = PlotRunContext(
            start_year=start_year,
            end_year=end_year,
            out_dir=out_dir,
            t_min_c=t_min_c,
            t_max_c=t_max_c,
        )
        plot_file = orchestrator.create_individual_plot(loc=loc, df=df, run_ctx=run_ctx)

        # Show plot if requested
        if show_main or show_individual:
            Visualizer.show_saved_plots([plot_file])
    else:
        logger.info(f"Creating combined plot for {num_places} locations")
        run_ctx = PlotRunContext(
            start_year=start_year,
            end_year=end_year,
            out_dir=out_dir,
            t_min_c=t_min_c,
            t_max_c=t_max_c,
        )
        batch_plot_files = orchestrator.create_main_plots(
            df_overall=df_overall,
            place_list=place_list,
            run_ctx=run_ctx,
            grid=grid,
            list_name=list_name,
        )

        # Show plots if requested
        if show_main or show_individual:
            Visualizer.show_saved_plots(batch_plot_files)
