import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yaml
import logging
from matplotlib import cm
from matplotlib.colors import Normalize
from .settings_manager import SettingsManager

logger = logging.getLogger("geo")


class Visualizer:
    """
    Visualizer for creating polar temperature plots and subplots from temperature data.
    Handles loading of settings, data preparation, and all plotting routines.
    """

    PAGE_A3_WIDTH_CM = 33.87  # A3 width in cm
    PAGE_A3_HEIGHT_CM = 19.05  # A3 height in cm
    CM_PER_INCH = 2.54
    DEFAULT_Y_STEP = 10.0
    MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    def __init__(
        self,
        df: pd.DataFrame,
        t_min_c: float = None,
        t_max_c: float = None,
        y_step: float | None = None,
        max_y_steps: int | None = None,
        out_dir: str = 'output',
        settings_file: str = 'geo_plot/settings.yaml',
        y_value_column: str = 'temp_C',
        colour_value_column: str | None = None,
        colourbar_title: str | None = None,
        range_text_template: str | None = None,
        range_text_context: dict[str, str] | None = None,
        colour_mode: str = 'y_value',
        colormap_name: str = 'turbo',
        plot_format: str = 'points',
        wedge_width_scale: float = 1.0,
    ) -> None:
        """
        Initialize the Visualizer with data and plotting settings.

        Args:
            df: DataFrame containing temperature and date columns (must include 'date' and 'temp_C').
            t_min_c: Minimum temperature for color normalization (optional).
            t_max_c: Maximum temperature for color normalization (optional).
            y_step: Optional explicit y-axis step override.
            out_dir: Output directory for plots.
            settings_file: Path to YAML settings file.
            y_value_column: DataFrame column used for y-value range text.
            range_text_template: Optional template used for the per-plot value range text.
            range_text_context: Optional context values used with range_text_template.
            colour_mode: Colour mapping mode ('y_value', 'colour_value', or 'year').
            colormap_name: Matplotlib colormap name for point colouring.
        Raises:
            ValueError: If the DataFrame is empty or None.
        """

        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None.")

        self.df = df
        self.out_dir = out_dir
        self.y_step = float(y_step) if y_step is not None else None
        if self.y_step is not None and self.y_step <= 0:
            raise ValueError("y_step must be > 0")
        self.max_y_steps = int(max_y_steps) if max_y_steps is not None else None
        if self.max_y_steps is not None and self.max_y_steps <= 0:
            raise ValueError("max_y_steps must be > 0")
        self.y_value_column = y_value_column
        self.colour_value_column = colour_value_column or y_value_column
        self.colourbar_title = colourbar_title
        self.measure_unit = str((range_text_context or {}).get('measure_unit', '')).strip()
        if range_text_template is None:
            self.range_text_template = "{measure_label}: {min_value:.1f} to {max_value:.1f} {measure_unit}"
        elif not isinstance(range_text_template, str) or not range_text_template.strip():
            raise ValueError("range_text_template must be a non-empty string")
        else:
            self.range_text_template = range_text_template
        self.range_text_context = range_text_context or {}

        self.settings_file = settings_file
        try:
            self.all_settings = self.load_settings_from_yaml(settings_file)
            self.layouts = list(self.all_settings.keys())
            self.layout = self.layouts[0]  # Default to the first layout
        except Exception as e:
            logger.error(f"Error loading settings from YAML file: {e}")
            self.all_settings = {}

        self.df = self.add_data_fields(df)
        if self.y_value_column not in self.df.columns:
            raise KeyError(f"Missing y_value_column '{self.y_value_column}' in DataFrame")
        if self.colour_value_column not in self.df.columns:
            raise KeyError(f"Missing colour_value_column '{self.colour_value_column}' in DataFrame")

        valid_modes = {"y_value", "colour_value", "year"}
        if colour_mode not in valid_modes:
            raise ValueError(f"Invalid colour_mode '{colour_mode}'. Expected one of {sorted(valid_modes)}")
        self.colour_mode = colour_mode
        if self.colour_mode == 'y_value':
            self.colour_source_column = self.y_value_column
        else:
            self.colour_source_column = self.colour_value_column

        normalized_plot_format = 'wedges' if plot_format == 'radial_bars' else plot_format
        valid_plot_formats = {"points", "radial_bars", "wedges"}
        if normalized_plot_format not in valid_plot_formats:
            raise ValueError(
                f"Invalid plot_format '{plot_format}'. Expected one of {sorted(valid_plot_formats)}"
            )
        self.plot_format = normalized_plot_format

        self.wedge_width_scale = float(wedge_width_scale)
        if self.wedge_width_scale <= 0:
            raise ValueError("wedge_width_scale must be > 0")

        self.first_year = pd.to_datetime(self.df['date'].min()).year
        self.last_year = pd.to_datetime(self.df['date'].max()).year

        self.tmin_c = t_min_c if t_min_c is not None else np.min(self.df[self.y_value_column])
        self.tmax_c = t_max_c if t_max_c is not None else np.max(self.df[self.y_value_column])
        self.colour_min = float(np.min(self.df[self.colour_source_column]))
        self.colour_max = float(np.max(self.df[self.colour_source_column]))
        try:
            self.cmap = plt.get_cmap(colormap_name)
        except Exception as e:
            raise ValueError(f"Unknown colormap '{colormap_name}': {e}") from e
        self.colormap_name = colormap_name
        self.norm = Normalize(vmin=self.colour_min, vmax=self.colour_max)
        if self.first_year == self.last_year:
            self.year_norm = Normalize(vmin=self.first_year - 0.5, vmax=self.first_year + 0.5)
        else:
            self.year_norm = Normalize(vmin=self.first_year, vmax=self.last_year)
        self.year_cmap = self.cmap

    def _get_range_bounds(self, df: pd.DataFrame) -> tuple[float, float]:
        """Return min/max values for configured y-value column."""
        if self.y_value_column not in df.columns:
            raise KeyError(f"Missing y_value_column '{self.y_value_column}' in DataFrame")
        range_column = self.y_value_column
        min_value = float(df[range_column].min())
        max_value = float(df[range_column].max())
        return min_value, max_value

    def _format_range_text(self, min_value: float, max_value: float, df: pd.DataFrame | None = None) -> str:
        """Format value-range text using configured template/context."""
        measure_unit = self.range_text_context.get('measure_unit', '')
        if df is not None and 'precip_mm' in df.columns:
            max_daily_precip_mm = float(df['precip_mm'].max())
        else:
            max_daily_precip_mm = max_value
        max_daily_precip_in = self.mm_to_inches(max_daily_precip_mm)
        context = {
            'min_value': min_value,
            'max_value': max_value,
            'min_y_value': min_value,
            'max_y_value': max_value,
            'measure': self.range_text_context.get('measure', ''),
            'measure_key': self.range_text_context.get('measure_key', ''),
            'measure_label': self.range_text_context.get('measure_label', ''),
            'measure_unit': measure_unit,
            'y_value_label': self.range_text_context.get('y_value_label', ''),
            'min_temp_c': min_value,
            'max_temp_c': max_value,
            'min_temp_f': self.temp_c_to_f(min_value),
            'max_temp_f': self.temp_c_to_f(max_value),
            'max_daily_precip_mm': max_daily_precip_mm,
            'max_daily_precip_in': max_daily_precip_in,
        }
        try:
            return self.range_text_template.format(**context)
        except KeyError as exc:
            raise ValueError(f"Missing placeholder context for range_text_template: {exc}") from exc

    @classmethod
    def load_settings_from_yaml(cls, yaml_path: str) -> dict:
        """
        Load settings from a YAML file and return as a dictionary.

        Args:
            yaml_path: Path to the YAML settings file.
        Returns:
            dict: Settings loaded from YAML.
        """
        with open(yaml_path, 'r') as f:
            settings = yaml.safe_load(f)
        return settings or {}

    def add_data_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the DataFrame by ensuring required columns are present.
        Adds 'day_of_year' and 'angle' columns if missing.

        Args:
            df: Input DataFrame.
        Returns:
            DataFrame with necessary columns.
        """
        if 'day_of_year' not in df.columns or 'angle' not in df.columns:
            df['day_of_year'] = pd.to_datetime(df['date']).dt.dayofyear
            df['angle'] = 2 * np.pi * (df['day_of_year'] - 1) / 365.0
        return df

    @staticmethod
    def temp_c_to_f(temp_c: float) -> float:
        """
        Convert Celsius to Fahrenheit.

        Args:
            temp_c: Temperature in Celsius.
        Returns:
            Temperature in Fahrenheit.
        """
        return temp_c * 9.0 / 5.0 + 32.0

    @staticmethod
    def mm_to_inches(mm_value: float) -> float:
        """Convert millimeters to inches."""
        return mm_value / 25.4

    def _default_metric_colourbar_title(self) -> str:
        """Resolve default metric colourbar title for non-year colour modes."""
        if self.colourbar_title:
            return self.colourbar_title
        if self.colour_source_column != self.y_value_column:
            return self.colour_source_column
        return self.measure_unit if self.measure_unit else self.y_value_column

    def _is_precipitation_colour_scale(self) -> bool:
        """Detect whether the active colour scale represents precipitation values."""
        source = self.colour_source_column.lower()
        title = (self.colourbar_title or '').lower()
        return (
            'precip' in source
            or 'rain' in source
            or 'precip' in title
            or 'rain' in title
        )

    @staticmethod
    def _imperial_precip_title(metric_title: str) -> str:
        """Map metric precipitation colourbar titles to imperial equivalents."""
        lowered = metric_title.strip().lower()
        if lowered == 'mm/hr' or lowered == 'mm/h':
            return 'in/hr'
        if lowered == 'mm/day' or lowered == 'mm/d':
            return 'in/day'
        if lowered == 'mm':
            return 'in'
        return 'in'

    @staticmethod
    def show_saved_plots(plot_files: list[str]) -> None:
        """
        Open saved plot images with the system's default image viewer.

        Args:
            plot_files: List of file paths to saved plot images.
        """
        import subprocess
        import sys

        if not plot_files:
            return

        if len(plot_files) > 1:
            logger.info(f"Opening {len(plot_files)} plots...")
        else:
            logger.info("Opening plot...")

        for plot_file in plot_files:
            try:
                if sys.platform == 'darwin':  # macOS
                    subprocess.run(['open', plot_file], check=True)
                elif sys.platform == 'win32':  # Windows
                    subprocess.run(['start', '', plot_file], shell=True, check=True)
                else:  # Linux and other Unix-like
                    subprocess.run(['xdg-open', plot_file], check=True)
            except subprocess.CalledProcessError as e:
                logger.warning(f"Failed to open {plot_file}: {e}")
            except FileNotFoundError:
                logger.warning(f"Could not find system image viewer for {plot_file}")

    def add_dual_colourbars(self, fig: plt.Figure) -> None:
        """
        Add Celsius and Fahrenheit colorbars to a figure with improved sizing and font.

        Args:
            fig: Matplotlib Figure object to which colorbars are added.
        """
        mgr = SettingsManager(self.all_settings[self.layout], num_rows=1)

        left_c = mgr.get('colourbar.left_c')
        left_f = mgr.get('colourbar.left_f')
        bottom = mgr.get('colourbar.bottom')
        width = mgr.get('colourbar.width')
        height = mgr.get('colourbar.height')
        fontsize = mgr.get('colourbar.fontsize')

        if self.colour_mode == 'year':
            left_year = (left_c + left_f) / 2.0
            cbar_ax_year = fig.add_axes([left_year, bottom, width, height], frameon=False)
            cbar_ax_year.set_yticks([]), cbar_ax_year.set_xticks([])
            cbar_year = plt.colorbar(
                cm.ScalarMappable(norm=self.year_norm, cmap=self.year_cmap),
                ax=cbar_ax_year,
                orientation='vertical'
            )
            cbar_year.ax.set_title('Year', fontsize=fontsize)
            cbar_year.ax.tick_params(labelsize=fontsize-2)
            if self.first_year != self.last_year:
                year_span = self.last_year - self.first_year
                if year_span <= 10:
                    cbar_year.set_ticks(np.arange(self.first_year, self.last_year + 1, 1))
                elif year_span <= 30:
                    cbar_year.set_ticks(np.arange(self.first_year, self.last_year + 1, 5))
            else:
                cbar_year.set_ticks([self.first_year])
            return

        if self.colour_mode == 'colour_value' or self.y_value_column != 'temp_C':
            metric_title = self._default_metric_colourbar_title()
            if self._is_precipitation_colour_scale():
                precip_title_fontsize = max(fontsize - 2, 1)
                metric_norm = Normalize(vmin=self.colour_min, vmax=self.colour_max)
                imperial_norm = Normalize(
                    vmin=self.mm_to_inches(self.colour_min),
                    vmax=self.mm_to_inches(self.colour_max),
                )

                cbar_ax_metric = fig.add_axes([left_c, bottom, width, height], frameon=False)
                cbar_ax_metric.set_yticks([]), cbar_ax_metric.set_xticks([])
                cbar_metric = plt.colorbar(
                    cm.ScalarMappable(norm=metric_norm, cmap=self.cmap),
                    ax=cbar_ax_metric,
                    orientation='vertical'
                )
                cbar_metric.ax.set_title(metric_title, fontsize=precip_title_fontsize)
                cbar_metric.ax.tick_params(labelsize=fontsize-2)

                cbar_ax_imperial = fig.add_axes([left_f, bottom, width, height], frameon=False)
                cbar_ax_imperial.set_yticks([]), cbar_ax_imperial.set_xticks([])
                cbar_imperial = plt.colorbar(
                    cm.ScalarMappable(norm=imperial_norm, cmap=self.cmap),
                    ax=cbar_ax_imperial,
                    orientation='vertical'
                )
                cbar_imperial.ax.set_title(
                    self._imperial_precip_title(metric_title),
                    fontsize=precip_title_fontsize,
                )
                cbar_imperial.ax.tick_params(labelsize=fontsize-2)
            else:
                left_single = (left_c + left_f) / 2.0
                cbar_ax = fig.add_axes([left_single, bottom, width, height], frameon=False)
                cbar_ax.set_yticks([]), cbar_ax.set_xticks([])
                norm = Normalize(vmin=self.colour_min, vmax=self.colour_max)
                cbar = plt.colorbar(cm.ScalarMappable(norm=norm, cmap=self.cmap), ax=cbar_ax, orientation='vertical')
                cbar.ax.set_title(metric_title, fontsize=fontsize)
                cbar.ax.tick_params(labelsize=fontsize-2)
            return

        # Celsius colorbar
        cbar_ax_c = fig.add_axes([left_c, bottom, width, height], frameon=False)
        cbar_ax_c.set_yticks([]), cbar_ax_c.set_xticks([])
        norm_c = Normalize(vmin=self.tmin_c, vmax=self.tmax_c)
        cbar_c = plt.colorbar(cm.ScalarMappable(norm=norm_c, cmap=self.cmap), ax=cbar_ax_c, orientation='vertical')
        cbar_c.ax.set_title(r'$^\circ\mathrm{C}$', fontsize=fontsize)
        cbar_c.ax.tick_params(labelsize=fontsize-2)

        # Fahrenheit colorbar
        cbar_ax_f = fig.add_axes([left_f, bottom, width, height], frameon=False)
        cbar_ax_f.set_yticks([]), cbar_ax_f.set_xticks([])
        norm_f = Normalize(vmin=self.temp_c_to_f(self.tmin_c), vmax=self.temp_c_to_f(self.tmax_c))
        cbar_f = plt.colorbar(cm.ScalarMappable(norm=norm_f, cmap=self.cmap), ax=cbar_ax_f, orientation='vertical')
        cbar_f.ax.set_title(r'$^\circ\mathrm{F}$', fontsize=fontsize)
        cbar_f.ax.tick_params(labelsize=fontsize-2)

    def get_point_colours(self, df: pd.DataFrame):
        """
        Build RGBA point colours based on the active colour mode.

        Args:
            df: DataFrame with plotting data.

        Returns:
            Array-like RGBA colours for scatter plotting.
        """
        if self.colour_mode == 'year':
            years = pd.to_datetime(df['date']).dt.year.astype(float)
            return self.year_cmap(self.year_norm(years))

        c = self.norm(df[self.colour_source_column])
        return self.cmap(c)

    def _prepare_render_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return DataFrame in plotting order for improved visibility in overlaps."""
        if self.y_value_column == 'wet_hours_per_day' and 'precip_mm' in df.columns:
            sort_columns = ['angle', 'precip_mm', 'wet_hours_per_day']
            return df.sort_values(
                by=sort_columns,
                ascending=[True, True, True],
                kind='mergesort',
            )
        return df

    def draw_temp_circles(self, ax: plt.Axes, num_rows: int = 1) -> None:
        """
        Draw value circles and labels at configured step boundaries on the polar plot.

        Args:
            ax: Polar axes to draw on.
            num_rows: Number of rows in subplot grid (for font scaling).
        """
        settings = SettingsManager(self.all_settings[self.layout], num_rows)

        temp_step = self.y_step if self.y_step is not None else self.DEFAULT_Y_STEP
        ytick_fontsize = settings.get('figure.ytick_fontsize')
        ytick_colour = settings.get('figure.ytick_colour')

        start_tick = np.ceil(self.tmin_c / temp_step) * temp_step
        ticks = np.arange(start_tick, self.tmax_c + 1, temp_step)
        if self.max_y_steps is not None and len(ticks) > self.max_y_steps:
            step_multiplier = int(np.ceil(len(ticks) / self.max_y_steps))
            adjusted_step = temp_step * step_multiplier
            ticks = np.arange(start_tick, self.tmax_c + 1, adjusted_step)

        for t in ticks:
            ax.plot(np.linspace(0, 2*np.pi, 361), np.full(361, t), '--', color='gray', lw=0.7, alpha=0.7)
            if self.y_value_column == 'temp_C':
                # °C label above X-axis
                ax.text(np.pi/2, t, f'{int(t)}°C', color=ytick_colour, fontsize=ytick_fontsize, ha='center', va='bottom', alpha=0.8)
                # °F label below X-axis
                ax.text(3*np.pi/2, t, f'{int(self.temp_c_to_f(t))}°F', color=ytick_colour, fontsize=ytick_fontsize, ha='center', va='top', alpha=0.8)
            else:
                unit_suffix = self.measure_unit if self.measure_unit else self.y_value_column
                ax.text(
                    np.pi/2,
                    t,
                    f"{int(t)} {unit_suffix}",
                    color=ytick_colour,
                    fontsize=ytick_fontsize,
                    ha='center',
                    va='bottom',
                    alpha=0.8,
                )

    def create_polar_plot(self, ax: plt.Axes, df: pd.DataFrame, num_rows: int = 1) -> None:
        """
        Create a polar scatter plot for the given DataFrame and axes.

        Args:
            ax: Polar axes to plot on.
            df: DataFrame with temperature and angle columns.
            num_rows: Number of rows in subplot grid (for font scaling).
        """
        settings = SettingsManager(self.all_settings[self.layout], num_rows)
        render_df = self._prepare_render_df(df)
        point_colours = self.get_point_colours(render_df)

        marker_size = settings.get('figure.marker_size')
        xtick_fontsize = settings.get('figure.xtick_fontsize')

        radial_base = self.tmin_c
        if self.plot_format in {'radial_bars', 'wedges'}:
            bar_width = (2 * np.pi / 365.0) * self.wedge_width_scale
            values = render_df[self.y_value_column].to_numpy(dtype=float)
            radial_base = min(0.0, float(np.nanmin(values)))
            heights = np.maximum(values - radial_base, 0.0)
            theta_left = render_df['angle'].to_numpy(dtype=float) - (bar_width / 2.0)
            ax.bar(
                theta_left,
                heights,
                width=bar_width,
                bottom=radial_base,
                color=point_colours,
                linewidth=0,
                align='edge',
            )
        else:
            ax.scatter(render_df['angle'], render_df[self.y_value_column], c=point_colours, s=marker_size)
        self.draw_temp_circles(ax, num_rows)
        ax.set_theta_offset(np.pi/2)
        ax.set_theta_direction(-1)
        ax.set_xticks(np.arange(0, 2 * np.pi, np.pi / 6))
        ax.set_xticklabels(self.MONTH_LABELS, fontsize=xtick_fontsize)
        ax.set_yticks([])

        # Set y-axis limits, handling case where min == max
        y_min = min(self.tmin_c, radial_base)
        if y_min < self.tmax_c:
            ax.set_ylim(y_min, self.tmax_c)
        else:
            # Add a small margin when all temps are the same
            margin = 1.0
            ax.set_ylim(y_min - margin, self.tmax_c + margin)

    def plot_polar(
        self,
        title: str = "",
        credit: str = "",
        data_source: str = "",
        save_file: str = "",
        layout: str = "polar_single",
        show_plot: bool = True
    ) -> None:
        """
        Plot a single polar temperature plot and save to file.

        Args:
            title: Plot title.
            credit: Credit text.
            data_source: Data source text.
            save_file: Output file path.
            layout: Settings layout to use (default 'polar_single').
            show_plot: Whether to display the plot on screen (default True).
        """
        try:
            self.layout = layout if layout else self.layout
            settings = self.all_settings[self.layout]
        except Exception as e:
            raise RuntimeError(f"Error loading settings layout {layout}: {e}") from e

        mgr = SettingsManager(settings, num_rows=1)

        fig_width = mgr.get('figure.fig_width_in')
        fig_height = mgr.get('figure.fig_height_in')
        fig = plt.figure(figsize=(fig_width, fig_height))

        # Make left and right margins symmetrical (match colorbar width)
        cbar_width = mgr.get('colourbar.width')
        cbar_bottom = mgr.get('colourbar.bottom')
        cbar_height = mgr.get('colourbar.height')
        plot_width = 1.0 - 2 * cbar_width  # Remaining width for polar plot
        ax = fig.add_axes([cbar_width, cbar_bottom, plot_width, cbar_height], polar=True)
        self.create_polar_plot(ax, self.df)

        # Show temp range under the subplot using the parent figure
        range_min, range_max = self._get_range_bounds(self.df)
        temp_range_text = self._format_range_text(range_min, range_max, df=self.df)

        # Place the text just below the subplot
        bbox = ax.get_position()
        temp_label_vspace = mgr.get('figure.temp_label_vspace')
        temp_label_fontsize = mgr.get('figure.temp_label_fontsize')
        temp_label_colour = mgr.get('figure.temp_label_colour')
        fig.text(
            bbox.x0 + bbox.width / 2,
            bbox.y0 - temp_label_vspace,
            temp_range_text,
            ha='center',
            va='top',
            fontsize=temp_label_fontsize,
            color=temp_label_colour,
        )

        # Add dual colorbars further from the polar plot
        self.add_dual_colourbars(fig)

        # Centre the title over the polar plot (not the whole figure) and move it down slightly
        title_fontsize = mgr.get('page.title_fontsize')
        title_colour = mgr.get('page.title_colour')
        label_fontsize = mgr.get('page.label_fontsize')
        label_left = mgr.get('page.label_left')
        label_right = mgr.get('page.label_right')
        label_bottom = mgr.get('page.label_bottom')
        dpi = mgr.get('page.dpi')

        ax.set_title(title, fontsize=title_fontsize, pad=12, color=title_colour)
        plt.figtext(label_left, label_bottom, credit, verticalalignment='center', horizontalalignment='left', fontsize=label_fontsize)
        plt.figtext(label_right, label_bottom, data_source, verticalalignment='center', horizontalalignment='right', fontsize=label_fontsize)
        if show_plot:
            plt.show()
        fig.savefig(save_file, dpi=dpi, bbox_inches="tight")
        plt.close(fig)

    def subplot_polar(
        self,
        ax: plt.Axes,
        df: pd.DataFrame,
        cbar: bool = False,
        title: str = "",
        num_rows: int = 1
    ) -> None:
        """
        Plot a polar subplot for a given DataFrame and axes.

        Args:
            ax: Polar axes to plot on.
            df: DataFrame with temperature and angle columns.
            cbar: Whether to add colorbars (default False).
            title: Subplot title (default empty).
            num_rows: Number of rows in subplot grid (for font scaling).
        """
        settings = self.all_settings[self.layout]
        mgr = SettingsManager(settings, num_rows)

        fig = ax.get_figure()
        self.create_polar_plot(ax, df, num_rows)

        # Move month xticklabels closer to the polar plot
        xtick_pad = mgr.get('figure.xtick_pad')
        ax.tick_params(axis='x', pad=xtick_pad)

        # Get row-based scaled settings
        title_fontsize = mgr.get('page.title_fontsize')
        title_colour = mgr.get('page.title_colour')
        temp_label_fontsize = mgr.get('figure.temp_label_fontsize')
        temp_label_vspace = mgr.get('figure.temp_label_vspace')
        temp_label_colour = mgr.get('figure.temp_label_colour')

        ax.set_title(title if title else '', fontsize=title_fontsize, pad=0, color=title_colour)

        # Show temp range under the subplot using the parent figure
        range_min, range_max = self._get_range_bounds(df)
        temp_range_text = self._format_range_text(range_min, range_max, df=df)

        # Place the text just below the subplot using scaled vspace
        bbox = ax.get_position()
        fig.text(
            bbox.x0 + bbox.width / 2,
            bbox.y0 - temp_label_vspace,
            temp_range_text,
            ha='center',
            va='top',
            fontsize=temp_label_fontsize,
            color=temp_label_colour,
        )
        if cbar:
            self.add_dual_colourbars(fig)

    def plot_polar_subplots(
        self,
        subplot_field: str = "place_name",
        subplot_title_template: str | None = None,
        subplot_title_context: dict[str, object] | None = None,
        num_rows: int = 2,
        num_cols: int = None,
        title: str = "",
        credit: str = "",
        data_source: str = "",
        save_file: str = "",
        layout: str = "polar_subplot",
        show_plot: bool = True
    ) -> None:
        """
        Plot multiple polar subplots for each unique value in a DataFrame field.

        Args:
            subplot_field: DataFrame column to split subplots by.
            subplot_title_template: Optional template for each subplot title.
            subplot_title_context: Optional context values for subplot title template.
            num_rows: Number of subplot rows (default 2).
            num_cols: Number of subplot columns (default None, auto-calculated).
            title: Overall plot title.
            credit: Credit text.
            data_source: Data source text.
            save_file: Output file path.
            layout: Settings layout to use (default 'polar_subplot').
            show_plot: Whether to display the plot on screen (default True).
        """
        try:
            self.layout = layout if layout else self.layout
            settings = self.all_settings[self.layout]
        except Exception as e:
            raise RuntimeError(f"Error loading settings layout {layout}: {e}") from e

        overall_title = title

        place_list = self.df[subplot_field].unique()
        num_plots = len(place_list)
        if num_cols is None:
            num_cols = int(np.ceil(num_plots / num_rows))

        # Use SettingsManager for row-based settings
        mgr = SettingsManager(settings, num_rows)

        # Always use A3 landscape size (13.34" × 7.5")
        base_width = mgr.get('figure.fig_width_in')
        base_height = mgr.get('figure.fig_height_in')

        fig, axs = plt.subplots(num_rows, num_cols, figsize=(base_width, base_height), subplot_kw={'polar': True})

        # Get spacing settings (already row-scaled via SettingsManager)
        adjusted_hspace = mgr.get('subplot.hspace')
        adjusted_top = mgr.get('subplot.top')
        adjusted_bottom = mgr.get('subplot.bottom')
        subplot_left = mgr.get('subplot.left')
        subplot_right = mgr.get('subplot.right')
        wspace = mgr.get('subplot.wspace')

        plt.subplots_adjust(left=subplot_left, right=subplot_right, hspace=adjusted_hspace, wspace=wspace, top=adjusted_top, bottom=adjusted_bottom)

        for row in range(num_rows):
            for col in range(num_cols):
                plot_idx = row * num_cols + col
                if num_rows == 1 and num_cols == 1:
                    ax = axs  # single subplot
                elif num_rows > 1 and num_cols > 1:
                    ax = axs[row, col]
                elif num_rows == 1:
                    ax = axs[col]
                else:  # num_cols == 1
                    ax = axs[row]
                if plot_idx < num_plots:
                    place = place_list[plot_idx]
                    df_place = self.df[self.df[subplot_field] == place].sort_values('day_of_year')

                    # Control subplot size using row-scaled settings
                    pos = ax.get_position()
                    height_scale = mgr.get('subplot.height_scale')
                    width_scale = mgr.get('subplot.width_scale')

                    if num_rows > 2:
                        # For 3+ rows: calculate size based on allocated space
                        available_height = adjusted_top - adjusted_bottom
                        height_per_row = available_height / num_rows
                        target_height = height_per_row * height_scale

                        available_width = subplot_right - subplot_left
                        width_per_col = available_width / num_cols
                        target_width = width_per_col * width_scale

                        # Center the subplot in its allocated space
                        center_x = subplot_left + (col + 0.5) * width_per_col
                        center_y = adjusted_bottom + (num_rows - row - 0.5) * height_per_row
                        new_x = center_x - target_width / 2
                        new_y = center_y - target_height / 2

                        ax.set_position([new_x, new_y, target_width, target_height])
                    else:
                        # For 1-2 rows: expand from default position using scale factors
                        new_width = pos.width * width_scale
                        new_height = pos.height * height_scale
                        ax.set_position([pos.x0, pos.y0, new_width, new_height])

                    if subplot_title_template is not None:
                        base_context = dict(subplot_title_context or {})
                        start_year = int(base_context.get('start_year', self.first_year))
                        end_year = int(base_context.get('end_year', self.last_year))
                        year_range = base_context.get('year_range')
                        if year_range is None:
                            year_range = str(start_year) if start_year == end_year else f"{start_year}-{end_year}"
                        title_context = {
                            **base_context,
                            'location': place,
                            'place': place,
                            'place_name': place,
                            'start_year': start_year,
                            'end_year': end_year,
                            'year_range': year_range,
                        }
                        try:
                            subplot_title = str(subplot_title_template).format(**title_context)
                        except KeyError as exc:
                            raise ValueError(
                                f"Missing placeholder context for subplot_title_template: {exc}"
                            ) from exc
                    elif self.first_year != self.last_year:
                        subplot_title = f"{place} ({self.first_year}-{self.last_year})"
                    else:
                        subplot_title = f"{place} ({self.first_year})"
                    self.subplot_polar(df=df_place, ax=ax, cbar=False, title=subplot_title, num_rows=num_rows)
                else:
                    ax.axis('off')  # Hide unused subplots

        self.add_dual_colourbars(fig)

        if overall_title:
            title_fontsize = mgr.get('page.overall_title_fontsize', mgr.get('page.title_fontsize'))
            title_colour = mgr.get('page.title_colour')
            left_title_x = mgr.get('page.left_title_x', 0.015)
            fig.text(
                left_title_x,
                0.5,
                overall_title,
                rotation=90,
                va='center',
                ha='center',
                fontsize=title_fontsize,
                color=title_colour,
            )

        label_fontsize = mgr.get('page.label_fontsize')
        dpi = mgr.get('page.dpi')
        plt.figtext(0.05, 0.03, credit, verticalalignment='center', horizontalalignment='left', fontsize=label_fontsize)
        plt.figtext(0.93, 0.03, data_source, verticalalignment='center', horizontalalignment='right', fontsize=label_fontsize)
        if show_plot:
            plt.show()
        fig.savefig(save_file, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
