import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yaml
import logging
from matplotlib import cm
from matplotlib.colors import Normalize
from settings_manager import SettingsManager

logger = logging.getLogger("geo")


class Visualizer:
    """
    Visualizer for creating polar temperature plots and subplots from temperature data.
    Handles loading of settings, data preparation, and all plotting routines.
    """

    PAGE_A3_WIDTH_CM = 33.87  # A3 width in cm
    PAGE_A3_HEIGHT_CM = 19.05  # A3 height in cm
    CM_PER_INCH = 2.54
    MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    def __init__(
        self,
        df: pd.DataFrame,
        t_min_c: float = None,
        t_max_c: float = None,
        out_dir: str = 'output',
        settings_file: str = 'settings.yaml',
        y_value_column: str = 'temp_C',
        range_text_template: str | None = None,
        range_text_context: dict[str, str] | None = None,
        colour_mode: str = 'y_value',
        colormap_name: str = 'turbo'
    ) -> None:
        """
        Initialize the Visualizer with data and plotting settings.

        Args:
            df: DataFrame containing temperature and date columns (must include 'date' and 'temp_C').
            t_min_c: Minimum temperature for color normalization (optional).
            t_max_c: Maximum temperature for color normalization (optional).
            out_dir: Output directory for plots.
            settings_file: Path to YAML settings file.
            y_value_column: DataFrame column used for y-value range text.
            range_text_template: Optional template used for the per-plot value range text.
            range_text_context: Optional context values used with range_text_template.
            colour_mode: Colour mapping mode ('y_value' or 'year').
            colormap_name: Matplotlib colormap name for point colouring.
        Raises:
            ValueError: If the DataFrame is empty or None.
        """

        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None.")

        self.df = df
        self.out_dir = out_dir
        self.y_value_column = y_value_column
        self.range_text_template = (
            range_text_template
            if isinstance(range_text_template, str) and range_text_template.strip()
            else "{measure_label}: {min_value:.1f} to {max_value:.1f} {measure_unit}"
        )
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

        valid_modes = {"y_value", "year"}
        if colour_mode not in valid_modes:
            raise ValueError(f"Invalid colour_mode '{colour_mode}'. Expected one of {sorted(valid_modes)}")
        self.colour_mode = colour_mode

        self.first_year = pd.to_datetime(self.df['date'].min()).year
        self.last_year = pd.to_datetime(self.df['date'].max()).year

        self.tmin_c = t_min_c if t_min_c is not None else np.min(self.df["temp_C"])
        self.tmax_c = t_max_c if t_max_c is not None else np.max(self.df["temp_C"])
        try:
            self.cmap = plt.get_cmap(colormap_name)
        except Exception as e:
            raise ValueError(f"Unknown colormap '{colormap_name}': {e}") from e
        self.colormap_name = colormap_name
        self.norm = Normalize(vmin=self.tmin_c, vmax=self.tmax_c)
        if self.first_year == self.last_year:
            self.year_norm = Normalize(vmin=self.first_year - 0.5, vmax=self.first_year + 0.5)
        else:
            self.year_norm = Normalize(vmin=self.first_year, vmax=self.last_year)
        self.year_cmap = self.cmap

    def _get_range_bounds(self, df: pd.DataFrame) -> tuple[float, float]:
        """Return min/max values for configured y-value column with safe fallback."""
        range_column = self.y_value_column if self.y_value_column in df.columns else 'temp_C'
        min_value = float(df[range_column].min())
        max_value = float(df[range_column].max())
        return min_value, max_value

    def _format_range_text(self, min_value: float, max_value: float) -> str:
        """Format value-range text using configured template/context."""
        measure_unit = self.range_text_context.get('measure_unit', '')
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
        }
        try:
            return self.range_text_template.format(**context)
        except Exception:
            measure_label = self.range_text_context.get('measure_label', 'Value')
            return f"{measure_label}: {min_value:.1f} to {max_value:.1f} {measure_unit}".strip()

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

        c = self.norm(df["temp_C"])
        return self.cmap(c)

    def draw_temp_circles(self, ax: plt.Axes, num_rows: int = 1) -> None:
        """
        Draw circles and temperature labels at every 10°C boundary on the polar plot.

        Args:
            ax: Polar axes to draw on.
            num_rows: Number of rows in subplot grid (for font scaling).
        """
        settings = SettingsManager(self.all_settings[self.layout], num_rows)

        temp_step = settings.get('figure.temp_step')
        ytick_fontsize = settings.get('figure.ytick_fontsize')
        ytick_colour = settings.get('figure.ytick_colour')

        for t in np.arange(np.ceil(self.tmin_c/temp_step)*temp_step, self.tmax_c+1, temp_step):
            ax.plot(np.linspace(0, 2*np.pi, 361), np.full(361, t), '--', color='gray', lw=0.7, alpha=0.7)
            # °C label above X-axis
            ax.text(np.pi/2, t, f'{int(t)}°C', color=ytick_colour, fontsize=ytick_fontsize, ha='center', va='bottom', alpha=0.8)
            # °F label below X-axis
            ax.text(3*np.pi/2, t, f'{int(self.temp_c_to_f(t))}°F', color=ytick_colour, fontsize=ytick_fontsize, ha='center', va='top', alpha=0.8)

    def create_polar_plot(self, ax: plt.Axes, df: pd.DataFrame, num_rows: int = 1) -> None:
        """
        Create a polar scatter plot for the given DataFrame and axes.

        Args:
            ax: Polar axes to plot on.
            df: DataFrame with temperature and angle columns.
            num_rows: Number of rows in subplot grid (for font scaling).
        """
        settings = SettingsManager(self.all_settings[self.layout], num_rows)
        point_colours = self.get_point_colours(df)

        marker_size = settings.get('figure.marker_size')
        xtick_fontsize = settings.get('figure.xtick_fontsize')

        ax.scatter(df['angle'], df["temp_C"], c=point_colours, s=marker_size)
        self.draw_temp_circles(ax, num_rows)
        ax.set_theta_offset(np.pi/2)
        ax.set_theta_direction(-1)
        ax.set_xticks(np.arange(0, 2 * np.pi, np.pi / 6))
        ax.set_xticklabels(self.MONTH_LABELS, fontsize=xtick_fontsize)
        ax.set_yticks([])

        # Set y-axis limits, handling case where min == max
        if self.tmin_c < self.tmax_c:
            ax.set_ylim(self.tmin_c, self.tmax_c)
        else:
            # Add a small margin when all temps are the same
            margin = 1.0 if self.tmin_c != 0 else 1.0
            ax.set_ylim(self.tmin_c - margin, self.tmax_c + margin)

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
        temp_range_text = self._format_range_text(range_min, range_max)

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
        temp_range_text = self._format_range_text(range_min, range_max)

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

        place_list = self.df[subplot_field].unique()
        num_plots = len(place_list)
        if num_cols is None:
            num_cols = int(np.ceil(num_plots / num_rows))

        # Use SettingsManager for row-based settings
        mgr = SettingsManager(settings, num_rows)

        # Always use A3 landscape size (13.34" × 7.5")
        base_width = mgr.get('figure.fig_width_in')
        base_height = mgr.get('figure.fig_height_in')
        logger.debug(f"Using A3 landscape size: {base_width:.2f}\" × {base_height:.2f}\"")

        fig, axs = plt.subplots(num_rows, num_cols, figsize=(base_width, base_height), subplot_kw={'polar': True})

        # Get spacing settings (already row-scaled via SettingsManager)
        adjusted_hspace = mgr.get('subplot.hspace')
        adjusted_top = mgr.get('subplot.top')
        adjusted_bottom = mgr.get('subplot.bottom')
        subplot_left = mgr.get('subplot.left')
        subplot_right = mgr.get('subplot.right')
        wspace = mgr.get('subplot.wspace')

        logger.debug(f"Grid {num_rows}x{num_cols} spacing: hspace={adjusted_hspace}, top={adjusted_top}, bottom={adjusted_bottom}")
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

                        logger.debug(f"Row {row}, Col {col} - Sizing: {target_width:.3f}x{target_height:.3f} at ({new_x:.3f}, {new_y:.3f})")
                        ax.set_position([new_x, new_y, target_width, target_height])
                    else:
                        # For 1-2 rows: expand from default position using scale factors
                        new_width = pos.width * width_scale
                        new_height = pos.height * height_scale
                        logger.debug(f"Row {row}, Col {col} - Expanding: {new_width:.3f}x{new_height:.3f}")
                        ax.set_position([pos.x0, pos.y0, new_width, new_height])

                    if self.first_year != self.last_year:
                        title = f"{place} ({self.first_year}-{self.last_year})"
                    else:
                        title = f"{place} ({self.first_year})"
                    self.subplot_polar(df=df_place, ax=ax, cbar=False, title=title, num_rows=num_rows)
                else:
                    ax.axis('off')  # Hide unused subplots

        self.add_dual_colourbars(fig)

        label_fontsize = mgr.get('page.label_fontsize')
        dpi = mgr.get('page.dpi')
        plt.figtext(0.05, 0.03, credit, verticalalignment='center', horizontalalignment='left', fontsize=label_fontsize)
        plt.figtext(0.93, 0.03, data_source, verticalalignment='center', horizontalalignment='right', fontsize=label_fontsize)
        if show_plot:
            plt.show()
        fig.savefig(save_file, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
