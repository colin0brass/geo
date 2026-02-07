import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import yaml
import logging
from matplotlib import cm
from matplotlib.colors import Normalize

logger = logging.getLogger("geo_temp")


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
    
    def __init__(self, df: pd.DataFrame, t_min_c: float = None, t_max_c: float = None, out_dir: str = 'output', settings_file: str = 'settings.yaml') -> None:
        """
        Initialize the Visualizer with data and plotting settings.
        
        Args:
            df: DataFrame containing temperature and date columns (must include 'date' and 'temp_C').
            t_min_c: Minimum temperature for color normalization (optional).
            t_max_c: Maximum temperature for color normalization (optional).
            out_dir: Output directory for plots.
            settings_file: Path to YAML settings file.
        Raises:
            ValueError: If the DataFrame is empty or None.
        """

        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None.")
        
        self.df = df
        self.out_dir = out_dir

        self.settings_file = settings_file
        try:
            self.all_settings = self.load_settings_from_yaml(settings_file)
            self.layouts = list(self.all_settings.keys())
            self.layout = self.layouts[0]  # Default to the first layout
        except Exception as e:
            logger.error(f"Error loading settings from YAML file: {e}")
            self.all_settings = {}

        self.df = self.add_data_fields(df)

        self.first_year = pd.to_datetime(self.df['date'].min()).year
        self.last_year = pd.to_datetime(self.df['date'].max()).year

        self.tmin_c = t_min_c if t_min_c is not None else np.min(self.df["temp_C"])
        self.tmax_c = t_max_c if t_max_c is not None else np.max(self.df["temp_C"])
        self.norm = Normalize(vmin=self.tmin_c, vmax=self.tmax_c)
        self.cmap = cm.turbo

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
        Display saved plot images by loading them from disk.
        
        Args:
            plot_files: List of file paths to saved plot images.
        """
        if not plot_files:
            return
        
        if len(plot_files) > 1:
            logger.info(f"Displaying all {len(plot_files)} batch images...")
        else:
            logger.info(f"Displaying main plot...")
        
        for plot_file in plot_files:
            img = mpimg.imread(plot_file)
            fig, ax = plt.subplots(figsize=(13.34, 7.5))
            ax.imshow(img)
            ax.axis('off')
            plt.tight_layout()
            plt.show()
            plt.close(fig)
    
    def add_dual_colourbars(self, fig: plt.Figure) -> None:
        """
        Add Celsius and Fahrenheit colorbars to a figure with improved sizing and font.
        
        Args:
            fig: Matplotlib Figure object to which colorbars are added.
        """
        settings=self.all_settings[self.layout]['colourbar']

        # Celsius colorbar
        cbar_ax_c = fig.add_axes([settings['left_c'], settings['bottom'], settings['width'], settings['height']], frameon=False)
        cbar_ax_c.set_yticks([]), cbar_ax_c.set_xticks([])
        norm_c = Normalize(vmin=self.tmin_c, vmax=self.tmax_c)
        cbar_c = plt.colorbar(cm.ScalarMappable(norm=norm_c, cmap=self.cmap), ax=cbar_ax_c, orientation='vertical')
        cbar_c.ax.set_title(r'$^\circ\mathrm{C}$', fontsize=settings['fontsize'])
        cbar_c.ax.tick_params(labelsize=settings['fontsize']-2)

        # Fahrenheit colorbar
        cbar_ax_f = fig.add_axes([settings['left_f'], settings['bottom'], settings['width'], settings['height']], frameon=False)
        cbar_ax_f.set_yticks([]), cbar_ax_f.set_xticks([])
        norm_f = Normalize(vmin=self.temp_c_to_f(self.tmin_c), vmax=self.temp_c_to_f(self.tmax_c))
        cbar_f = plt.colorbar(cm.ScalarMappable(norm=norm_f, cmap=self.cmap), ax=cbar_ax_f, orientation='vertical')
        cbar_f.ax.set_title(r'$^\circ\mathrm{F}$', fontsize=settings['fontsize'])
        cbar_f.ax.tick_params(labelsize=settings['fontsize']-2)

    def draw_temp_circles(self, ax: plt.Axes, num_rows: int = 1) -> None:
        """
        Draw circles and temperature labels at every 10°C boundary on the polar plot.
        
        Args:
            ax: Polar axes to draw on.
            num_rows: Number of rows in subplot grid (for font scaling).
        """
        settings=self.all_settings[self.layout]['figure']
        
        # Reduce radial label font size based on number of rows
        if num_rows >= 4:
            ytick_fontsize = max(3, settings['ytick_fontsize'] - 3)  # Reduce by 3 for 4+ rows, min 3
        elif num_rows > 2:
            ytick_fontsize = max(4, settings['ytick_fontsize'] - 2)  # Reduce by 2 for 3 rows, min 4
        else:
            ytick_fontsize = settings['ytick_fontsize']
            
        for t in np.arange(np.ceil(self.tmin_c/settings['temp_step'])*settings['temp_step'], self.tmax_c+1, settings['temp_step']):
            ax.plot(np.linspace(0, 2*np.pi, 361), np.full(361, t), '--', color='gray', lw=0.7, alpha=0.7)
            # °C label above X-axis
            ax.text(np.pi/2, t, f'{int(t)}°C', color=settings['ytick_colour'], fontsize=ytick_fontsize, ha='center', va='bottom', alpha=0.8)
            # °F label below X-axis
            ax.text(3*np.pi/2, t, f'{int(self.temp_c_to_f(t))}°F', color=settings['ytick_colour'], fontsize=ytick_fontsize, ha='center', va='top', alpha=0.8)

    def create_polar_plot(self, ax: plt.Axes, df: pd.DataFrame, num_rows: int = 1) -> None:
        """
        Create a polar scatter plot for the given DataFrame and axes.
        
        Args:
            ax: Polar axes to plot on.
            df: DataFrame with temperature and angle columns.
            num_rows: Number of rows in subplot grid (for font scaling).
        """
        settings = self.all_settings[self.layout]
        c = self.norm(df["temp_C"])
        ax.scatter(df['angle'], df["temp_C"], c=self.cmap(c), s=2)
        self.draw_temp_circles(ax, num_rows)
        ax.set_theta_offset(np.pi/2)
        ax.set_theta_direction(-1)
        ax.set_xticks(np.arange(0, 2 * np.pi, np.pi / 6))
        figure_settings=settings['figure']
        
        # Reduce month label font size based on number of rows
        if num_rows >= 4:
            xtick_fontsize = max(6, figure_settings['xtick_fontsize'] - 3)  # Reduce by 3 for 4+ rows, min 6
        elif num_rows > 2:
            xtick_fontsize = max(7, figure_settings['xtick_fontsize'] - 2)  # Reduce by 2 for 3 rows, min 7
        else:
            xtick_fontsize = figure_settings['xtick_fontsize']
            
        ax.set_xticklabels(self.MONTH_LABELS, fontsize=xtick_fontsize)
        ax.set_yticks([])
        ax.set_ylim(self.tmin_c, self.tmax_c)

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
            raise(f"Error loading settings layout {layout}: {e}")
        
        fig = plt.figure(figsize=(settings['figure']['fig_width_in'], settings['figure']['fig_height_in']))
        
        # Make left and right margins symmetrical (match colorbar width)
        colourbar_settings = settings['colourbar']
        plot_width = 1.0 - 2 * colourbar_settings['width']  # Remaining width for polar plot
        ax = fig.add_axes([colourbar_settings['width'], colourbar_settings['bottom'], plot_width, colourbar_settings['height']], polar=True)
        self.create_polar_plot(ax, self.df)

        # Show temp range under the subplot using the parent figure
        plot_tmin_c = self.df["temp_C"].min()
        plot_tmax_c = self.df["temp_C"].max()
        temp_range_text = f"{plot_tmin_c:.1f}°C to {plot_tmax_c:.1f}°C; ({self.temp_c_to_f(plot_tmin_c):.1f}°F to {self.temp_c_to_f(plot_tmax_c):.1f}°F)"

        # Place the text just below the subplot
        bbox = ax.get_position()
        figure_settings = settings['figure']
        fig.text(bbox.x0 + bbox.width/2, bbox.y0 - figure_settings['temp_label_vspace'], temp_range_text,
            ha='center', va='top', fontsize=figure_settings['temp_label_fontsize'],
            color=figure_settings['temp_label_colour'])
        
        # Add dual colorbars further from the polar plot
        self.add_dual_colourbars(fig)

        # Centre the title over the polar plot (not the whole figure) and move it down slightly
        page_settings = settings['page']
        ax.set_title(title, fontsize=page_settings['title_fontsize'], pad=12, color=page_settings['title_colour'])
        plt.figtext(page_settings['label_left'], page_settings['label_bottom'], credit, verticalalignment='center', horizontalalignment='left', fontsize=page_settings['label_fontsize'])
        plt.figtext(page_settings['label_right'], page_settings['label_bottom'], data_source, verticalalignment='center', horizontalalignment='right', fontsize=page_settings['label_fontsize'])
        if show_plot:
            plt.show()
        fig.savefig(save_file, dpi=page_settings['dpi'], bbox_inches="tight")
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
        page_settings = settings['page']
        figure_settings = settings['figure']

        fig = ax.get_figure()
        self.create_polar_plot(ax, df, num_rows)

        # Move month xticklabels closer to the polar plot
        ax.tick_params(axis='x', pad=figure_settings['xtick_pad'])
        
        # Reduce font sizes based on number of rows
        if num_rows >= 4:
            # More aggressive reduction for 4+ rows
            title_fontsize = max(7, page_settings['title_fontsize'] - 5)  # Reduce by 5, min 7
            temp_label_fontsize = max(3, figure_settings['temp_label_fontsize'] - 4)  # Reduce by 4, min 3
        elif num_rows > 2:
            # Moderate reduction for 3 rows
            title_fontsize = max(8, page_settings['title_fontsize'] - 3)  # Reduce by 3, min 8
            temp_label_fontsize = max(4, figure_settings['temp_label_fontsize'] - 3)  # Reduce by 3, min 4
        else:
            title_fontsize = page_settings['title_fontsize']
            temp_label_fontsize = figure_settings['temp_label_fontsize']
            
        ax.set_title(title if title else '', fontsize=title_fontsize, pad=0, color=page_settings['title_colour'])

        # Show temp range under the subplot using the parent figure
        plot_tmin_c = df["temp_C"].min()
        plot_tmax_c = df["temp_C"].max()
        temp_range_text = f"{plot_tmin_c:.1f}°C to {plot_tmax_c:.1f}°C; ({self.temp_c_to_f(plot_tmin_c):.1f}°F to {self.temp_c_to_f(plot_tmax_c):.1f}°F)"

        # Place the text just below the subplot
        bbox = ax.get_position()
        fig.text(bbox.x0 + bbox.width/2, bbox.y0 - figure_settings['temp_label_vspace'], temp_range_text,
            ha='center', va='top', fontsize=temp_label_fontsize,
            color=figure_settings['temp_label_colour'])
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
        show_plot: bool = True,
        scale_height: bool = True
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
            scale_height: Whether to scale figure height for 3+ rows (default True).
        """
        try:
            self.layout = layout if layout else self.layout
            settings = self.all_settings[self.layout]
        except Exception as e:
            raise(f"Error loading settings layout {layout}: {e}")

        place_list = self.df[subplot_field].unique()
        num_plots = len(place_list)
        if num_cols is None:
            num_cols = int(np.ceil(num_plots / num_rows))
            
        # Adjust the figure so that colorbars are inside the page boundaries
        # Reserve space on the right for the colorbars within the page width
        figure_settings = settings['figure']
        
        # Always use A3 landscape size (13.34" × 7.5")
        base_height = figure_settings['fig_height_in']
        base_width = figure_settings['fig_width_in']
        logger.debug(f"Using A3 landscape size: {base_width:.2f}\" × {base_height:.2f}\"")
            
        fig, axs = plt.subplots(num_rows, num_cols, figsize=(base_width, base_height), subplot_kw={'polar': True})
    
        # Adjust spacing based on number of rows
        subplot_settings = settings['subplot']
        if scale_height and num_rows > 2:
            # Smart adjustments for 3+ rows to fit in A3 landscape
            adjusted_hspace = 0.25
            adjusted_top = 0.92
            adjusted_bottom = 0.08
            logger.debug(f"SMART SCALING ENABLED - {num_rows} rows with hspace={adjusted_hspace}, top={adjusted_top}, bottom={adjusted_bottom}")
        else:
            # Use default settings (will likely cause overlap for 3+ rows)
            adjusted_hspace = subplot_settings['hspace']
            adjusted_top = subplot_settings['top']
            adjusted_bottom = subplot_settings['bottom']
            if num_rows > 2:
                logger.debug(f"SMART SCALING DISABLED - Using default spacing (will likely overlap)")
            
        plt.subplots_adjust(left=subplot_settings['left'], right=subplot_settings['right'], hspace=adjusted_hspace, wspace=subplot_settings['wspace'], top=adjusted_top, bottom=adjusted_bottom)

        for row in range(num_rows):
            for col in range(num_cols):
                plot_idx = row * num_cols + col
                if num_rows > 1 and num_cols > 1:
                    ax = axs[row, col]
                elif num_rows == 1:
                    ax = axs[col]
                elif num_cols == 1:
                    ax = axs[row]
                else:
                    ax = axs  # single subplot
                if plot_idx < num_plots:
                    place = place_list[plot_idx]
                    df_place = self.df[self.df[subplot_field] == place].sort_values('day_of_year')

                    # Explicitly control subplot size
                    pos = ax.get_position()
                    if scale_height and num_rows > 2:
                        # Smart sizing for 3+ rows to fit in A3 landscape
                        # Calculate available vertical space and divide among rows
                        available_height = adjusted_top - adjusted_bottom
                        height_per_row = available_height / num_rows
                        target_height = height_per_row * 0.62  # Use 62% of space
                        
                        # Calculate width similarly
                        available_width = subplot_settings['right'] - subplot_settings['left']
                        width_per_col = available_width / num_cols
                        target_width = width_per_col * 0.83  # Use 83% of space
                        
                        # Center the subplot in its allocated space
                        center_x = subplot_settings['left'] + (col + 0.5) * width_per_col
                        center_y = adjusted_bottom + (num_rows - row - 0.5) * height_per_row
                        new_x = center_x - target_width / 2
                        new_y = center_y - target_height / 2
                        
                        logger.debug(f"Row {row}, Col {col} - Smart sizing: {target_width:.3f}x{target_height:.3f} at ({new_x:.3f}, {new_y:.3f})")
                        ax.set_position([new_x, new_y, target_width, target_height])
                    elif num_rows <= 2:
                        # For 1-2 rows, expand slightly for better appearance
                        scale_factor = 1.08
                        new_width = pos.width * scale_factor
                        new_height = pos.height * scale_factor
                        logger.debug(f"Row {row}, Col {col} - Expanding: {new_width:.3f}x{new_height:.3f}")
                        ax.set_position([pos.x0, pos.y0, new_width, new_height])
                    else:
                        # No smart scaling - keep matplotlib's default (will overlap)
                        logger.debug(f"Row {row}, Col {col} - Using default sizing (no adjustment)")
                    if self.first_year != self.last_year:
                        title = f"{place} ({self.first_year}-{self.last_year})"
                    else:
                        title = f"{place} ({self.first_year})"
                    self.subplot_polar(df=df_place, ax=ax, cbar=False, title=title, num_rows=num_rows)
                else:
                    ax.axis('off')  # Hide unused subplots

        self.add_dual_colourbars(fig)

        page_settings = settings['page']
        plt.figtext(0.05, 0.03, credit, verticalalignment='center', horizontalalignment='left', fontsize=page_settings['label_fontsize'])
        plt.figtext(0.93, 0.03, data_source, verticalalignment='center', horizontalalignment='right', fontsize=page_settings['label_fontsize'])
        if show_plot:
            plt.show()
        fig.savefig(save_file, dpi=page_settings['dpi'], bbox_inches="tight")
        plt.close(fig)
