# Test Visualizer class and related data handling
import pytest
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for tests
from geo_plot.visualizer import Visualizer  # noqa: E402


def test_temp_c_to_f():
    assert Visualizer.temp_c_to_f(0) == 32.0
    assert Visualizer.temp_c_to_f(100) == 212.0
    assert Visualizer.temp_c_to_f(-40) == -40.0


def test_add_data_fields():
    df = pd.DataFrame({'date': ['2025-01-01', '2025-01-02'], 'temp_C': [10, 12]})
    vis = Visualizer(df)
    df2 = vis.add_data_fields(df.copy())
    assert 'day_of_year' in df2.columns
    assert 'angle' in df2.columns
    assert df2['day_of_year'].iloc[0] == 1
    assert df2['angle'].iloc[0] == 0.0


def test_add_data_fields_missing_columns():
    df = pd.DataFrame({'date': ['2025-01-01', '2025-01-02'], 'temp_C': [10, 12]})
    vis = Visualizer(df)
    assert 'day_of_year' in vis.df.columns
    assert 'angle' in vis.df.columns
    df2 = df.copy()
    df2['day_of_year'] = [1, 2]
    df2['angle'] = [0.0, 0.1]
    vis2 = Visualizer(df2)
    assert (vis2.df['day_of_year'] == [1, 2]).all()
    assert (vis2.df['angle'] == [0.0, 0.1]).all()


def test_add_data_fields_empty_df():
    with pytest.raises(ValueError):
        Visualizer(pd.DataFrame())


def test_add_data_fields_invalid_date():
    df = pd.DataFrame({'date': ['not-a-date'], 'temp_C': [10]})
    with pytest.raises(Exception):
        Visualizer(df)


def test_visualizer_init_and_error():
    df = pd.DataFrame({'date': ['2025-01-01'], 'temp_C': [10]})
    vis = Visualizer(df)
    assert vis.df is not None
    assert vis.tmin_c == 10
    assert vis.tmax_c == 10
    with pytest.raises(ValueError):
        Visualizer(pd.DataFrame())


def test_visualizer_temp_c_to_f_edge_cases():
    assert Visualizer.temp_c_to_f(37.5) == 99.5
    assert Visualizer.temp_c_to_f(-273.15) == pytest.approx(-459.67, abs=0.01)


def test_plot_polar_subplots_single_place(tmp_path):
    """Test plot_polar_subplots with a single place (1x1 grid)."""
    # Create test data for a single place
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=365),
        'temp_C': [20 + 5 * (i % 30) / 30 for i in range(365)],
        'place_name': ['Test Place'] * 365
    })

    vis = Visualizer(df)
    output_file = tmp_path / "single_place.png"

    # Should not raise error for 1x1 grid
    vis.plot_polar_subplots(
        title="Single Place Test",
        save_file=str(output_file),
        num_rows=1,
        num_cols=1,
        show_plot=False
    )

    assert output_file.exists()


def test_plot_polar_basic(tmp_path):
    """Test basic plot_polar functionality."""
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=100),
        'temp_C': [20] * 100
    })

    vis = Visualizer(df)
    output_file = tmp_path / "basic_polar.png"

    vis.plot_polar(title="Basic Test", save_file=str(output_file), show_plot=False)

    assert output_file.exists()


def test_plot_polar_with_range(tmp_path):
    """Test plot_polar with varying temperatures."""
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=365),
        'temp_C': [10 + 20 * (i / 365) for i in range(365)]
    })

    vis = Visualizer(df)
    output_file = tmp_path / "range_polar.png"

    vis.plot_polar(title="Range Test", save_file=str(output_file), show_plot=False)

    assert output_file.exists()
    # Check temp range was calculated
    assert vis.tmin_c < vis.tmax_c


def test_plot_polar_year_colour_mode(tmp_path):
    """Test plot_polar with year-based colouring."""
    df = pd.DataFrame({
        'date': pd.date_range('2020-01-01', periods=730),
        'temp_C': [15 + 10 * (i % 365) / 365 for i in range(730)]
    })

    vis = Visualizer(df, colour_mode='year')
    output_file = tmp_path / "year_colour_mode.png"

    vis.plot_polar(title="Year Colour Test", save_file=str(output_file), show_plot=False)

    assert output_file.exists()
    assert vis.colour_mode == 'year'


def test_visualizer_invalid_colour_mode():
    df = pd.DataFrame({'date': ['2025-01-01'], 'temp_C': [10]})
    with pytest.raises(ValueError):
        Visualizer(df, colour_mode='invalid')


def test_visualizer_invalid_colormap_name():
    df = pd.DataFrame({'date': ['2025-01-01'], 'temp_C': [10]})
    with pytest.raises(ValueError):
        Visualizer(df, colormap_name='not_a_cmap')


def test_visualizer_invalid_plot_format():
    df = pd.DataFrame({'date': ['2025-01-01'], 'temp_C': [10]})
    with pytest.raises(ValueError):
        Visualizer(df, plot_format='invalid')


def test_visualizer_range_text_template_formatting():
    df = pd.DataFrame({
        'date': ['2025-01-01', '2025-01-02'],
        'temp_C': [10, 12],
        'precip_mm': [1.25, 4.75],
    })
    vis = Visualizer(
        df,
        y_value_column='precip_mm',
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={
            'measure_label': 'Daily Precipitation',
            'measure_unit': 'mm',
        }
    )
    min_value, max_value = vis._get_range_bounds(df)
    text = vis._format_range_text(min_value, max_value)
    assert text == "Daily Precipitation: 1.2-4.8 mm"


def test_visualizer_precipitation_max_daily_range_text_placeholders():
    df = pd.DataFrame({
        'date': ['2025-01-01', '2025-01-02'],
        'wet_hours_per_day': [3, 8],
        'precip_mm': [12.7, 25.4],
    })
    vis = Visualizer(
        df,
        y_value_column='wet_hours_per_day',
        range_text_template="Max daily precipitation: {max_daily_precip_mm:.1f} mm ({max_daily_precip_in:.2f} in)",
        range_text_context={
            'measure_label': 'Wet Hours per Day',
            'measure_unit': 'h',
        }
    )
    min_value, max_value = vis._get_range_bounds(df)
    text = vis._format_range_text(min_value, max_value, df=df)
    assert text == "Max daily precipitation: 25.4 mm (1.00 in)"


def test_visualizer_range_text_template_missing_context_raises():
    df = pd.DataFrame({'date': ['2025-01-01'], 'temp_C': [10]})
    vis = Visualizer(
        df,
        range_text_template="{missing_key}",
        range_text_context={'measure_label': 'Any Value', 'measure_unit': 'u'},
    )
    with pytest.raises(ValueError):
        vis._format_range_text(1.0, 2.0)


def test_visualizer_uses_colour_value_column_for_colour_value_mode_colours():
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=3),
        'wet_hours_per_day': [2.0, 2.0, 2.0],
        'max_hourly_precip_mm': [0.1, 1.0, 5.0],
    })
    vis = Visualizer(
        df,
        y_value_column='wet_hours_per_day',
        colour_value_column='max_hourly_precip_mm',
        colour_mode='colour_value',
        colourbar_title='mm',
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Wet Hours per Day', 'measure_unit': 'h'},
    )

    colours = vis.get_point_colours(vis.df)
    assert len({tuple(row) for row in colours}) > 1


def test_visualizer_y_value_mode_colours_by_y_value_column():
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=3),
        'wet_hours_per_day': [2.0, 2.0, 2.0],
        'max_hourly_precip_mm': [0.1, 1.0, 5.0],
    })
    vis = Visualizer(
        df,
        y_value_column='wet_hours_per_day',
        colour_value_column='max_hourly_precip_mm',
        colour_mode='y_value',
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Wet Hours per Day', 'measure_unit': 'h'},
    )

    colours = vis.get_point_colours(vis.df)
    assert len({tuple(row) for row in colours}) == 1


def test_visualizer_requires_range_text_template():
    df = pd.DataFrame({'date': ['2025-01-01'], 'temp_C': [10]})
    with pytest.raises(ValueError):
        Visualizer(df, range_text_template="")


def test_plot_polar_precipitation_without_temp_column(tmp_path):
    """Visualizer should support non-temperature measures via y_value_column."""
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=30),
        'precip_mm': [float(i % 7) for i in range(30)],
    })

    vis = Visualizer(
        df,
        y_value_column='precip_mm',
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Daily Precipitation', 'measure_unit': 'mm'},
    )
    output_file = tmp_path / "precip_polar.png"

    vis.plot_polar(title="Precipitation Test", save_file=str(output_file), show_plot=False)

    assert output_file.exists()


def test_create_polar_plot_wedges_uses_bar(tmp_path):
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=30),
        'precip_mm': [float(i % 7) for i in range(30)],
    })
    vis = Visualizer(
        df,
        y_value_column='precip_mm',
        plot_format='wedges',
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Daily Precipitation', 'measure_unit': 'mm'},
    )

    fig = matplotlib.pyplot.figure()
    ax = fig.add_subplot(111, polar=True)
    vis.create_polar_plot(ax, vis.df)

    assert len(ax.patches) > 0
    matplotlib.pyplot.close(fig)


def test_create_polar_plot_wedge_width_scale_changes_patch_width(tmp_path):
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=30),
        'precip_mm': [float(i % 7) for i in range(30)],
    })

    vis_default = Visualizer(
        df,
        y_value_column='precip_mm',
        plot_format='wedges',
        wedge_width_scale=1.0,
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Daily Precipitation', 'measure_unit': 'mm'},
    )
    vis_wide = Visualizer(
        df,
        y_value_column='precip_mm',
        plot_format='wedges',
        wedge_width_scale=1.5,
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Daily Precipitation', 'measure_unit': 'mm'},
    )

    fig_default = matplotlib.pyplot.figure()
    ax_default = fig_default.add_subplot(111, polar=True)
    vis_default.create_polar_plot(ax_default, vis_default.df)
    default_width = ax_default.patches[0].get_width()

    fig_wide = matplotlib.pyplot.figure()
    ax_wide = fig_wide.add_subplot(111, polar=True)
    vis_wide.create_polar_plot(ax_wide, vis_wide.df)
    wide_width = ax_wide.patches[0].get_width()

    assert wide_width > default_width
    matplotlib.pyplot.close(fig_default)
    matplotlib.pyplot.close(fig_wide)


def test_visualizer_accepts_wedges_plot_format_alias():
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=5),
        'wet_hours_per_day': [1.0, 2.0, 3.0, 2.0, 1.0],
    })
    vis = Visualizer(
        df,
        y_value_column='wet_hours_per_day',
        plot_format='wedges',
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Wet Hours per Day', 'measure_unit': 'hours'},
    )
    assert vis.plot_format == 'wedges'


def test_visualizer_rejects_non_positive_wedge_width_scale():
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=2),
        'precip_mm': [0.0, 1.0],
    })
    with pytest.raises(ValueError):
        Visualizer(
            df,
            y_value_column='precip_mm',
            plot_format='wedges',
            wedge_width_scale=0,
            range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
            range_text_context={'measure_label': 'Daily Precipitation', 'measure_unit': 'mm'},
        )


def test_prepare_render_df_sorts_wet_hours_by_precipitation_within_angle():
    df = pd.DataFrame({
        'date': pd.to_datetime(['2025-01-01', '2024-01-01', '2023-01-01']),
        'wet_hours_per_day': [6, 6, 6],
        'precip_mm': [5.0, 1.0, 10.0],
    })
    vis = Visualizer(
        df,
        y_value_column='wet_hours_per_day',
        colour_value_column='precip_mm',
        colour_mode='colour_value',
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Wet Hours per Day', 'measure_unit': 'h'},
    )

    prepared = vis._prepare_render_df(vis.df)
    assert prepared['precip_mm'].tolist() == [1.0, 5.0, 10.0]


def test_prepare_render_df_noop_for_non_wet_hours_measure():
    df = pd.DataFrame({
        'date': pd.to_datetime(['2025-01-01', '2025-01-02']),
        'precip_mm': [5.0, 1.0],
    })
    vis = Visualizer(
        df,
        y_value_column='precip_mm',
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Daily Precipitation', 'measure_unit': 'mm'},
    )

    prepared = vis._prepare_render_df(vis.df)
    assert prepared['precip_mm'].tolist() == vis.df['precip_mm'].tolist()


def test_visualizer_accepts_custom_y_step():
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=5),
        'precip_mm': [0.0, 1.0, 2.0, 3.0, 4.0],
    })
    vis = Visualizer(
        df,
        y_value_column='precip_mm',
        y_step=0.5,
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Daily Precipitation', 'measure_unit': 'mm'},
    )
    assert vis.y_step == 0.5


def test_visualizer_rejects_non_positive_y_step():
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=2),
        'precip_mm': [0.0, 1.0],
    })
    with pytest.raises(ValueError):
        Visualizer(
            df,
            y_value_column='precip_mm',
            y_step=0,
            range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
            range_text_context={'measure_label': 'Daily Precipitation', 'measure_unit': 'mm'},
        )


def test_visualizer_limits_y_circles_with_max_y_steps():
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=30),
        'wet_hours_per_day': [float(i % 25) for i in range(30)],
    })

    vis = Visualizer(
        df,
        y_value_column='wet_hours_per_day',
        y_step=2,
        max_y_steps=4,
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Wet Hours per Day', 'measure_unit': 'hours'},
    )

    fig = matplotlib.pyplot.figure()
    ax = fig.add_subplot(111, polar=True)
    vis.create_polar_plot(ax, vis.df)

    assert len(ax.lines) <= 4
    matplotlib.pyplot.close(fig)


def test_visualizer_precipitation_adds_dual_metric_and_imperial_colourbars():
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=3),
        'wet_hours_per_day': [2.0, 4.0, 6.0],
        'max_hourly_precip_mm': [0.5, 5.0, 10.0],
    })

    vis = Visualizer(
        df,
        y_value_column='wet_hours_per_day',
        colour_value_column='max_hourly_precip_mm',
        colour_mode='colour_value',
        colourbar_title='mm/hr',
        range_text_template="{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}",
        range_text_context={'measure_label': 'Wet Hours per Day', 'measure_unit': 'h'},
    )

    fig = matplotlib.pyplot.figure()
    vis.add_dual_colourbars(fig)

    titles = [ax.get_title() for ax in fig.axes if ax.get_title()]
    assert 'mm/hr' in titles
    assert 'in/hr' in titles

    matplotlib.pyplot.close(fig)


def test_plot_polar_subplots_uses_subplot_title_template(tmp_path, monkeypatch):
    df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=4),
        'temp_C': [10.0, 11.0, 12.0, 13.0],
        'place_name': ['City A', 'City A', 'City B', 'City B'],
    })
    vis = Visualizer(df)
    output_file = tmp_path / "subplot_title_template.png"

    captured_titles: list[str] = []

    def _capture_subplot_title(self, ax, df, cbar=False, title="", num_rows=1):
        captured_titles.append(title)

    monkeypatch.setattr(Visualizer, "subplot_polar", _capture_subplot_title)

    vis.plot_polar_subplots(
        subplot_field="place_name",
        subplot_title_template="{location}",
        subplot_title_context={'start_year': 2025, 'end_year': 2025, 'year_range': '2025'},
        num_rows=1,
        num_cols=2,
        save_file=str(output_file),
        show_plot=False,
    )

    assert output_file.exists()
    assert captured_titles == ['City A', 'City B']
