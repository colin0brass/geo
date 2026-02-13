# Test Visualizer class and related data handling
import pytest
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for tests
from plot import Visualizer


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
