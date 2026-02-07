"""
Tests for orchestrator module (plot coordination and batching).
"""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pandas as pd

from orchestrator import (
    calculate_grid_dimensions,
    create_batch_subplot,
    create_individual_plot,
    create_main_plots,
    plot_all
)
from cds import Location


def test_calculate_grid_dimensions_no_custom_grid():
    """Test automatic grid dimension calculation."""
    # Single place
    rows, cols, total = calculate_grid_dimensions(1, None)
    assert rows == 1 and cols == 1 and total == 1
    
    # 4 places
    rows, cols, total = calculate_grid_dimensions(4, None)
    assert rows == 2 and cols == 2 and total == 4
    
    # 6 places
    rows, cols, total = calculate_grid_dimensions(6, None)
    assert rows == 2 and cols == 3 and total == 6
    
    # 10 places (auto-calculated total equals num_places, not grid size)
    rows, cols, total = calculate_grid_dimensions(10, None)
    assert rows == 3 and cols == 4 and total == 10


def test_calculate_grid_dimensions_with_custom_grid():
    """Test grid dimensions with custom grid specification."""
    # Custom 3x4 grid
    rows, cols, total = calculate_grid_dimensions(8, (3, 4))
    assert rows == 3 and cols == 4 and total == 12
    
    # Custom 2x5 grid
    rows, cols, total = calculate_grid_dimensions(7, (2, 5))
    assert rows == 2 and cols == 5 and total == 10


def test_calculate_grid_dimensions_zero_places():
    """Test handling of zero places."""
    rows, cols, total = calculate_grid_dimensions(0, None)
    assert rows == 1 and cols == 1 and total == 0


@patch('orchestrator.Visualizer')
def test_create_batch_subplot_single_batch(mock_visualizer_class, tmp_path):
    """Test creating a single batch subplot."""
    # Setup
    df_batch = pd.DataFrame({
        'place_name': ['City A', 'City B'],
        'temp_C': [10.0, 15.0]
    })
    loc1 = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    loc2 = Location(name="City B", lat=51.5, lon=-0.1, tz="Europe/London")
    batch_places = [loc1, loc2]
    
    mock_vis_instance = MagicMock()
    mock_visualizer_class.return_value = mock_vis_instance
    
    # Execute
    result = create_batch_subplot(
        df_batch=df_batch,
        batch_places=batch_places,
        batch_idx=0,
        num_batches=1,
        batch_rows=1,
        batch_cols=2,
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        settings=Path("settings.yaml"),
        t_min_c=5.0,
        t_max_c=20.0,
        scale_height=True
    )
    
    # Verify
    assert "Overall_noon_temps_polar_2024_2024.png" in result
    mock_visualizer_class.assert_called_once()
    mock_vis_instance.plot_polar_subplots.assert_called_once()
    call_kwargs = mock_vis_instance.plot_polar_subplots.call_args[1]
    assert "Mid-Day Temperatures (2024-2024)" in call_kwargs['title']
    assert call_kwargs['num_rows'] == 1
    assert call_kwargs['num_cols'] == 2


@patch('orchestrator.Visualizer')
def test_create_batch_subplot_multiple_batches(mock_visualizer_class, tmp_path):
    """Test creating a batch subplot when there are multiple batches."""
    df_batch = pd.DataFrame({'place_name': ['City A'], 'temp_C': [10.0]})
    loc = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    
    mock_vis_instance = MagicMock()
    mock_visualizer_class.return_value = mock_vis_instance
    
    result = create_batch_subplot(
        df_batch=df_batch,
        batch_places=[loc],
        batch_idx=1,
        num_batches=3,
        batch_rows=2,
        batch_cols=2,
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        settings=Path("settings.yaml"),
        t_min_c=5.0,
        t_max_c=20.0,
        scale_height=True
    )
    
    # Should include batch suffix
    assert "_part2of3.png" in result
    call_kwargs = mock_vis_instance.plot_polar_subplots.call_args[1]
    assert "Part 2/3" in call_kwargs['title']


@patch('orchestrator.Visualizer')
def test_create_individual_plot(mock_visualizer_class, tmp_path):
    """Test creating an individual location plot."""
    df = pd.DataFrame({
        'place_name': ['Austin, TX'],
        'temp_C': [25.5],
        'date': ['2024-01-01']
    })
    loc = Location(name="Austin, TX", lat=30.27, lon=-97.74, tz="America/Chicago")
    
    mock_vis_instance = MagicMock()
    mock_visualizer_class.return_value = mock_vis_instance
    
    result = create_individual_plot(
        loc=loc,
        df=df,
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        settings=Path("settings.yaml"),
        t_min_c=5.0,
        t_max_c=35.0
    )
    
    # Verify filename format (spaces and commas removed)
    assert "Austin_TX_noon_temps_polar_2024_2024.png" in result
    mock_visualizer_class.assert_called_once()
    mock_vis_instance.plot_polar.assert_called_once()
    
    call_kwargs = mock_vis_instance.plot_polar.call_args[1]
    assert "Austin, TX" in call_kwargs['title']
    assert call_kwargs['layout'] == 'polar_single'
    assert call_kwargs['show_plot'] is False


@patch('orchestrator.create_batch_subplot')
@patch('orchestrator.calculate_grid_layout')
def test_create_main_plots_single_batch(mock_grid_layout, mock_create_batch, tmp_path):
    """Test creating main plots with a single batch."""
    mock_grid_layout.return_value = (2, 2)
    mock_create_batch.return_value = str(tmp_path / "plot.png")
    
    df = pd.DataFrame({
        'place_name': ['City A', 'City B'],
        'temp_C': [10.0, 15.0]
    })
    loc1 = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    loc2 = Location(name="City B", lat=51.5, lon=-0.1, tz="Europe/London")
    
    result = create_main_plots(
        df_overall=df,
        place_list=[loc1, loc2],
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        settings=Path("settings.yaml"),
        t_min_c=5.0,
        t_max_c=20.0,
        grid=None,
        scale_height=True
    )
    
    assert len(result) == 1
    mock_create_batch.assert_called_once()


@patch('orchestrator.create_batch_subplot')
def test_create_main_plots_multiple_batches(mock_create_batch, tmp_path):
    """Test creating main plots split into multiple batches."""
    mock_create_batch.side_effect = [
        str(tmp_path / "plot1.png"),
        str(tmp_path / "plot2.png")
    ]
    
    df = pd.DataFrame({
        'place_name': ['A', 'B', 'C', 'D', 'E'],
        'temp_C': [10.0, 12.0, 14.0, 16.0, 18.0]
    })
    places = [
        Location(name="A", lat=40.0, lon=-73.0, tz="America/New_York"),
        Location(name="B", lat=41.0, lon=-74.0, tz="America/New_York"),
        Location(name="C", lat=42.0, lon=-75.0, tz="America/New_York"),
        Location(name="D", lat=43.0, lon=-76.0, tz="America/New_York"),
        Location(name="E", lat=44.0, lon=-77.0, tz="America/New_York"),
    ]
    
    # Fixed grid 2x2 = 4 places per batch, so 5 places needs 2 batches
    result = create_main_plots(
        df_overall=df,
        place_list=places,
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        settings=Path("settings.yaml"),
        t_min_c=10.0,
        t_max_c=18.0,
        grid=(2, 2),
        scale_height=True
    )
    
    assert len(result) == 2
    assert mock_create_batch.call_count == 2


@patch('orchestrator.Visualizer')
@patch('orchestrator.create_individual_plot')
@patch('orchestrator.create_main_plots')
def test_plot_all_no_show(mock_create_main, mock_create_individual, mock_visualizer_class, tmp_path):
    """Test plot_all without showing plots."""
    mock_create_main.return_value = [str(tmp_path / "main.png")]
    mock_create_individual.return_value = str(tmp_path / "individual.png")
    
    df = pd.DataFrame({
        'place_name': ['City A'],
        'temp_C': [10.0]
    })
    loc = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    
    plot_all(
        df_overall=df,
        place_list=[loc],
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        settings=Path("settings.yaml"),
        show_main=False,
        show_individual=False,
        scale_height=True,
        grid=None
    )
    
    mock_create_main.assert_called_once()
    mock_create_individual.assert_called_once()
    # show_saved_plots should not be called when both show flags are False
    mock_visualizer_class.show_saved_plots.assert_not_called()


@patch('orchestrator.Visualizer')
@patch('orchestrator.create_individual_plot')
@patch('orchestrator.create_main_plots')
def test_plot_all_show_main(mock_create_main, mock_create_individual, mock_visualizer_class, tmp_path):
    """Test plot_all with showing main plot."""
    main_plot = str(tmp_path / "main.png")
    individual_plot = str(tmp_path / "individual.png")
    
    mock_create_main.return_value = [main_plot]
    mock_create_individual.return_value = individual_plot
    
    df = pd.DataFrame({
        'place_name': ['City A'],
        'temp_C': [10.0]
    })
    loc = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    
    plot_all(
        df_overall=df,
        place_list=[loc],
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        settings=Path("settings.yaml"),
        show_main=True,
        show_individual=False,
        scale_height=True,
        grid=None
    )
    
    # Should show only main plot
    mock_visualizer_class.show_saved_plots.assert_called_once_with([main_plot])


@patch('orchestrator.Visualizer')
@patch('orchestrator.create_individual_plot')
@patch('orchestrator.create_main_plots')
def test_plot_all_show_all(mock_create_main, mock_create_individual, mock_visualizer_class, tmp_path):
    """Test plot_all with showing all plots."""
    main_plot = str(tmp_path / "main.png")
    individual_plot = str(tmp_path / "individual.png")
    
    mock_create_main.return_value = [main_plot]
    mock_create_individual.return_value = individual_plot
    
    df = pd.DataFrame({
        'place_name': ['City A'],
        'temp_C': [10.0]
    })
    loc = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    
    plot_all(
        df_overall=df,
        place_list=[loc],
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        settings=Path("settings.yaml"),
        show_main=True,
        show_individual=True,
        scale_height=True,
        grid=None
    )
    
    # Should show both main and individual plots
    mock_visualizer_class.show_saved_plots.assert_called_once_with([main_plot, individual_plot])


@patch('orchestrator.Visualizer')
@patch('orchestrator.create_individual_plot')
@patch('orchestrator.create_main_plots')
def test_plot_all_multiple_locations(mock_create_main, mock_create_individual, mock_visualizer_class, tmp_path):
    """Test plot_all with multiple locations."""
    mock_create_main.return_value = [str(tmp_path / "main.png")]
    mock_create_individual.side_effect = [
        str(tmp_path / "city1.png"),
        str(tmp_path / "city2.png")
    ]
    
    df = pd.DataFrame({
        'place_name': ['City A', 'City B'],
        'temp_C': [10.0, 15.0]
    })
    loc1 = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    loc2 = Location(name="City B", lat=51.5, lon=-0.1, tz="Europe/London")
    
    plot_all(
        df_overall=df,
        place_list=[loc1, loc2],
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        settings=Path("settings.yaml"),
        show_main=False,
        show_individual=False,
        scale_height=True,
        grid=None
    )
    
    # Should create individual plot for each location
    assert mock_create_individual.call_count == 2
# Unit testing these functions would require either:
# - Significant refactoring to inject dependencies
# - Complex mock setups that are brittle and hard to maintain
# - Integration test fixtures with real plotting libraries
#
# The calculate_grid_dimensions function above tests the core logic
# that drives the orchestration behavior.
