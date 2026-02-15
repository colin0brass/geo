"""
Tests for orchestrator module (plot coordination and batching).
"""
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd

from geo_plot.orchestrator import (
    PlotOrchestrator,
    PlotRunContext,
    plot_all
)
from geo_data.cds_base import Location


def test_calculate_grid_dimensions_no_custom_grid(tmp_path):
    """Test automatic grid dimension calculation."""
    # Create config file with grid config
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 4\n"
        "  max_auto_cols: 6\n"
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
        "      range_text: '{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)'\n"
    )

    orchestrator = PlotOrchestrator(config=config_file, settings=Path("geo_plot/settings.yaml"))

    # Single place: grid 1x1, max capacity 24 (4x6)
    rows, cols, max_capacity = orchestrator.calculate_grid_dimensions(1, None)
    assert rows == 1 and cols == 1 and max_capacity == 24

    # 4 places: grid 2x2, max capacity 24
    rows, cols, max_capacity = orchestrator.calculate_grid_dimensions(4, None)
    assert rows == 2 and cols == 2 and max_capacity == 24

    # 6 places: grid 2x3, max capacity 24
    rows, cols, max_capacity = orchestrator.calculate_grid_dimensions(6, None)
    assert rows == 2 and cols == 3 and max_capacity == 24

    # 10 places: grid 3x4, max capacity 24 (with 4x6 max)
    rows, cols, max_capacity = orchestrator.calculate_grid_dimensions(10, None)
    assert rows == 3 and cols == 4 and max_capacity == 24


def test_calculate_grid_dimensions_with_custom_grid(tmp_path):
    """Test grid dimensions with custom grid specification."""
    # Create dummy config file (not used when grid is specified)
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 4\n"
        "  max_auto_cols: 6\n"
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
        "      range_text: '{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)'\n"
    )

    orchestrator = PlotOrchestrator(config=config_file, settings=Path("geo_plot/settings.yaml"))

    # Custom 3x4 grid
    rows, cols, total = orchestrator.calculate_grid_dimensions(8, (3, 4))
    assert rows == 3 and cols == 4 and total == 12

    # Custom 2x5 grid
    rows, cols, total = orchestrator.calculate_grid_dimensions(7, (2, 5))
    assert rows == 2 and cols == 5 and total == 10


def test_calculate_grid_dimensions_zero_places(tmp_path):
    """Test handling of zero places."""
    # Create config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 4\n"
        "  max_auto_cols: 6\n"
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
        "      range_text: '{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)'\n"
    )

    orchestrator = PlotOrchestrator(config=config_file, settings=Path("geo_plot/settings.yaml"))

    # With 0 places, grid is 1x1 but max capacity is still 24 (4x6)
    rows, cols, max_capacity = orchestrator.calculate_grid_dimensions(0, None)
    assert rows == 1 and cols == 1 and max_capacity == 24


@patch('geo_plot.orchestrator.Visualizer')
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
    orchestrator = PlotOrchestrator(
        config=Path("config.yaml"),
        settings=Path("geo_plot/settings.yaml"),
        colour_mode='year',
        colormap_name='plasma',
    )
    run_ctx = PlotRunContext(
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        t_min_c=5.0,
        t_max_c=20.0,
    )

    # Execute
    result = orchestrator.create_batch_subplot(
        df_batch=df_batch,
        batch_places=batch_places,
        batch_idx=0,
        num_batches=1,
        batch_rows=1,
        batch_cols=2,
        run_ctx=run_ctx,
    )

    # Verify
    assert "Overall_noon_temperature_2024_2024.png" in result
    mock_visualizer_class.assert_called_once()
    assert mock_visualizer_class.call_args[1]['colour_mode'] == 'year'
    assert mock_visualizer_class.call_args[1]['colormap_name'] == 'plasma'
    assert mock_visualizer_class.call_args[1]['y_value_column'] == 'temp_C'
    assert 'range_text_template' in mock_visualizer_class.call_args[1]
    assert 'range_text_context' in mock_visualizer_class.call_args[1]
    mock_vis_instance.plot_polar_subplots.assert_called_once()
    call_kwargs = mock_vis_instance.plot_polar_subplots.call_args[1]
    assert "Mid-Day Temperature (2024-2024)" in call_kwargs['title']
    assert call_kwargs['num_rows'] == 1
    assert call_kwargs['num_cols'] == 2


@patch('geo_plot.orchestrator.Visualizer')
def test_create_batch_subplot_multiple_batches(mock_visualizer_class, tmp_path):
    """Test creating a batch subplot when there are multiple batches."""
    df_batch = pd.DataFrame({'place_name': ['City A'], 'temp_C': [10.0]})
    loc = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")

    mock_vis_instance = MagicMock()
    mock_visualizer_class.return_value = mock_vis_instance
    orchestrator = PlotOrchestrator(
        config=Path("config.yaml"),
        settings=Path("geo_plot/settings.yaml"),
    )
    run_ctx = PlotRunContext(
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        t_min_c=5.0,
        t_max_c=20.0,
    )

    result = orchestrator.create_batch_subplot(
        df_batch=df_batch,
        batch_places=[loc],
        batch_idx=1,
        num_batches=3,
        batch_rows=2,
        batch_cols=2,
        run_ctx=run_ctx,
    )

    # Should include batch suffix
    assert "_part2of3.png" in result
    call_kwargs = mock_vis_instance.plot_polar_subplots.call_args[1]
    assert "Part 2/3" in call_kwargs['title']


@patch('geo_plot.orchestrator.Visualizer')
def test_create_batch_subplot_uses_measure_labels_from_config(mock_visualizer_class, tmp_path):
    """Test measure label text is loaded from plotting.measure_labels config."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "\n".join([
            "plot_text:",
            "  single_plot_title: '{location} {measure_label} ({start_year}-{end_year})'",
            "  subplot_title: '{measure_label} ({start_year}-{end_year})'",
            "  subplot_title_with_batch: '{measure_label} ({start_year}-{end_year}) - Part {batch}/{total_batches}'",
            "  single_plot_filename: '{location}_{measure_key}_{start_year}_{end_year}.png'",
            "  subplot_filename: '{list_name}_{measure_key}_{start_year}_{end_year}.png'",
            "  subplot_filename_with_batch: '{list_name}_{measure_key}_{start_year}_{end_year}_part{batch}of{total_batches}.png'",
            "  credit: 'Climate Data Analysis & Visualisation by Colin Osborne'",
            "  single_plot_credit: 'Analysis & visualisation by Colin Osborne'",
            "  data_source: 'Data from: ERA5 via CDS'",
            "plotting:",
            "  measure_labels:",
            "    daily_precipitation:",
            "      label: Rainfall",
            "      unit: mm/day",
            "      y_value_column: precip_mm",
            "      range_text: '{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}'",
        ])
    )

    df_batch = pd.DataFrame({'place_name': ['City A'], 'temp_C': [10.0]})
    loc = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")

    mock_vis_instance = MagicMock()
    mock_visualizer_class.return_value = mock_vis_instance
    orchestrator = PlotOrchestrator(
        config=config_file,
        settings=Path("geo_plot/settings.yaml"),
        measure='daily_precipitation',
    )
    run_ctx = PlotRunContext(
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        t_min_c=5.0,
        t_max_c=20.0,
    )

    orchestrator.create_batch_subplot(
        df_batch=df_batch,
        batch_places=[loc],
        batch_idx=0,
        num_batches=1,
        batch_rows=1,
        batch_cols=1,
        run_ctx=run_ctx,
    )

    call_kwargs = mock_vis_instance.plot_polar_subplots.call_args[1]
    assert "Rainfall (2024-2024)" in call_kwargs['title']
    vis_kwargs = mock_visualizer_class.call_args[1]
    assert vis_kwargs['y_value_column'] == 'precip_mm'
    assert vis_kwargs['range_text_template'] == '{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}'


@patch('geo_plot.orchestrator.Visualizer')
def test_orchestrator_passes_plot_format_to_visualizer(mock_visualizer_class, tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "\n".join([
            "plot_text:",
            "  single_plot_title: '{location} {measure_label} ({start_year}-{end_year})'",
            "  subplot_title: '{measure_label} ({start_year}-{end_year})'",
            "  subplot_title_with_batch: '{measure_label} ({start_year}-{end_year}) - Part {batch}/{total_batches}'",
            "  single_plot_filename: '{location}_{measure_key}_{start_year}_{end_year}.png'",
            "  subplot_filename: '{list_name}_{measure_key}_{start_year}_{end_year}.png'",
            "  subplot_filename_with_batch: '{list_name}_{measure_key}_{start_year}_{end_year}_part{batch}of{total_batches}.png'",
            "  credit: 'Climate Data Analysis & Visualisation by Colin Osborne'",
            "  single_plot_credit: 'Analysis & visualisation by Colin Osborne'",
            "  data_source: 'Data from: ERA5 via CDS'",
            "plotting:",
            "  measure_labels:",
            "    daily_precipitation:",
            "      label: Rainfall",
            "      unit: mm/day",
            "      plot_format: radial_wedges",
            "      wedge_width_scale: 1.5",
            "      max_y_steps: 4",
            "      y_value_column: precip_mm",
            "      range_text: '{measure_label}: {min_value:.1f}-{max_value:.1f} {measure_unit}'",
        ])
    )

    df_batch = pd.DataFrame({'place_name': ['City A'], 'precip_mm': [5.0]})
    loc = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")
    mock_visualizer_class.return_value = MagicMock()
    orchestrator = PlotOrchestrator(
        config=config_file,
        settings=Path("geo_plot/settings.yaml"),
        measure='daily_precipitation',
    )
    run_ctx = PlotRunContext(
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        t_min_c=0.0,
        t_max_c=20.0,
    )

    orchestrator.create_batch_subplot(
        df_batch=df_batch,
        batch_places=[loc],
        batch_idx=0,
        num_batches=1,
        batch_rows=1,
        batch_cols=1,
        run_ctx=run_ctx,
    )

    vis_kwargs = mock_visualizer_class.call_args[1]
    assert vis_kwargs['plot_format'] == 'radial_wedges'
    assert vis_kwargs['wedge_width_scale'] == 1.5
    assert vis_kwargs['max_y_steps'] == 4


@patch('geo_plot.orchestrator.Visualizer')
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
    orchestrator = PlotOrchestrator(
        config=Path("config.yaml"),
        settings=Path("geo_plot/settings.yaml"),
        colour_mode='year',
        colormap_name='plasma',
    )
    run_ctx = PlotRunContext(
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        t_min_c=5.0,
        t_max_c=35.0,
    )

    result = orchestrator.create_individual_plot(
        loc=loc,
        df=df,
        run_ctx=run_ctx,
    )

    # Verify filename format (spaces and commas removed)
    assert "Austin_TX_noon_temperature_2024_2024.png" in result
    mock_visualizer_class.assert_called_once()
    assert mock_visualizer_class.call_args[1]['colour_mode'] == 'year'
    assert mock_visualizer_class.call_args[1]['colormap_name'] == 'plasma'
    assert 'range_text_template' in mock_visualizer_class.call_args[1]
    assert 'range_text_context' in mock_visualizer_class.call_args[1]
    mock_vis_instance.plot_polar.assert_called_once()

    call_kwargs = mock_vis_instance.plot_polar.call_args[1]
    assert "Austin, TX" in call_kwargs['title']
    assert call_kwargs['layout'] == 'polar_single'
    assert call_kwargs['show_plot'] is False


@patch.object(PlotOrchestrator, 'create_batch_subplot')
@patch('geo_plot.orchestrator.calculate_grid_layout')
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

    # Create dummy config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 4\n"
        "  max_auto_cols: 6\n"
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
        "      range_text: '{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)'\n"
    )

    orchestrator = PlotOrchestrator(
        config=config_file,
        settings=Path("geo_plot/settings.yaml"),
    )
    run_ctx = PlotRunContext(
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        t_min_c=5.0,
        t_max_c=20.0,
    )

    result = orchestrator.create_main_plots(
        df_overall=df,
        place_list=[loc1, loc2],
        run_ctx=run_ctx,
        grid=None
    )

    assert len(result) == 1
    mock_create_batch.assert_called_once()


@patch.object(PlotOrchestrator, 'create_batch_subplot')
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

    # Create dummy config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 4\n"
        "  max_auto_cols: 6\n"
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
        "      range_text: '{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)'\n"
    )

    # Fixed grid 2x2 = 4 places per batch, so 5 places needs 2 batches
    orchestrator = PlotOrchestrator(
        config=config_file,
        settings=Path("geo_plot/settings.yaml"),
    )
    run_ctx = PlotRunContext(
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        t_min_c=10.0,
        t_max_c=18.0,
    )

    result = orchestrator.create_main_plots(
        df_overall=df,
        place_list=places,
        run_ctx=run_ctx,
        grid=(2, 2)
    )

    assert len(result) == 2
    assert mock_create_batch.call_count == 2


@patch('geo_plot.orchestrator.Visualizer')
@patch.object(PlotOrchestrator, 'create_individual_plot')
@patch.object(PlotOrchestrator, 'create_main_plots')
def test_plot_all_no_show(mock_create_main, mock_create_individual, mock_visualizer_class, tmp_path):
    """Test plot_all without showing plots."""
    mock_create_main.return_value = [str(tmp_path / "main.png")]
    mock_create_individual.return_value = str(tmp_path / "individual.png")

    df = pd.DataFrame({
        'place_name': ['City A'],
        'temp_C': [10.0]
    })
    loc = Location(name="City A", lat=40.0, lon=-73.0, tz="America/New_York")

    # Create dummy config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 4\n"
        "  max_auto_cols: 6\n"
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
        "      range_text: '{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)'\n"
    )

    plot_all(
        df_overall=df,
        place_list=[loc],
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        config=config_file,
        settings=Path("geo_plot/settings.yaml"),
        show_main=False,
        show_individual=False,
        grid=None
    )

    # Single place: should create individual plot only, not combined
    mock_create_main.assert_not_called()
    mock_create_individual.assert_called_once()
    # show_saved_plots should not be called when both show flags are False
    mock_visualizer_class.show_saved_plots.assert_not_called()


@patch('geo_plot.orchestrator.Visualizer')
@patch.object(PlotOrchestrator, 'create_individual_plot')
@patch.object(PlotOrchestrator, 'create_main_plots')
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

    # Create dummy config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 4\n"
        "  max_auto_cols: 6\n"
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
        "      range_text: '{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)'\n"
    )

    plot_all(
        df_overall=df,
        place_list=[loc],
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        config=config_file,
        settings=Path("geo_plot/settings.yaml"),
        show_main=True,
        show_individual=False,
        grid=None
    )

    # Single place: should create and show individual plot only
    mock_visualizer_class.show_saved_plots.assert_called_once_with([individual_plot])


@patch('geo_plot.orchestrator.Visualizer')
@patch.object(PlotOrchestrator, 'create_individual_plot')
@patch.object(PlotOrchestrator, 'create_main_plots')
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

    # Create dummy config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 4\n"
        "  max_auto_cols: 6\n"
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
        "      range_text: '{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)'\n"
    )

    plot_all(
        df_overall=df,
        place_list=[loc],
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        config=config_file,
        settings=Path("geo_plot/settings.yaml"),
        show_main=True,
        show_individual=True,
        grid=None
    )

    # Single place: should create and show individual plot only (combined would be redundant)
    mock_visualizer_class.show_saved_plots.assert_called_once_with([individual_plot])


@patch('geo_plot.orchestrator.Visualizer')
@patch.object(PlotOrchestrator, 'create_individual_plot')
@patch.object(PlotOrchestrator, 'create_main_plots')
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

    # Create dummy config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 4\n"
        "  max_auto_cols: 6\n"
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
        "      range_text: '{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)'\n"
    )

    plot_all(
        df_overall=df,
        place_list=[loc1, loc2],
        start_year=2024,
        end_year=2024,
        out_dir=tmp_path,
        config=config_file,
        settings=Path("geo_plot/settings.yaml"),
        show_main=False,
        show_individual=False,
        grid=None,
        colour_mode='year',
        colormap_name='plasma'
    )

    # Multiple places: should create combined plot only, no individual plots
    mock_create_main.assert_called_once()
    assert mock_create_main.call_args[1]['grid'] is None
    assert 'run_ctx' in mock_create_main.call_args[1]
    mock_create_individual.assert_not_called()


@patch.object(PlotOrchestrator, 'create_individual_plot')
def test_plot_all_daily_precipitation_uses_measure_column(mock_create_individual, tmp_path):
    """plot_all should compute ranges from configured y_value_column, not hard-coded temp_C."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "\n".join([
            "plot_text:",
            "  single_plot_title: '{location} {measure_label} ({start_year}-{end_year})'",
            "  subplot_title: '{measure_label} ({start_year}-{end_year})'",
            "  subplot_title_with_batch: '{measure_label} ({start_year}-{end_year}) - Part {batch}/{total_batches}'",
            "  single_plot_filename: '{location}_{measure_key}_{start_year}_{end_year}.png'",
            "  subplot_filename: '{list_name}_{measure_key}_{start_year}_{end_year}.png'",
            "  subplot_filename_with_batch: '{list_name}_{measure_key}_{start_year}_{end_year}_part{batch}of{total_batches}.png'",
            "  credit: 'Climate Data Analysis & Visualisation by Colin Osborne'",
            "  single_plot_credit: 'Analysis & visualisation by Colin Osborne'",
            "  data_source: 'Data from: ERA5 via CDS'",
            "plotting:",
            "  measure_labels:",
            "    daily_precipitation:",
            "      label: Daily Precipitation",
            "      unit: mm",
            "      y_value_column: precip_mm",
            "      y_min: 0",
            "      y_max: 10",
            "      y_step: 2",
            "      range_text: '{measure_label}: {min_value:.1f} to {max_value:.1f} {measure_unit}'",
        ])
    )

    df = pd.DataFrame({
        'place_name': ['Cambridge, UK'],
        'precip_mm': [1.2],
        'date': ['2025-01-01'],
    })
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="Europe/London")
    mock_create_individual.return_value = str(tmp_path / "plot.png")

    plot_all(
        df_overall=df,
        place_list=[loc],
        start_year=2025,
        end_year=2025,
        out_dir=tmp_path,
        config=config_file,
        settings=Path("geo_plot/settings.yaml"),
        show_main=False,
        show_individual=False,
        measure='daily_precipitation',
    )

    kwargs = mock_create_individual.call_args[1]
    run_ctx = kwargs['run_ctx']
    assert run_ctx.t_min_c == 0.0
    assert run_ctx.t_max_c == 10.0
# Unit testing these functions would require either:
# - Significant refactoring to inject dependencies
# - Complex mock setups that are brittle and hard to maintain
# - Integration test fixtures with real plotting libraries
#
# The calculate_grid_dimensions function above tests the core logic
# that drives the orchestration behavior.
