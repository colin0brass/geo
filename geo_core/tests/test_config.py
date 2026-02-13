import pytest

from geo_core.config import (
    DEFAULT_COLORMAP,
    load_colormap,
    load_colour_mode,
    load_grid_settings,
)


def test_load_colour_mode_default(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("grid:\n  max_auto_rows: 4\n")
    assert load_colour_mode(config_file) == 'y_value'


def test_load_colour_mode_from_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  colour_mode: year\n")
    assert load_colour_mode(config_file) == 'year'


def test_load_colour_mode_cli_override(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  colour_mode: y_value\n")
    assert load_colour_mode(config_file, cli_colour_mode='year') == 'year'


def test_load_colour_mode_invalid_temperature_config_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  colour_mode: temperature\n")
    with pytest.raises(ValueError) as exc_info:
        load_colour_mode(config_file)
    assert "Invalid plotting.colour_mode" in str(exc_info.value)


def test_load_colour_mode_invalid_config_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  colour_mode: invalid\n")
    with pytest.raises(ValueError) as exc_info:
        load_colour_mode(config_file)
    assert "Invalid plotting.colour_mode" in str(exc_info.value)


def test_load_colormap_default(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n")
    assert load_colormap(config_file) == 'viridis'


def test_colormap_default_constant():
    assert DEFAULT_COLORMAP == 'turbo'


def test_load_colormap_from_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n  colormap: plasma\n")
    assert load_colormap(config_file) == 'plasma'


def test_load_colormap_invalid_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n  colormap: not_a_cmap\n")
    assert load_colormap(config_file) == 'viridis'


def test_load_colormap_blank_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n  colormap: \"\"\n")
    assert load_colormap(config_file) == 'viridis'


def test_load_colormap_non_string_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n  colormap: 123\n")
    assert load_colormap(config_file) == 'viridis'


def test_load_colormap_invalid_valid_colormap_list_falls_back(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [not_a_map, also_bad]\n")
    assert load_colormap(config_file) == DEFAULT_COLORMAP


def test_load_grid_settings_valid(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("grid:\n  max_auto_rows: 5\n  max_auto_cols: 8\n")

    max_rows, max_cols = load_grid_settings(config_file)
    assert max_rows == 5
    assert max_cols == 8


def test_load_grid_settings_defaults(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("other_section:\n  key: value\n")

    max_rows, max_cols = load_grid_settings(config_file)
    assert max_rows == 4
    assert max_cols == 6


def test_load_grid_settings_partial(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("grid:\n  max_auto_rows: 3\n")

    max_rows, max_cols = load_grid_settings(config_file)
    assert max_rows == 3
    assert max_cols == 6


def test_load_grid_settings_missing_file(tmp_path):
    config_file = tmp_path / "nonexistent.yaml"

    max_rows, max_cols = load_grid_settings(config_file)
    assert max_rows == 4
    assert max_cols == 6
