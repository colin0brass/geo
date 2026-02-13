import pytest

from geo_core.config import (
    CoreConfigService,
    DEFAULT_COLORMAP,
    DEFAULT_RETRIEVAL_SETTINGS,
    DEFAULT_RUNTIME_PATHS,
    extract_places_config,
    find_place_by_name,
    get_plot_text,
    load_measure_labels_config,
    load_plot_text_config,
    load_retrieval_settings,
    load_runtime_paths,
    render_config_yaml,
)


def test_load_colour_mode_default(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("grid:\n  max_auto_rows: 4\n")
    assert CoreConfigService(config_file).load_colour_mode() == 'y_value'


def test_core_config_service_delegates_loaders(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "grid:\n"
        "  max_auto_rows: 3\n"
        "  max_auto_cols: 5\n"
        "plotting:\n"
        "  colour_mode: year\n"
    )

    service = CoreConfigService(config_file)
    assert service.load_grid_settings() == (3, 5)
    assert service.load_colour_mode() == 'year'


def test_core_config_service_get_plot_text_uses_bound_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "plot_text:\n"
        "  single_plot_title: '{location} ({start_year}-{end_year})'\n"
        "  subplot_title: '{measure_label} ({start_year}-{end_year})'\n"
        "  subplot_title_with_batch: '{measure_label} ({start_year}-{end_year}) - Part {batch}/{total_batches}'\n"
        "  single_plot_filename: '{location}_{measure_key}_{start_year}_{end_year}.png'\n"
        "  subplot_filename: '{list_name}_{measure_key}_{start_year}_{end_year}.png'\n"
        "  subplot_filename_with_batch: '{list_name}_{measure_key}_{start_year}_{end_year}_part{batch}of{total_batches}.png'\n"
        "  credit: 'credit'\n"
        "  single_plot_credit: 'single'\n"
        "  data_source: 'source'\n"
    )

    service = CoreConfigService(config_file)
    title = service.get_plot_text(
        'single_plot_title',
        location='Austin, TX',
        start_year=2024,
        end_year=2024,
    )
    assert title == 'Austin, TX (2024-2024)'


def test_load_colour_mode_from_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  colour_mode: year\n")
    assert CoreConfigService(config_file).load_colour_mode() == 'year'


def test_load_colour_mode_cli_override(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  colour_mode: y_value\n")
    assert CoreConfigService(config_file).load_colour_mode(cli_colour_mode='year') == 'year'


def test_load_colour_mode_invalid_temperature_config_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  colour_mode: temperature\n")
    with pytest.raises(ValueError) as exc_info:
        CoreConfigService(config_file).load_colour_mode()
    assert "Invalid plotting.colour_mode" in str(exc_info.value)


def test_load_colour_mode_invalid_config_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  colour_mode: invalid\n")
    with pytest.raises(ValueError) as exc_info:
        CoreConfigService(config_file).load_colour_mode()
    assert "Invalid plotting.colour_mode" in str(exc_info.value)


def test_load_colormap_default(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n")
    assert CoreConfigService(config_file).load_colormap() == 'viridis'


def test_colormap_default_constant():
    assert DEFAULT_COLORMAP == 'turbo'


def test_load_colormap_from_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n  colormap: plasma\n")
    assert CoreConfigService(config_file).load_colormap() == 'plasma'


def test_load_colormap_invalid_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n  colormap: not_a_cmap\n")
    with pytest.raises(ValueError):
        CoreConfigService(config_file).load_colormap()


def test_load_colormap_blank_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n  colormap: \"\"\n")
    with pytest.raises(ValueError):
        CoreConfigService(config_file).load_colormap()


def test_load_colormap_non_string_value(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [viridis, plasma]\n  colormap: 123\n")
    with pytest.raises(ValueError):
        CoreConfigService(config_file).load_colormap()


def test_load_colormap_invalid_valid_colormap_list_raises(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plotting:\n  valid_colormaps: [not_a_map, also_bad]\n")
    with pytest.raises(ValueError):
        CoreConfigService(config_file).load_colormap()


def test_load_grid_settings_valid(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("grid:\n  max_auto_rows: 5\n  max_auto_cols: 8\n")

    max_rows, max_cols = CoreConfigService(config_file).load_grid_settings()
    assert max_rows == 5
    assert max_cols == 8


def test_load_grid_settings_defaults(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("other_section:\n  key: value\n")

    max_rows, max_cols = CoreConfigService(config_file).load_grid_settings()
    assert max_rows == 4
    assert max_cols == 6


def test_load_grid_settings_partial(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("grid:\n  max_auto_rows: 3\n")

    max_rows, max_cols = CoreConfigService(config_file).load_grid_settings()
    assert max_rows == 3
    assert max_cols == 6


def test_load_grid_settings_missing_file(tmp_path):
    config_file = tmp_path / "nonexistent.yaml"

    with pytest.raises(FileNotFoundError):
        CoreConfigService(config_file).load_grid_settings()


def test_load_grid_settings_invalid_value_raises(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("grid:\n  max_auto_rows: 0\n  max_auto_cols: 6\n")

    with pytest.raises(ValueError):
        CoreConfigService(config_file).load_grid_settings()


def test_extract_places_config_basic_payload():
    config = {
        'places': {
            'default_place': 'Cambridge, UK',
            'all_places': [
                {'name': 'Cambridge, UK', 'lat': 52.2053, 'lon': 0.1218},
                {'name': 'Austin, TX', 'lat': 30.2672, 'lon': -97.7431},
            ],
            'place_lists': {
                'default': ['Cambridge, UK', 'Austin, TX'],
            },
        }
    }

    all_places, default_place, place_lists = extract_places_config(config)

    assert len(all_places) == 2
    assert default_place == 'Cambridge, UK'
    assert place_lists == {'default': ['Cambridge, UK', 'Austin, TX']}


def test_extract_places_config_default_falls_back_to_first_place():
    config = {
        'places': {
            'all_places': [
                {'name': 'FirstPlace', 'lat': 1.0, 'lon': 2.0},
                {'name': 'SecondPlace', 'lat': 3.0, 'lon': 4.0},
            ]
        }
    }

    _, default_place, _ = extract_places_config(config)
    assert default_place == 'FirstPlace'


def test_render_config_yaml_places_flow_style_and_comments():
    config = {
        'logging': {
            'log_file': 'geo.log',
            'console_level': 'WARNING',
        },
        'places': {
            'default_place': 'São Paulo, Brazil',
            'all_places': [
                {'name': 'São Paulo, Brazil', 'lat': -23.55, 'lon': -46.63},
            ],
            'place_lists': {
                'default': ['São Paulo, Brazil'],
            },
        },
    }

    rendered = render_config_yaml(config)

    assert '# geo configuration file' in rendered
    assert 'logging:' in rendered
    assert 'places:' in rendered
    assert 'default_place: São Paulo, Brazil' in rendered
    assert '{name: "São Paulo, Brazil", lat: -23.55, lon: -46.63}' in rendered
    assert 'place_lists:' in rendered


def test_find_place_by_name_returns_match_or_none():
    all_places = [
        {'name': 'Cambridge, UK', 'lat': 52.2053, 'lon': 0.1218},
        {'name': 'Austin, TX', 'lat': 30.2672, 'lon': -97.7431},
    ]

    match = find_place_by_name(all_places, 'Austin, TX')
    assert match is not None
    assert match['lat'] == 30.2672

    missing = find_place_by_name(all_places, 'Not Found')
    assert missing is None


def test_load_retrieval_settings_defaults_when_section_missing(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("logging:\n  console_level: WARNING\n")

    settings = load_retrieval_settings(config_file)
    assert settings == DEFAULT_RETRIEVAL_SETTINGS


def test_load_retrieval_settings_from_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "retrieval:\n"
        "  half_box_deg: 0.1\n"
        "  max_nearest_time_delta_minutes: 20\n"
        "  month_fetch_day_span_threshold: 40\n"
        "  fetch_mode:\n"
        "    noon_temperature: yearly\n"
        "    daily_precipitation: yearly\n"
        "    daily_solar_radiation_energy: auto\n"
    )

    settings = load_retrieval_settings(config_file)
    assert settings["half_box_deg"] == 0.1
    assert settings["max_nearest_time_delta_minutes"] == 20
    assert settings["month_fetch_day_span_threshold"] == 40
    assert settings["fetch_mode"]["noon_temperature"] == "yearly"
    assert settings["fetch_mode"]["daily_precipitation"] == "yearly"
    assert settings["fetch_mode"]["daily_solar_radiation_energy"] == "auto"


def test_load_retrieval_settings_invalid_value_raises(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("retrieval:\n  month_fetch_day_span_threshold: 0\n")

    with pytest.raises(ValueError):
        load_retrieval_settings(config_file)


def test_load_retrieval_settings_invalid_fetch_mode_raises(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "retrieval:\n"
        "  fetch_mode:\n"
        "    daily_precipitation: invalid\n"
    )

    with pytest.raises(ValueError):
        load_retrieval_settings(config_file)


def test_load_retrieval_settings_legacy_fetch_mode_keys_still_work(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "retrieval:\n"
        "  temp_fetch_mode: yearly\n"
        "  precipitation_fetch_mode: auto\n"
        "  solar_fetch_mode: yearly\n"
    )

    settings = load_retrieval_settings(config_file)
    assert settings["fetch_mode"]["noon_temperature"] == "yearly"
    assert settings["fetch_mode"]["daily_precipitation"] == "auto"
    assert settings["fetch_mode"]["daily_solar_radiation_energy"] == "yearly"


def test_load_retrieval_settings_nested_short_fetch_mode_keys_still_work(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "retrieval:\n"
        "  fetch_mode:\n"
        "    temp: yearly\n"
        "    precipitation: auto\n"
        "    solar: yearly\n"
    )

    settings = load_retrieval_settings(config_file)
    assert settings["fetch_mode"]["noon_temperature"] == "yearly"
    assert settings["fetch_mode"]["daily_precipitation"] == "auto"
    assert settings["fetch_mode"]["daily_solar_radiation_energy"] == "yearly"


def test_load_runtime_paths_defaults_when_section_missing(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("logging:\n  console_level: WARNING\n")

    paths = load_runtime_paths(config_file)
    assert paths == DEFAULT_RUNTIME_PATHS


def test_load_runtime_paths_from_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "runtime_paths:\n"
        "  cache_dir: custom_cache\n"
        "  data_cache_dir: custom_data\n"
        "  out_dir: custom_out\n"
        "  settings_file: custom/settings.yaml\n"
    )

    paths = load_runtime_paths(config_file)
    assert paths["cache_dir"] == "custom_cache"
    assert paths["data_cache_dir"] == "custom_data"
    assert paths["out_dir"] == "custom_out"
    assert paths["settings_file"] == "custom/settings.yaml"


def test_load_runtime_paths_invalid_value_raises(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("runtime_paths:\n  cache_dir: 123\n")

    with pytest.raises(ValueError):
        load_runtime_paths(config_file)


def test_load_plot_text_config_valid(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "plot_text:\n"
        "  single_plot_title: '{location} {measure_label} ({start_year}-{end_year})'\n"
        "  subplot_title: '{measure_label} ({start_year}-{end_year})'\n"
        "  subplot_title_with_batch: '{measure_label} ({start_year}-{end_year}) - Part {batch}/{total_batches}'\n"
        "  single_plot_filename: '{location}_{measure_key}_{start_year}_{end_year}.png'\n"
        "  subplot_filename: '{list_name}_{measure_key}_{start_year}_{end_year}.png'\n"
        "  subplot_filename_with_batch: '{list_name}_{measure_key}_{start_year}_{end_year}_part{batch}of{total_batches}.png'\n"
        "  credit: 'Climate Data Analysis & Visualisation by Colin Osborne'\n"
        "  single_plot_credit: 'Analysis & visualisation by Colin Osborne'\n"
        "  data_source: 'Data from: ERA5 via CDS'\n"
    )

    plot_text = load_plot_text_config(config_file)
    assert plot_text["subplot_title"] == "{measure_label} ({start_year}-{end_year})"


def test_load_plot_text_config_missing_required_key_raises(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("plot_text:\n  subplot_title: '{measure_label}'\n")

    with pytest.raises(ValueError):
        load_plot_text_config(config_file)


def test_load_measure_labels_config_missing_required_field_raises(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "plotting:\n"
        "  measure_labels:\n"
        "    noon_temperature:\n"
        "      label: Mid-Day Temperature\n"
        "      unit: °C\n"
        "      y_value_column: temp_C\n"
    )

    with pytest.raises(ValueError):
        load_measure_labels_config(config_file)


def test_load_measure_labels_config_optional_range_controls(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "plotting:\n"
        "  measure_labels:\n"
        "    daily_precipitation:\n"
        "      label: Daily Precipitation\n"
        "      unit: mm\n"
        "      y_value_column: precip_mm\n"
        "      y_min: 0\n"
        "      y_max: 50\n"
        "      y_step: 5\n"
        "      range_text: '{measure_label}: {min_value:.1f} to {max_value:.1f} {measure_unit}'\n"
    )

    labels = load_measure_labels_config(config_file)
    assert labels["daily_precipitation"]["y_min"] == 0.0
    assert labels["daily_precipitation"]["y_max"] == 50.0
    assert labels["daily_precipitation"]["y_step"] == 5.0


def test_load_measure_labels_config_invalid_y_step_raises(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "plotting:\n"
        "  measure_labels:\n"
        "    daily_precipitation:\n"
        "      label: Daily Precipitation\n"
        "      unit: mm\n"
        "      y_value_column: precip_mm\n"
        "      y_step: 0\n"
        "      range_text: '{measure_label}: {min_value:.1f} to {max_value:.1f} {measure_unit}'\n"
    )

    with pytest.raises(ValueError):
        load_measure_labels_config(config_file)


def test_load_measure_labels_config_invalid_y_min_max_raises(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "plotting:\n"
        "  measure_labels:\n"
        "    daily_precipitation:\n"
        "      label: Daily Precipitation\n"
        "      unit: mm\n"
        "      y_value_column: precip_mm\n"
        "      y_min: 10\n"
        "      y_max: 5\n"
        "      range_text: '{measure_label}: {min_value:.1f} to {max_value:.1f} {measure_unit}'\n"
    )

    with pytest.raises(ValueError):
        load_measure_labels_config(config_file)


def test_get_plot_text_missing_placeholder_raises():
    config = {
        "subplot_title": "{measure_label} ({start_year}-{end_year})",
    }

    with pytest.raises(ValueError):
        get_plot_text(config, "subplot_title", measure_label="Temp", start_year=2020)
