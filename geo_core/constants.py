"""Core constants shared across configuration helpers."""

VALID_COLOUR_MODES = ("y_value", "colour_value", "year")
DEFAULT_COLOUR_MODE = VALID_COLOUR_MODES[0]
DEFAULT_COLORMAP = "turbo"
DEFAULT_RUNTIME_PATHS = {
    "cache_dir": "era5_cache",
    "data_cache_dir": "data_cache",
    "out_dir": "output",
    "settings_file": "geo_plot/settings.yaml",
}
DEFAULT_RETRIEVAL_SETTINGS = {
    "half_box_deg": 0.25,
    "max_nearest_time_delta_minutes": 30,
    "month_fetch_day_span_threshold": 62,
    "wet_hour_threshold_mm": 1.0,
    "fetch_mode": {
        "noon_temperature": "auto",
        "daily_precipitation": "monthly",
        "daily_solar_radiation_energy": "monthly",
    },
    "daily_source": {
        "noon_temperature": "timeseries",
        "daily_precipitation": "timeseries",
        "daily_solar_radiation_energy": "timeseries",
    },
}
REQUIRED_PLOT_TEXT_KEYS = (
    'single_plot_title',
    'overall_title',
    'overall_title_with_batch',
    'single_plot_filename',
    'subplot_filename',
    'subplot_filename_with_batch',
    'credit',
    'single_plot_credit',
    'data_source',
)
REQUIRED_MEASURE_LABEL_KEYS = (
    'label',
    'unit',
    'y_value_column',
    'range_text',
)
DEFAULT_GRID_SETTINGS = {
    'max_auto_rows': 4,
    'max_auto_cols': 6,
}
