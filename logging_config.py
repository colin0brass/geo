"""Centralized logging configuration for geo.

Reads logging configuration from config.yaml.
"""

import logging
import sys
from pathlib import Path

import yaml


DEFAULT_LOGGING_SETTINGS = {
    'log_file': 'geo.log',
    'console_level': 'WARNING',
    'file_mode': 'w',
    'suppress_cdsapi': True,
    'cds_warnings_in_verbose': True,
    'suppress_root_logger': True,
    'third_party_log_level': 'WARNING',
}

_CDS_LOGGER_NAMES = (
    'cdsapi',
    'ecmwf',
    'ecmwf.datastores',
    'ecmwf.datastores.client',
    'ecmwf.datastores.processing',
)

_cds_suppression_enabled = False
_cds_warnings_in_verbose_enabled = True
_cds_show_warnings = False


def _set_cds_external_logger_levels(show_warnings: bool) -> None:
    """Apply logger levels for CDS/ECMWF stack based on warning visibility."""
    target_level = logging.WARNING if show_warnings else logging.ERROR
    for logger_name in _CDS_LOGGER_NAMES:
        ext_logger = logging.getLogger(logger_name)
        ext_logger.handlers.clear()
        ext_logger.setLevel(target_level)
        ext_logger.propagate = False


def sync_cds_warning_visibility(console_is_debug: bool) -> None:
    """Synchronize CDS warning visibility with current console verbosity mode."""
    global _cds_show_warnings

    if not _cds_suppression_enabled:
        _cds_show_warnings = True
        return
    show_warnings = bool(_cds_warnings_in_verbose_enabled and console_is_debug)
    _set_cds_external_logger_levels(show_warnings)
    _cds_show_warnings = show_warnings


def should_show_cds_warnings() -> bool:
    """Return whether CDS/ECMWF warning messages should be surfaced to console."""
    return _cds_show_warnings


def _load_logging_settings(config_path: Path) -> dict:
    """Load and validate logging settings from config.yaml."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f) or {}

    logging_config = config.get('logging', {})
    if not isinstance(logging_config, dict):
        raise ValueError(f"Invalid logging section in {config_path}; expected mapping.")

    settings = DEFAULT_LOGGING_SETTINGS.copy()
    settings.update(logging_config)

    console_level = str(settings['console_level']).upper()
    if not hasattr(logging, console_level):
        raise ValueError(f"Invalid logging.console_level '{settings['console_level']}'")
    settings['console_level'] = console_level

    third_party_level = str(settings['third_party_log_level']).upper()
    if not hasattr(logging, third_party_level):
        raise ValueError(f"Invalid logging.third_party_log_level '{settings['third_party_log_level']}'")
    settings['third_party_log_level'] = third_party_level

    if settings['file_mode'] not in ('w', 'a'):
        raise ValueError("logging.file_mode must be 'w' or 'a'")
    if not isinstance(settings['suppress_cdsapi'], bool):
        raise ValueError("logging.suppress_cdsapi must be boolean")
    if not isinstance(settings['cds_warnings_in_verbose'], bool):
        raise ValueError("logging.cds_warnings_in_verbose must be boolean")
    if not isinstance(settings['suppress_root_logger'], bool):
        raise ValueError("logging.suppress_root_logger must be boolean")
    if not isinstance(settings['log_file'], str) or not settings['log_file'].strip():
        raise ValueError("logging.log_file must be a non-empty string")

    return settings


def setup_logging(config_path: Path = Path("config.yaml")) -> logging.Logger:
    """
    Configure logging using settings from config.yaml.

    Args:
        config_path: Path to the configuration YAML file.

    Returns:
        Configured logger instance.
    """
    global _cds_suppression_enabled, _cds_warnings_in_verbose_enabled, _cds_show_warnings

    settings = _load_logging_settings(config_path)
    log_file = settings['log_file']
    console_level = settings['console_level']

    # Create logger
    logger = logging.getLogger("geo")
    logger.setLevel(logging.DEBUG)  # Capture everything

    # Avoid adding handlers multiple times if called repeatedly
    if logger.handlers:
        return logger

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    simple_formatter = logging.Formatter(
        '%(levelname)s: %(message)s'
    )

    # File handler (DEBUG level - captures everything)
    file_handler = logging.FileHandler(log_file, mode=settings['file_mode'], encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)

    # Console handler (WARNING level by default - only warnings and errors)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level))
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)

    third_party_level = getattr(logging, settings['third_party_log_level'])
    _cds_suppression_enabled = bool(settings['suppress_cdsapi'])
    _cds_warnings_in_verbose_enabled = bool(settings['cds_warnings_in_verbose'])

    if settings['suppress_cdsapi']:
        sync_cds_warning_visibility(console_is_debug=(console_level == 'DEBUG'))
    else:
        _cds_show_warnings = True

    if settings['suppress_root_logger']:
        root_logger = logging.getLogger()
        root_logger.setLevel(third_party_level)

    return logger


def get_logger(name: str = "geo") -> logging.Logger:
    """
    Get a logger instance.

    Args:
        name: Logger name (default: "geo").

    Returns:
        Logger instance.
    """
    return logging.getLogger(name)
