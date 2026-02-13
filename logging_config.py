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
    'suppress_root_logger': True,
    'third_party_log_level': 'WARNING',
}


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
    if settings['suppress_cdsapi']:
        cdsapi_logger = logging.getLogger('cdsapi')
        cdsapi_logger.handlers.clear()
        cdsapi_logger.setLevel(third_party_level)
        cdsapi_logger.propagate = False

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
