"""Centralized logging configuration for geo.

Reads logging configuration from config.yaml.
"""

import logging
import sys
from pathlib import Path

import yaml


def setup_logging(config_path: Path = Path("config.yaml")) -> logging.Logger:
    """
    Configure logging using settings from config.yaml.

    Args:
        config_path: Path to the configuration YAML file.

    Returns:
        Configured logger instance.
    """
    # Load configuration
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logging_config = config.get('logging', {})
    log_file = logging_config.get('log_file', 'geo.log')
    console_level = logging_config.get('console_level', 'WARNING')

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
    # Use 'w' mode to clear log file on each run for a clean start
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)

    # Console handler (WARNING level by default - only warnings and errors)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)

    # Suppress verbose output from cdsapi library
    # Remove any existing handlers and set level to WARNING
    cdsapi_logger = logging.getLogger('cdsapi')
    cdsapi_logger.handlers.clear()  # Remove any handlers cdsapi may have added
    cdsapi_logger.setLevel(logging.WARNING)
    cdsapi_logger.propagate = False

    # Also suppress the root logger to catch any cdsapi messages that bypass their logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)

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
