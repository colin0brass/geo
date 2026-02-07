"""Centralized logging configuration for geo_temp.

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
    log_file = logging_config.get('log_file', 'geo_temp.log')
    console_level = logging_config.get('console_level', 'WARNING')
    
    # Create logger
    logger = logging.getLogger("geo_temp")
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
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    # Console handler (WARNING level by default - only warnings and errors)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str = "geo_temp") -> logging.Logger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name (default: "geo_temp").
    
    Returns:
        Logger instance.
    """
    return logging.getLogger(name)
