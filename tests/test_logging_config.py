"""
Tests for logging_config module (logging configuration).
"""
import pytest
import logging
from logging_config import setup_logging, get_logger


def test_setup_logging_default(tmp_path):
    """Test setup_logging with default config.yaml."""
    config_content = """
logging:
  log_file: test.log
  console_level: WARNING
places:
  default_place: Test City
  all_places: []
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    # Clear any existing handlers
    logger = logging.getLogger("geo")
    logger.handlers.clear()
    
    result = setup_logging(config_file)
    
    assert result is not None
    assert result.name == "geo"
    assert result.level == logging.DEBUG
    assert len(result.handlers) == 2  # File and console handlers


def test_setup_logging_custom_console_level(tmp_path):
    """Test setup_logging with custom console level."""
    config_content = """
logging:
  log_file: test.log
  console_level: INFO
places:
  default_place: Test City
  all_places: []
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    logger = logging.getLogger("geo")
    logger.handlers.clear()
    
    result = setup_logging(config_file)
    
    # Check that console handler has INFO level (filter out FileHandler which is also StreamHandler subclass)
    console_handler = [h for h in result.handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)][0]
    assert console_handler.level == logging.INFO


def test_setup_logging_missing_config_section(tmp_path):
    """Test setup_logging when logging section is missing (uses defaults)."""
    config_content = """
places:
  default_place: Test City
  all_places: []
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    logger = logging.getLogger("geo")
    logger.handlers.clear()
    
    result = setup_logging(config_file)
    
    # Should use defaults
    assert result is not None
    assert len(result.handlers) == 2


def test_setup_logging_creates_log_file(tmp_path):
    """Test that setup_logging creates the log file."""
    config_content = """
logging:
  log_file: test_output.log
  console_level: WARNING
places:
  default_place: Test City
  all_places: []
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    # Change to tmp directory for log file creation
    import os
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    
    try:
        logger = logging.getLogger("geo")
        logger.handlers.clear()
        
        setup_logging(config_file)
        
        # Log something
        logger.info("Test message")
        
        # Check that log file exists
        log_file = tmp_path / "test_output.log"
        assert log_file.exists()
    finally:
        os.chdir(old_cwd)


def test_setup_logging_prevents_duplicate_handlers(tmp_path):
    """Test that calling setup_logging multiple times doesn't add duplicate handlers."""
    config_content = """
logging:
  log_file: test.log
  console_level: WARNING
places:
  default_place: Test City
  all_places: []
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    logger = logging.getLogger("geo")
    logger.handlers.clear()
    
    setup_logging(config_file)
    handler_count_1 = len(logger.handlers)
    
    setup_logging(config_file)
    handler_count_2 = len(logger.handlers)
    
    assert handler_count_1 == handler_count_2


def test_setup_logging_file_handler_debug_level(tmp_path):
    """Test that file handler always logs at DEBUG level."""
    config_content = """
logging:
  log_file: test.log
  console_level: ERROR
places:
  default_place: Test City
  all_places: []
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    logger = logging.getLogger("geo")
    logger.handlers.clear()
    
    result = setup_logging(config_file)
    
    # Find file handler
    file_handler = [h for h in result.handlers if isinstance(h, logging.FileHandler)][0]
    assert file_handler.level == logging.DEBUG


def test_setup_logging_console_handler_level(tmp_path):
    """Test different console logging levels."""
    for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        config_content = f"""
logging:
  log_file: test.log
  console_level: {level}
places:
  default_place: Test City
  all_places: []
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)
        
        logger = logging.getLogger("geo")
        logger.handlers.clear()
        
        result = setup_logging(config_file)
        
        console_handler = [h for h in result.handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)][0]
        expected_level = getattr(logging, level)
        assert console_handler.level == expected_level


def test_get_logger():
    """Test get_logger function."""
    logger = get_logger()
    assert logger is not None
    assert logger.name == "geo"
    
    custom_logger = get_logger("custom")
    assert custom_logger.name == "custom"


def test_setup_logging_log_format(tmp_path):
    """Test that log formatters are correctly configured."""
    config_content = """
logging:
  log_file: test.log
  console_level: WARNING
places:
  default_place: Test City
  all_places: []
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    logger = logging.getLogger("geo")
    logger.handlers.clear()
    
    result = setup_logging(config_file)
    
    # File handler should have detailed format
    file_handler = [h for h in result.handlers if isinstance(h, logging.FileHandler)][0]
    assert file_handler.formatter is not None
    assert "asctime" in file_handler.formatter._fmt or "levelname" in file_handler.formatter._fmt
    
    # Console handler should have simple format
    console_handler = [h for h in result.handlers if isinstance(h, logging.StreamHandler)][0]
    assert console_handler.formatter is not None


def test_setup_logging_missing_file(tmp_path):
    """Test setup_logging with missing config file."""
    missing_file = tmp_path / "nonexistent.yaml"
    
    logger = logging.getLogger("geo")
    logger.handlers.clear()
    
    with pytest.raises(FileNotFoundError):
        setup_logging(missing_file)
