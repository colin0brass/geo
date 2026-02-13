"""Tests for progress reporting system."""

from progress import (
    ConsoleProgressHandler,
)


def test_console_progress_handler_output(capsys):
    """Test console progress handler output."""
    handler = ConsoleProgressHandler()
    handler.on_location_start("Austin, TX", 1, 1)

    # Test year complete
    handler.on_year_complete("Austin, TX", 2024, 1, 1)
    captured = capsys.readouterr()
    assert "Austin, TX" in captured.out
    assert "2024" in captured.out
    assert "Year 1/1" in captured.out
    assert "[" in captured.out and "]" in captured.out  # Progress bar delimiters


def test_console_progress_handler_with_place_numbering(capsys):
    """Test console progress handler with place numbering."""
    handler = ConsoleProgressHandler()

    # Simulate location start (sets place context and total years)
    handler.on_location_start("Austin, TX", 2, 5, total_years=3)

    # Simulate year start and complete
    handler.on_year_start("Austin, TX", 2024, 1, 3)
    handler.on_year_complete("Austin, TX", 2024, 1, 3)
    captured = capsys.readouterr()

    # Check for place numbering
    assert "Place 2/5" in captured.out
    # Check for year numbering
    assert "Year 1/3" in captured.out
    # Check location and other details
    assert "Austin, TX" in captured.out
    assert "2024" in captured.out


def test_console_progress_handler_location_complete(capsys):
    """Test that location complete moves to next line."""
    handler = ConsoleProgressHandler()

    # First location
    handler.on_location_start("City A", 1, 2)
    handler.on_year_start("City A", 2024, 1, 1)
    handler.on_year_complete("City A", 2024, 1, 1)
    handler.on_location_complete("City A")
    captured = capsys.readouterr()

    # Should have newline from location complete
    assert "\n" in captured.out
