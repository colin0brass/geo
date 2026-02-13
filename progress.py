"""
Progress reporting system for geo.

Provides a callback-based progress reporting mechanism with pluggable handlers.
"""

from __future__ import annotations
from typing import Protocol


class ProgressHandler(Protocol):
    """Protocol for progress event handlers."""

    def on_location_start(self, location_name: str, location_num: int, total_locations: int, total_years: int = 1) -> None:
        """Called when processing starts for a location."""
        ...

    def on_year_start(self, location_name: str, year: int, current_year: int, total_years: int) -> None:
        """Called when processing starts for a year."""
        ...

    def on_year_complete(self, location_name: str, year: int, current_year: int, total_years: int) -> None:
        """Called when a year is completed."""
        ...

    def on_location_complete(self, location_name: str) -> None:
        """Called when processing completes for a location."""
        ...


class ConsoleProgressHandler:
    """Console-based progress handler with progress bars."""

    def on_location_start(self, location_name: str, location_num: int, total_locations: int, total_years: int = 1) -> None:
        """Display location start message."""
        self._current_location_num = location_num
        self._total_locations = total_locations
        self._total_years = total_years

    def on_year_start(self, location_name: str, year: int, current_year: int, total_years: int) -> None:
        """Display initial progress bar for the year."""
        bar_width = 12
        # Show progress of completed years (before starting this year)
        filled = int(bar_width * (current_year - 1) / total_years)
        bar = '█' * filled + '░' * (bar_width - filled)

        # Get current place number from context
        place_prefix = ""
        if hasattr(self, '_current_location_num') and hasattr(self, '_total_locations'):
            place_prefix = f"Place {self._current_location_num}/{self._total_locations} "

        # Pad location name to 30 characters for alignment
        padded_name = f"{location_name:<30}"

        # Show number of completed years (not including current year being downloaded)
        completed_years = current_year - 1
        percentage = int(100 * completed_years / total_years)
        print(f"\r  {place_prefix}{padded_name} - Year {completed_years}/{total_years} ({year}): [{bar}] {percentage}%", end='', flush=True)

    def on_year_complete(self, location_name: str, year: int, current_year: int, total_years: int) -> None:
        """Update the progress bar after year completes."""
        bar_width = 12
        filled = int(bar_width * current_year / total_years)
        bar = '█' * filled + '░' * (bar_width - filled)

        # Get current place number from context
        place_prefix = ""
        if hasattr(self, '_current_location_num') and hasattr(self, '_total_locations'):
            place_prefix = f"Place {self._current_location_num}/{self._total_locations} "

        # Pad location name to 30 characters for alignment
        padded_name = f"{location_name:<30}"

        # Show updated progress with filled bar
        percentage = int(100 * current_year / total_years)
        print(f"\r  {place_prefix}{padded_name} - Year {current_year}/{total_years} ({year}): [{bar}] {percentage}%", end='', flush=True)

    def on_location_complete(self, location_name: str) -> None:
        """Location processing complete - move to next line."""
        print()  # Move to next line after location is done


class ProgressManager:
    """Manages progress event handlers and dispatches events."""

    def __init__(self) -> None:
        """Initialize the progress manager."""
        self.handlers: list[ProgressHandler] = []

    def register_handler(self, handler: ProgressHandler) -> None:
        """
        Register a progress handler.

        Args:
            handler: Handler implementing the ProgressHandler protocol.
        """
        self.handlers.append(handler)

    def clear_handlers(self) -> None:
        """Remove all registered handlers."""
        self.handlers.clear()

    def notify_location_start(self, location_name: str, location_num: int, total_locations: int, total_years: int = 1) -> None:
        """Notify all handlers that location processing started.

        Args:
            location_name: Name of the location.
            location_num: Current location number (1-based).
            total_locations: Total number of locations to process.
            total_years: Total number of years to fetch for this location (default 1).
        """
        for handler in self.handlers:
            handler.on_location_start(location_name, location_num, total_locations, total_years)
        for handler in self.handlers:
            handler.on_location_start(location_name, location_num, total_locations)

    def notify_year_start(self, location_name: str, year: int, current_year: int, total_years: int) -> None:
        """Notify all handlers that year processing started."""
        for handler in self.handlers:
            handler.on_year_start(location_name, year, current_year, total_years)

    def notify_year_complete(self, location_name: str, year: int, current_year: int, total_years: int) -> None:
        """Notify all handlers that year processing completed."""
        for handler in self.handlers:
            handler.on_year_complete(location_name, year, current_year, total_years)

    def notify_location_complete(self, location_name: str) -> None:
        """Notify all handlers that location processing completed."""
        for handler in self.handlers:
            handler.on_location_complete(location_name)


# Global progress manager instance
_progress_manager = ProgressManager()


def get_progress_manager() -> ProgressManager:
    """
    Get the global progress manager instance.

    Returns:
        Global ProgressManager instance.
    """
    return _progress_manager
