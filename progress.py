"""
Progress reporting system for geo.

Provides a callback-based progress reporting mechanism with pluggable handlers.
"""

from __future__ import annotations
from geo_core.progress import ProgressHandler, ProgressManager, get_progress_manager

__all__ = ["ProgressHandler", "ProgressManager", "get_progress_manager", "ConsoleProgressHandler"]


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
