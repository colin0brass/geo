"""
Progress reporting system for geo.

Provides a callback-based progress reporting mechanism with pluggable handlers.
"""

from __future__ import annotations
from geo_core.progress import ProgressHandler, ProgressManager, get_progress_manager

__all__ = ["ProgressHandler", "ProgressManager", "get_progress_manager", "ConsoleProgressHandler"]


class ConsoleProgressHandler:
    """Console-based progress handler with progress bars."""

    def _render_progress_line(
        self,
        location_name: str,
        year: int,
        completed_years: int,
        total_years: int,
        *,
        month: int | None = None,
        completed_months: int | None = None,
        total_months: int | None = None,
    ) -> None:
        year_bar_width = 12
        year_filled = int(year_bar_width * completed_years / total_years)
        year_bar = '█' * year_filled + '░' * (year_bar_width - year_filled)
        year_pct = int(100 * completed_years / total_years)

        place_prefix = ""
        if hasattr(self, '_current_location_num') and hasattr(self, '_total_locations'):
            place_prefix = f"Place {self._current_location_num}/{self._total_locations} "

        padded_name = f"{location_name:<30}"
        line = (
            f"\r  {place_prefix}{padded_name} - "
            f"Year {completed_years}/{total_years} ({year}): [{year_bar}] {year_pct}%"
        )

        if month is not None and completed_months is not None and total_months is not None:
            month_bar_width = 10
            month_filled = int(month_bar_width * completed_months / total_months)
            month_bar = '█' * month_filled + '░' * (month_bar_width - month_filled)
            month_pct = int(100 * completed_months / total_months)
            line += (
                f" | Month {completed_months}/{total_months} ({month:02d}): "
                f"[{month_bar}] {month_pct}%"
            )

        print(line, end='', flush=True)

    def on_location_start(self, location_name: str, location_num: int, total_locations: int, total_years: int = 1) -> None:
        """Display location start message."""
        self._current_location_num = location_num
        self._total_locations = total_locations
        self._total_years = total_years

    def on_year_start(self, location_name: str, year: int, current_year: int, total_years: int) -> None:
        """Display initial progress bar for the year."""
        self._active_year = year
        self._active_year_index = current_year
        self._active_total_years = total_years
        self._active_total_months = None
        self._active_month = None
        self._render_progress_line(
            location_name,
            year,
            current_year - 1,
            total_years,
        )

    def on_year_complete(self, location_name: str, year: int, current_year: int, total_years: int) -> None:
        """Update the progress bar after year completes."""
        self._render_progress_line(
            location_name,
            year,
            current_year,
            total_years,
        )
        self._active_total_months = None
        self._active_month = None

    def on_month_start(
        self,
        location_name: str,
        year: int,
        month: int,
        current_month: int,
        total_months: int,
    ) -> None:
        """Display month progress while preserving year progress on same line."""
        year_idx = getattr(self, '_active_year_index', 1)
        total_years = getattr(self, '_active_total_years', 1)
        self._active_total_months = total_months
        self._active_month = month
        self._render_progress_line(
            location_name,
            year,
            year_idx - 1,
            total_years,
            month=month,
            completed_months=current_month - 1,
            total_months=total_months,
        )

    def on_month_complete(
        self,
        location_name: str,
        year: int,
        month: int,
        current_month: int,
        total_months: int,
    ) -> None:
        """Update month progress while preserving year progress on same line."""
        year_idx = getattr(self, '_active_year_index', 1)
        total_years = getattr(self, '_active_total_years', 1)
        self._active_total_months = total_months
        self._active_month = month
        self._render_progress_line(
            location_name,
            year,
            year_idx - 1,
            total_years,
            month=month,
            completed_months=current_month,
            total_months=total_months,
        )

    def on_location_complete(self, location_name: str) -> None:
        """Location processing complete - move to next line."""
        print()  # Move to next line after location is done
