"""
Progress reporting system for geo_temp.

Provides a callback-based progress reporting mechanism with pluggable handlers.
"""

from __future__ import annotations
from typing import Protocol


class ProgressHandler(Protocol):
    """Protocol for progress event handlers."""
    
    def on_location_start(self, location_name: str, location_num: int, total_locations: int) -> None:
        """Called when processing starts for a location."""
        ...
    
    def on_year_start(self, location_name: str, year: int, total_months: int) -> None:
        """Called when processing starts for a year."""
        ...
    
    def on_month_complete(self, location_name: str, year: int, month_num: int, total_months: int) -> None:
        """Called when a month is completed."""
        ...
    
    def on_year_complete(self, location_name: str, year: int) -> None:
        """Called when all months for a year are completed."""
        ...
    
    def on_location_complete(self, location_name: str) -> None:
        """Called when processing completes for a location."""
        ...


class ConsoleProgressHandler:
    """Console-based progress handler with progress bars."""
    
    def on_location_start(self, location_name: str, location_num: int, total_locations: int) -> None:
        """Display location start message."""
        # Store context for progress bar display
        self._current_location_num = location_num
        self._total_locations = total_locations
        self._current_year_num = 0
        self._total_years = 0
    
    def on_year_start(self, location_name: str, year: int, total_months: int) -> None:
        """Initialize progress bar for the year."""
        # Increment year counter for this location
        self._current_year_num += 1
    
    def on_month_complete(self, location_name: str, year: int, month_num: int, total_months: int) -> None:
        """Update progress bar for the current year."""
        bar_width = 12
        filled = int(bar_width * month_num / total_months)
        bar = '█' * filled + '░' * (bar_width - filled)
        # Get current place number from context if available
        place_prefix = ""
        if hasattr(self, '_current_location_num') and hasattr(self, '_total_locations'):
            place_prefix = f"Place {self._current_location_num}/{self._total_locations} "
        # Get year number context
        year_prefix = ""
        if hasattr(self, '_current_year_num') and hasattr(self, '_total_years') and self._total_years > 0:
            year_prefix = f"Year {self._current_year_num}/{self._total_years} "
        # Pad location name to 30 characters for alignment
        padded_name = f"{location_name:<30}"
        print(f"\r  {place_prefix}{padded_name} - {year_prefix}{year}: [{bar}] {month_num}/{total_months} months", end='', flush=True)
    
    def on_year_complete(self, location_name: str, year: int) -> None:
        """Complete the progress bar line."""
        print()  # Move to next line
    
    def on_location_complete(self, location_name: str) -> None:
        """Location processing complete."""
        # Reset year counter for next location
        self._current_year_num = 0


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
    
    def notify_location_start(self, location_name: str, location_num: int, total_locations: int) -> None:
        """Notify all handlers that location processing started."""
        for handler in self.handlers:
            handler.on_location_start(location_name, location_num, total_locations)
    
    def notify_year_start(self, location_name: str, year: int, total_months: int) -> None:
        """Notify all handlers that year processing started."""
        for handler in self.handlers:
            handler.on_year_start(location_name, year, total_months)
    
    def notify_month_complete(self, location_name: str, year: int, month_num: int, total_months: int) -> None:
        """Notify all handlers that a month was completed."""
        for handler in self.handlers:
            handler.on_month_complete(location_name, year, month_num, total_months)
    
    def notify_year_complete(self, location_name: str, year: int) -> None:
        """Notify all handlers that year processing completed."""
        for handler in self.handlers:
            handler.on_year_complete(location_name, year)
    
    def notify_location_complete(self, location_name: str) -> None:
        """Notify all handlers that location processing completed."""
        for handler in self.handlers:
            handler.on_location_complete(location_name)
    
    def set_total_years(self, total_years: int) -> None:
        """Set the total number of years to be fetched for current location."""
        for handler in self.handlers:
            if hasattr(handler, '_total_years'):
                handler._total_years = total_years


# Global progress manager instance
_progress_manager = ProgressManager()


def get_progress_manager() -> ProgressManager:
    """
    Get the global progress manager instance.
    
    Returns:
        Global ProgressManager instance.
    """
    return _progress_manager
