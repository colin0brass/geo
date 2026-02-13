"""Core progress primitives shared across layers."""

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

    def on_month_start(
        self,
        location_name: str,
        year: int,
        month: int,
        current_month: int,
        total_months: int,
    ) -> None:
        """Called when processing starts for a month within a year."""
        ...

    def on_month_complete(
        self,
        location_name: str,
        year: int,
        month: int,
        current_month: int,
        total_months: int,
    ) -> None:
        """Called when processing completes for a month within a year."""
        ...


class ProgressManager:
    """Manages progress event handlers and dispatches events."""

    def __init__(self) -> None:
        """Initialize the progress manager."""
        self.handlers: list[ProgressHandler] = []

    def register_handler(self, handler: ProgressHandler) -> None:
        """Register a progress handler."""
        self.handlers.append(handler)

    def clear_handlers(self) -> None:
        """Remove all registered handlers."""
        self.handlers.clear()

    def notify_location_start(self, location_name: str, location_num: int, total_locations: int, total_years: int = 1) -> None:
        """Notify all handlers that location processing started."""
        for handler in self.handlers:
            handler.on_location_start(location_name, location_num, total_locations, total_years)

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

    def notify_month_start(
        self,
        location_name: str,
        year: int,
        month: int,
        current_month: int,
        total_months: int,
    ) -> None:
        """Notify handlers that month processing started (if supported)."""
        for handler in self.handlers:
            month_start = getattr(handler, "on_month_start", None)
            if callable(month_start):
                month_start(location_name, year, month, current_month, total_months)

    def notify_month_complete(
        self,
        location_name: str,
        year: int,
        month: int,
        current_month: int,
        total_months: int,
    ) -> None:
        """Notify handlers that month processing completed (if supported)."""
        for handler in self.handlers:
            month_complete = getattr(handler, "on_month_complete", None)
            if callable(month_complete):
                month_complete(location_name, year, month, current_month, total_months)


_progress_manager = ProgressManager()


def get_progress_manager() -> ProgressManager:
    """Get the global progress manager instance."""
    return _progress_manager
