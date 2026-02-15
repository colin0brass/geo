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

    def on_stage_progress(
        self,
        stage_label: str,
        item_label: str,
        current_item: int,
        total_items: int,
        detail: str | None = None,
    ) -> None:
        """Called for generic one-line progress updates (for example cache/plot phases)."""
        ...

    def on_stage_complete(self, stage_label: str) -> None:
        """Called when a generic stage progress stream is complete."""
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

    def notify_stage_progress(
        self,
        stage_label: str,
        item_label: str,
        current_item: int,
        total_items: int,
        detail: str | None = None,
    ) -> None:
        """Notify handlers about generic one-line stage progress (if supported)."""
        for handler in self.handlers:
            stage_progress = getattr(handler, "on_stage_progress", None)
            if callable(stage_progress):
                stage_progress(stage_label, item_label, current_item, total_items, detail)

    def notify_stage_complete(self, stage_label: str) -> None:
        """Notify handlers that generic one-line stage progress is complete (if supported)."""
        for handler in self.handlers:
            stage_complete = getattr(handler, "on_stage_complete", None)
            if callable(stage_complete):
                stage_complete(stage_label)


_progress_manager = ProgressManager()


def get_progress_manager() -> ProgressManager:
    """Get the global progress manager instance."""
    return _progress_manager
