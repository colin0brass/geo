"""Settings manager with automatic row-based scaling support."""

from typing import Any


class SettingsManager:
    """Manages settings with automatic row-based value selection.

    Supports settings that can have different values based on grid row count.
    If a setting value is a dict with numeric keys and/or 'default', automatically
    selects the appropriate value based on num_rows.
    """

    def __init__(self, settings_dict: dict, num_rows: int = 1):
        """Initialize settings manager.

        Args:
            settings_dict: Dictionary of settings (typically one layout section)
            num_rows: Number of rows in grid (for row-based scaling)
        """
        self.settings = settings_dict
        self.num_rows = num_rows
        self._row_key = min(num_rows, 4) if num_rows >= 3 else 'default'

    def get(self, path: str, default: Any = None) -> Any:
        """Get setting value with automatic row-based override.

        Args:
            path: Dot-notation path like 'figure.marker_size' or 'page.title_fontsize'
            default: Default value if path not found

        Returns:
            Setting value. If value is dict with numeric/default keys, returns
            row-specific value based on num_rows, otherwise returns value as-is.
        """
        keys = path.split('.')
        value = self.settings

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        # If value is a dict with row-based overrides, select appropriate value
        if isinstance(value, dict) and ('default' in value or any(isinstance(k, int) for k in value.keys())):
            return value.get(self._row_key, value.get('default', default))

        return value

    def get_dict(self, path: str) -> dict:
        """Get a dictionary setting without row-based resolution.

        Useful for accessing entire sections like 'figure' or 'page'.
        """
        keys = path.split('.')
        value = self.settings

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return {}

        return value if isinstance(value, dict) else {}
