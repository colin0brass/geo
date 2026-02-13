"""Grid layout helpers shared across CLI and plotting layers."""

from __future__ import annotations

import math


def calculate_grid_layout(num_places: int, max_rows: int = 4, max_cols: int = 6) -> tuple[int, int]:
    """
    Calculate optimal grid layout (rows, columns) for subplot arrangement.

    Prioritizes balanced aspect ratio while limiting maximum grid size for readability.
    If places exceed max capacity, they should be batched into multiple images.

    Args:
        num_places: Number of subplots to arrange.
        max_rows: Maximum number of rows allowed (default 4).
        max_cols: Maximum number of columns allowed (default 6).

    Returns:
        tuple[int, int]: (num_rows, num_cols) for the grid layout.
    """
    if num_places == 0:
        return (1, 1)

    max_grid_size = max_rows * max_cols
    places_to_fit = min(num_places, max_grid_size)

    if places_to_fit <= 2:
        return (1, places_to_fit)
    if places_to_fit <= 4:
        return (2, 2)

    num_cols = min(max_cols, math.ceil(math.sqrt(places_to_fit)))
    num_rows = math.ceil(places_to_fit / num_cols)

    if num_rows > max_rows:
        num_rows = max_rows
        num_cols = min(max_cols, math.ceil(places_to_fit / num_rows))

    for cols in range(num_cols, 0, -1):
        rows = math.ceil(places_to_fit / cols)

        if rows > max_rows:
            continue

        empty_spaces = (rows * cols) - places_to_fit

        if empty_spaces <= cols and rows <= cols + 1:
            return (rows, cols)

    return (num_rows, num_cols)
