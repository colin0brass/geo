import pytest

from geo_core.grid import calculate_grid_layout


@pytest.mark.parametrize(
    "num_places,expected_rows,expected_cols",
    [
        (1, 1, 1),
        (2, 1, 2),
        (4, 2, 2),
        (6, 2, 3),
        (8, 3, 3),
        (10, 3, 4),
        (12, 3, 4),
        (16, 4, 4),
    ],
)
def test_grid_layout_expected_shapes(num_places, expected_rows, expected_cols):
    rows, cols = calculate_grid_layout(num_places)
    assert (rows, cols) == (expected_rows, expected_cols)


@pytest.mark.parametrize("num_places", [1, 2, 4, 6, 8, 9, 10, 12, 16])
def test_grid_layout_has_capacity(num_places):
    rows, cols = calculate_grid_layout(num_places)
    assert rows * cols >= num_places
