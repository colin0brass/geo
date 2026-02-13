from geo_core.grid import calculate_grid_layout


def test_calculate_grid_layout_single():
    assert calculate_grid_layout(1, 4, 6) == (1, 1)


def test_calculate_grid_layout_two():
    assert calculate_grid_layout(2, 4, 6) == (1, 2)


def test_calculate_grid_layout_four():
    assert calculate_grid_layout(4, 4, 6) == (2, 2)


def test_calculate_grid_layout_six():
    assert calculate_grid_layout(6, 4, 6) == (2, 3)


def test_calculate_grid_layout_eight():
    rows, cols = calculate_grid_layout(8, 4, 6)
    assert rows == 3 and cols == 3


def test_calculate_grid_layout_ten():
    rows, cols = calculate_grid_layout(10, 4, 6)
    assert rows == 3 and cols == 4


def test_calculate_grid_layout_twelve():
    assert calculate_grid_layout(12, 4, 6) == (3, 4)


def test_calculate_grid_layout_sixteen():
    assert calculate_grid_layout(16, 4, 6) == (4, 4)


def test_calculate_grid_layout_twenty():
    rows, cols = calculate_grid_layout(20, 4, 6)
    assert rows == 4 and cols == 5


def test_calculate_grid_layout_zero():
    assert calculate_grid_layout(0, 4, 6) == (1, 1)


def test_calculate_grid_layout_custom_max_cols():
    rows, cols = calculate_grid_layout(10, max_rows=5, max_cols=3)
    assert cols <= 3
    assert rows * cols >= 10
