"""
Tests for orchestrator module (plot coordination and batching).
"""
import pytest
from orchestrator import calculate_grid_dimensions


def test_calculate_grid_dimensions_no_custom_grid():
    """Test automatic grid dimension calculation."""
    # Single place
    rows, cols, total = calculate_grid_dimensions(1, None)
    assert rows == 1 and cols == 1 and total == 1
    
    # 4 places
    rows, cols, total = calculate_grid_dimensions(4, None)
    assert rows == 2 and cols == 2 and total == 4
    
    # 6 places
    rows, cols, total = calculate_grid_dimensions(6, None)
    assert rows == 2 and cols == 3 and total == 6
    
    # 10 places (auto-calculated total equals num_places, not grid size)
    rows, cols, total = calculate_grid_dimensions(10, None)
    assert rows == 3 and cols == 4 and total == 10


def test_calculate_grid_dimensions_with_custom_grid():
    """Test grid dimensions with custom grid specification."""
    # Custom 3x4 grid
    rows, cols, total = calculate_grid_dimensions(8, (3, 4))
    assert rows == 3 and cols == 4 and total == 12
    
    # Custom 2x5 grid
    rows, cols, total = calculate_grid_dimensions(7, (2, 5))
    assert rows == 2 and cols == 5 and total == 10


def test_calculate_grid_dimensions_zero_places():
    """Test handling of zero places."""
    rows, cols, total = calculate_grid_dimensions(0, None)
    assert rows == 1 and cols == 1 and total == 0


# NOTE: Complex integration tests for create_batch_subplot, create_main_plots,
# create_individual_plot, and plot_all require extensive mocking of Visualizer
# class and matplotlib components. These functions are validated through:
# 1. End-to-end application runs
# 2. Manual testing with real data
# 3. Type checking and code review
#
# Unit testing these functions would require either:
# - Significant refactoring to inject dependencies
# - Complex mock setups that are brittle and hard to maintain
# - Integration test fixtures with real plotting libraries
#
# The calculate_grid_dimensions function above tests the core logic
# that drives the orchestration behavior.
