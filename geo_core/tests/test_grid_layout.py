#!/usr/bin/env python3
"""
Quick test script to visualize grid layout for debugging subplot overlap
"""

from geo_core.grid import calculate_grid_layout

print("Testing grid layout calculation:\n")

test_cases = [1, 2, 4, 6, 8, 9, 10, 12, 16]

for num_places in test_cases:
    rows, cols = calculate_grid_layout(num_places)
    empty_cells = (rows * cols) - num_places
    print(f"{num_places:2d} places → {rows}×{cols} grid ({rows*cols} cells, {empty_cells} empty)")

print("\n" + "="*60)
print("To test actual plotting with debug output:")
print("="*60)
print("\nFor 8 places (3×3 grid):")
print("python geo.py --list preferred --years 2024 --show none")
print("\nFor 6 places (2×3 grid):")
print("python geo.py --place Austin,TX Bangalore Cambridge,UK San_Jose Trondheim Beijing --years 2024 --show none")
