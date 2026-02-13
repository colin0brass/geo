# geo_core package

`geo_core` contains shared, dependency-light helpers used by multiple geo layers.

## Responsibilities

- Shared configuration loading/formatting helpers (`config.py`)
- Shared grid layout logic (`grid.py`)
- Core-layer tests (`tests/`)

## Design boundary

`geo_core` is intentionally small and reusable.
It should not depend on app entrypoints, plotting internals, or CDS retrieval internals.

Current upstream users include:
- `cli.py` (CLI-facing wrappers)
- `geo_plot/orchestrator.py` (grid and text/config helpers)
- `config_manager.py` (compatibility wrappers for plot text helpers)

## Key modules

- `config.py`
  - colour mode/colormap resolution
  - grid settings loading
  - plot text and measure label metadata helpers
- `formatting.py`
  - shared value/text formatting helpers
  - year-range condensation (`condense_year_ranges`)
- `grid.py`
  - grid shape computation (`calculate_grid_layout`)

## Tests

Run core-layer tests:

```bash
python -m pytest geo_core/tests
```

Run targeted modules:

```bash
python -m pytest geo_core/tests/test_config.py
python -m pytest geo_core/tests/test_grid.py
```
