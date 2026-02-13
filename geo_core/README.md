# geo_core package

`geo_core` contains shared, dependency-light helpers used by multiple geo layers.

## Responsibilities

- Shared configuration service and loading helpers (`config.py`)
- Shared configuration constants and required-key sets (`constants.py`)
- Shared formatting helpers (`formatting.py`)
- Shared grid layout logic (`grid.py`)
- Shared progress primitives (`progress.py`)
- Core-layer tests (`tests/`)

## Design boundary

`geo_core` is intentionally small and reusable.
It should not depend on app entrypoints, plotting internals, or CDS retrieval internals.

Current upstream users include:
- `cli.py` (CLI-facing wrappers)
- `geo_data/*` (retrieval settings and shared config constants)
- `geo_plot/orchestrator.py` (grid and text/config helpers)
- `config_manager.py` (place config extraction and canonical YAML rendering helpers)

## Key modules

- `config.py`
  - stateful config access via `CoreConfigService`
  - colour mode/colormap resolution
  - grid/runtime/retrieval settings loading
  - plot text and measure label metadata helpers
  - place-config extraction and lookup helpers
  - canonical config YAML rendering helper
- `constants.py`
  - shared defaults used by config loaders
  - required key sets for config validation
  - valid enum/value sets (for example colour modes)
- `formatting.py`
  - shared value/text formatting helpers
  - year-range condensation (`condense_year_ranges`)
- `grid.py`
  - grid shape computation (`calculate_grid_layout`)
- `progress.py`
  - callback protocol (`ProgressHandler`)
  - progress manager + singleton accessor (`ProgressManager`, `get_progress_manager`)

## Tests

Run core-layer tests:

```bash
python -m pytest geo_core/tests
```

Run targeted modules:

```bash
python -m pytest geo_core/tests/test_config.py
python -m pytest geo_core/tests/test_formatting.py
python -m pytest geo_core/tests/test_grid.py
python -m pytest geo_core/tests/test_grid_layout.py
python -m pytest geo_core/tests/test_progress.py
```
