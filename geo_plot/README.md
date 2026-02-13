# geo_plot package

`geo_plot` contains the plotting-layer implementation for `geo`.

## Responsibilities

- Polar plot rendering (`plot.py`, `Visualizer`)
- Plot orchestration and batching (`orchestrator.py`)
- Plot-layer tests (`tests/`)

## Design boundary

`geo_plot` is intentionally focused on visualization concerns.
Data retrieval, caching, and schema migration live in `geo_data`.

## Key modules

- `plot.py`
  - `Visualizer` class for single and multi-subplot polar charts
  - colour modes (`y_value`, `year`) and colormap handling
  - settings-driven rendering via `geo_plot/settings.yaml`
- `settings_manager.py`
  - row-aware settings resolution for plot/layout parameter scaling
- `orchestrator.py`
  - plot batching and grid coordination
  - title/filename text templating via config
  - main vs individual plot flow control

## Tests

Run plot-layer tests:

```bash
python -m pytest geo_plot/tests
```

Run targeted modules:

```bash
python -m pytest geo_plot/tests/test_visualizer.py
python -m pytest geo_plot/tests/test_orchestrator.py
```
