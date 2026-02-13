# geo_data package

`geo_data` contains the data-layer implementation for `geo`.

## Responsibilities

- CDS/ERA5 retrieval client (`cds.py`)
- Measure-aware data retrieval and cache orchestration (`data.py`)
- Cache schema registry (`schema.yaml`)
- Data-layer tests (`tests/`)

## Design boundary

`geo_data` is intentionally focused on data and schema concerns.
Plot rendering and plot orchestration remain in top-level modules (`plot.py`, `orchestrator.py`).

## Key modules

- `cds.py`
  - `Location` dataclass and timezone handling
  - ERA5 request building/retrieval helpers
  - measure retrieval methods (e.g. noon temperature, daily precipitation)
- `data.py`
  - cache read/write/merge utilities
  - schema migration + validation
  - measure routing (`retrieve_and_concat_data`)
- `schema.yaml`
  - schema registry and migration metadata for cached YAML files

## Tests

Run only data-layer tests:

```bash
python -m pytest geo_data/tests
```

Run targeted modules:

```bash
python -m pytest geo_data/tests/test_data.py
python -m pytest geo_data/tests/test_cds.py
```

Live CDS test remains opt-in:

```bash
GEO_RUN_LIVE_CDS=1 python -m pytest geo_data/tests/test_cds_live.py -ra
```
