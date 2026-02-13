# geo_data package

`geo_data` contains the data-layer implementation for `geo`.

## Responsibilities

- CDS/ERA5 retrieval clients (`cds_base.py`, `cds_temperature.py`, `cds_precipitation.py`)
- Measure-aware data retrieval orchestration (`data_retrieval.py` / `RetrievalCoordinator`)
- Cache path/read/write helpers (`cache_store.py` / `CacheStore`)
- Schema model and registry (`schema.py` / `Schema`)
- Schema support modules (`measure_mapping.py`, `cache_migration.py`, `cache_codec.py`)
- Cache schema registry (`schema.yaml`)
- Data-layer tests (`tests/`)

## Design boundary

`geo_data` is intentionally focused on data and schema concerns.
Plot rendering and plot orchestration live in `geo_plot` (`geo_plot/plot.py`, `geo_plot/orchestrator.py`).

## Key modules

- `cds_base.py`
  - `Location` dataclass and timezone handling
  - ERA5 request building/retrieval helpers
  - shared helpers used by measure clients
- `cds_temperature.py`
  - `TemperatureCDS` (daily local-noon temperature)
- `cds_precipitation.py`
  - `PrecipitationCDS` (daily precipitation)
- `data_retrieval.py`
  - retrieval orchestration class (`RetrievalCoordinator`)
- `cache_store.py`
  - cache path/read/write class (`CacheStore`)
- `schema.py`
  - schema model (`Schema`) and default loaded schema (`DEFAULT_SCHEMA`)
  - schema registry loading and core schema constants
- `measure_mapping.py`
  - measure-to-cache and measure-to-dataframe mapping helpers
- `cache_migration.py`
  - schema-version detection and migration helpers
- `cache_codec.py`
  - YAML read/write and migration codec helpers
- `schema.yaml`
  - schema registry and migration metadata for cached YAML files

## Schema API

- Preferred for new code: import from `geo_data.schema`.
- Package-level import also exposes `Schema` via `from geo_data import Schema`.
- Primary loader entry points are `Schema.load()` and `Schema.load_registry(...)`.
- Current schema supports `measure_cache_vars` and `measure_value_columns` mappings.
- Supporting modules: `geo_data.measure_mapping`, `geo_data.cache_migration`, and `geo_data.cache_codec`.

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
