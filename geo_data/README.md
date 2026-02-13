# geo_data package

`geo_data` contains the data-layer implementation for `geo`.

## Responsibilities

- CDS/ERA5 retrieval clients (`cds_base.py`, `cds_temperature.py`, `cds_precipitation.py`)
- Measure-aware data retrieval orchestration (`data.py` / `RetrievalCoordinator`)
- Cache path/read/write helpers (`data_store.py` / `CacheStore`)
- Schema/migration helpers (`data_schema.py` / `CacheSchemaRegistry`)
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
- `data.py`
  - retrieval orchestration class (`RetrievalCoordinator`)
- `data_store.py`
  - cache path/read/write class (`CacheStore`)
- `data_schema.py`
  - schema/migration class (`CacheSchemaRegistry`)
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
