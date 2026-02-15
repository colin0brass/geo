
# geo

**geo** is a Python package for downloading, caching, analyzing, and visualizing ERA5 climate data, with support for daily local-noon temperature, daily precipitation, and daily solar-radiation energy for specified locations. It supports batch processing, flexible configuration, and publication-quality polar plots.

---

## Features
- ✨ Download and cache ERA5 temperature, precipitation, and solar radiation data for any location
- 📊 Generate publication-quality polar plots of annual climate cycles
- 🌍 **55 pre-configured global locations** with 13 thematic place lists
- 🕐 **Automatic timezone detection** from coordinates (no manual lookup needed)
- 🎯 Smart grid layout with automatic batching for large datasets
- ⚙️ Highly configurable plotting via YAML settings
- 🚀 **Two-tier caching system**: NetCDF files (raw) and YAML data files (processed, git-friendly)
- 💻 User-friendly CLI with short options and argument validation
- 🐍 Clean Python API for programmatic use
- 📊 Real-time progress bars with place/year numbering during data downloads
- ✅ Comprehensive automated test suite

---

## Quickstart

Package docs:
- Core shared helpers: [geo_core/README.md](geo_core/README.md)
- Data layer: [geo_data/README.md](geo_data/README.md)
- Plot layer: [geo_plot/README.md](geo_plot/README.md)


### 1. Register for CDS API and install API key

To download ERA5 data, you must register for a free account and install your API key. See the [CDS API registration and setup instructions](https://cds.climate.copernicus.eu/api).

### 2. Install dependencies

```bash
pip install -r requirements.txt
```


### 3. Download and plot data (CLI)

**Quick start:**
```bash
# Default place, display plots on screen
python geo.py -y 2024 -s

# List available places
python geo.py -L

# Show help and version
python geo.py --help
python geo.py --version
```

**Single location:**
```bash
python geo.py -p "Austin, TX" -y 2020-2025 -s
```

**Predefined place list:**
```bash
# Use default list
python geo.py -l -y 2024 -s

# Use specific list
python geo.py -l extreme_range -y 2024 -s

# Alias for --all
python geo.py -l all -y 2024 -s
```

**All locations:**
```bash
python geo.py -a -y 2024 -s
```

**Custom location (timezone auto-detected):**
```bash
python geo.py -p "Custom Location" --lat 40.7128 --lon -74.0060 -y 2024
```

**Specify grid layout:**
```bash
# 4 columns by 3 rows = 12 places max per image
python geo.py -l -y 2024 --grid 4x3

# If places exceed grid capacity, multiple images are generated
python geo.py -a -y 2024 --grid 4x4
# Creates: all_noon_temperature_2024_2024_part1of2.png (16 places)
#          all_noon_temperature_2024_2024_part2of2.png (remaining places)
```

**Advanced options:**
```bash
# Add a new place to config (looks up coordinates automatically)
python geo.py --add-place "Seattle, WA"

# Colour points by year to reveal long-term trend shifts
python geo.py -p "Austin, TX" -y 1990-2025 --colour-mode year

# Choose data measure
python geo.py -p "Austin, TX" -y 2024 --measure noon_temperature

# Run multiple measures in one command
python geo.py -p "Austin, TX" -y 2024 -m temp,precipitation

# Run all supported measures
python geo.py -p "Austin, TX" -y 2024 -m all

# Note: noon_temperature, daily_precipitation, and daily_solar_radiation_energy are configured to use timeseries retrieval in the bundled config.

# Force monthly or yearly download chunking for this run
python geo.py -p "Austin, TX" -y 2024 --download-by month
python geo.py -p "Austin, TX" -y 2024 --download-by year

# Benchmark month vs year chunking (single year only)
python geo.py -l preferred -y 2024 --measure daily_precipitation --download-by compare

# Overwrite existing cached values for matching dates
python geo.py -p "Austin, TX" -y 2024 --update-cache

# Dry-run mode (preview without executing)
python geo.py -a -y 2024 --dry-run

# Verbose logging
python geo.py -p "Austin, TX" -y 2024 -v

# Quiet mode (errors only)
python geo.py -a -y 2024 -q
```


### 4. Use as a Python module

```python
from datetime import date
from geo_data.cds_base import Location
from geo_data.cds_temperature import TemperatureCDS
from geo_data.data_retrieval import RetrievalCoordinator
from geo_plot.visualizer import Visualizer

# Timezone is auto-detected from coordinates
loc = Location(name="Austin, TX", lat=30.2672, lon=-97.7431)
# Or explicitly specify: loc = Location(name="Austin, TX", lat=30.2672, lon=-97.7431, tz="America/Chicago")
cds = TemperatureCDS()
df = cds.get_noon_series(loc, start_d=date(2020,1,1), end_d=date(2020,12,31))
vis = Visualizer(df)
vis.plot_polar(title="Austin 2020 Noon Temps", save_file="output/austin_2020.png")

# Or use class-based orchestration for retrieval + cache merge
coordinator = RetrievalCoordinator()
df_all = coordinator.retrieve([loc], 2020, 2020, measure="noon_temperature")
```

---

## Project Structure
- `geo.py`: Main entry point and orchestration
- `cli.py`: Command-line argument parsing, configuration loading, and validation
- `config_manager.py`: Configuration file management and place geocoding
- `progress.py`: Progress reporting system with callback handlers
- `logging_config.py`: Centralized logging configuration
- `geo_core/`: Shared core helpers (config/grid/progress utilities and core tests)
  - `geo_core/config.py`: Shared config service and config/text/place-config helpers
  - `geo_core/constants.py`: Shared config defaults, required-key sets, and enum constants
  - `geo_core/grid.py`: Shared grid layout logic
  - `geo_core/progress.py`: Shared progress protocol and manager
  - `geo_core/tests/`: Core-layer tests
- `geo_data/`: Data-layer package (CDS client, cache pipeline, schema, and data tests)
  - `geo_data/cds_base.py`: Shared ERA5 retrieval primitives + `Location`
  - `geo_data/cds_temperature.py`: Temperature retrieval client (`TemperatureCDS`)
  - `geo_data/cds_precipitation.py`: Precipitation retrieval client (`PrecipitationCDS`)
  - `geo_data/cds_solar_radiation.py`: Solar-radiation retrieval client (`SolarRadiationCDS`)
  - `geo_data/data_retrieval.py`: Retrieval orchestration (`RetrievalCoordinator`)
  - `geo_data/cache_store.py`: Cache path/read/write helpers (`CacheStore`)
  - `geo_data/schema.py`: Schema model + registry constants (`Schema`, `DEFAULT_SCHEMA`)
  - `geo_data/measure_mapping.py`: Measure-to-cache and measure-to-dataframe mappings
  - `geo_data/cache_migration.py`: Schema-version detection and migration helpers
  - `geo_data/cache_codec.py`: Cache read/write and migration codec helpers
  - `geo_data/schema.yaml`: Cache schema registry and migration metadata
  - `geo_data/tests/`: Data-layer tests
- `geo_plot/`: Plot-layer package (visualization, orchestration, and plot tests)
  - `geo_plot/visualizer.py`: Polar plotting (`Visualizer`)
  - `geo_plot/settings_manager.py`: Plot settings accessor with row-based scaling
  - `geo_plot/settings.yaml`: Plot styling configuration
  - `geo_plot/orchestrator.py`: Plot coordination and batching
  - `geo_plot/tests/`: Plot-layer tests
- `config.yaml`: Application configuration (places, logging, grid, runtime paths, retrieval, plotting, and text templates)
- `era5_cache/`: Cached NetCDF files (auto-created)
- `data_cache/`: Cached YAML data files (auto-created)
- `output/`: Generated plots (auto-created)
- `tests/`: Application-layer tests (CLI, config, progress, logging)

Package docs:
- `geo_core/README.md`
- `geo_data/README.md`
- `geo_plot/README.md`

---

## Requirements

**Python:** 3.9+ (required for zoneinfo)

**Dependencies:**
- cdsapi
- matplotlib
- numpy
- pandas
- pyyaml
- pytest
- xarray
- netcdf4
- dask
- timezonefinder
- geopy

**Installation:**

1. Create and activate a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate  # On macOS/Linux
# or .venv\Scripts\activate on Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

---

## CLI Reference

### Quick Commands

```bash
# Add a new place (looks up coordinates online)
python geo.py --add-place "Seattle, WA"

# List all available places and place lists
python geo.py -L

# Alias for --all (all configured places)
python geo.py -l all -y 2024

# Show version
python geo.py --version

# Show help
python geo.py --help
```

### Location Options (choose one)

| Option | Short | Description |
|--------|-------|-------------|
| `--place NAME` | `-p` | Single configured or custom place |
| `--list [NAME]` | `-l` | Predefined place list. Use `-l` for 'default', `-l NAME` for specific list, or `-l all` as an alias for `--all` |
| `--all` | `-a` | All configured places |

### Information Options

| Option | Short | Description |
|--------|-------|-------------|
| `--list-places` | `-L` | List all available places and place lists, then exit |
| `--add-place NAME` | | Add a new place to config (looks up coordinates online) |

### Custom Location

```bash
# Add a new place to config (looks up coordinates)
python geo.py --add-place "Seattle, WA"

# Use custom coordinates without adding to config
python geo.py -p "MyCity" --lat 40.7 --lon -74.0 -y 2024
# Timezone auto-detected; use --tz to override
```

### Time Period

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--years YEARS` | `-y` | Year or range (e.g., 2024 or 2020-2025) | Previous year |

### Data Measure

| Option | Description | Default |
|--------|-------------|---------|
| `--measure VALUE` | Select one or more measures. Use canonical values (`noon_temperature`, `daily_precipitation`, `daily_solar_radiation_energy`), aliases (`temp`, `temperature`, `precipitation`, `solar`), comma-separated values (`temp,solar`), or `all`. | `noon_temperature` |

### Display Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--show` | `-s` | Display plots on screen after generation | off |
| `--grid COLSxROWS` | `-g` | Manual grid (e.g., 4x3) | auto |
| `--colour-mode {y_value,colour_value,year}` / `--color-mode {y_value,colour_value,year}` | `—` | Point colouring mode (`y_value`, `colour_value`, or `year`) | from per-measure config |

**Notes:**
- Individual plots are only created for single places (using `--place`)
- Place lists (`--all`, `--list`) only create combined subplot images
- Combined plots use the list name in filenames:
  - Single place: `Austin_TX_noon_temperature_2020_2025.png`
  - Place list: `default_noon_temperature_2020_2025.png`
  - All places: `all_noon_temperature_2020_2025.png`

### Paths and Files

| Option | Default | Description |
|--------|---------|-------------|
| `--out-dir DIR` | output | Output directory for plots |
| `--cache-dir DIR` | era5_cache | Cache directory for NetCDF files |
| `--data-cache-dir DIR` | data_cache | Cache directory for YAML data files |
| `--settings FILE` | geo_plot/settings.yaml | Plot settings YAML file |

**Cache schema note:** YAML cache files in `data_cache/` must include a supported `schema_version`.
Unversioned legacy cache documents are no longer auto-migrated; regenerate them by rerunning retrieval, or convert them to a supported versioned schema before use.

### Advanced Options

| Option | Short | Description |
|--------|-------|-------------|
| `--dry-run` | | Preview without downloading/plotting |
| `--download-by {config,month,year,compare}` | | Override retrieval chunking for this run, or benchmark month vs year for one year |
| `--update-cache` | `-u` | Overwrite existing cached values when newly retrieved data has matching dates |
| `--verbose` | `-v` | Show DEBUG messages on console (log file always at DEBUG) |
| `--quiet` | `-q` | Show only errors on console (log file unaffected) |

**Note:** The log file (`geo.log`) always captures all DEBUG messages regardless of console verbosity. It's cleared at the start of each run.

---

## Configuration

### config.yaml

Stores application configuration with seven main sections:

#### 1. Logging
```yaml
schema_version: 1

logging:
  log_file: geo.log
  console_level: WARNING  # DEBUG, INFO, WARNING, ERROR, CRITICAL
  file_mode: w            # w=overwrite each run, a=append
  suppress_cdsapi: true
  cds_warnings_in_verbose: true
  suppress_root_logger: true
  third_party_log_level: WARNING
```

#### 2. Grid Layout
```yaml
grid:
  max_auto_rows: 4    # Maximum rows for auto-calculated grids
  max_auto_cols: 6    # Maximum columns for auto-calculated grids
```

These settings determine the maximum grid size (default 4×6 = 24 places) when the grid is auto-calculated. If you have more places than this, the system will automatically batch them into multiple images.

**Note:** The `--grid` command-line option always overrides these configuration settings.

#### 3. Retrieval Tuning

```yaml
retrieval:
  half_box_deg: 0.25
  max_nearest_time_delta_minutes: 30
  month_fetch_day_span_threshold: 62
  wet_hour_threshold_mm: 1.0
  measures:
    noon_temperature:
      fetch_mode: auto
      daily_source: timeseries
    daily_precipitation:
      fetch_mode: auto
      daily_source: timeseries
    daily_solar_radiation_energy:
      fetch_mode: auto
      daily_source: timeseries
```

- `half_box_deg`: geographic half-width (degrees) for ERA5 retrieval around each location.
- `max_nearest_time_delta_minutes`: maximum tolerated offset between requested local noon and selected ERA5 time.
- `month_fetch_day_span_threshold`: day-range threshold for monthly fetch strategy before switching to full-year fetch.
- `wet_hour_threshold_mm`: hourly precipitation threshold (mm) used to count a wet hour in `wet_hours_per_day`.
- `measures.<measure>.fetch_mode`: `auto`, `monthly`, or `yearly` retrieval chunking.
- `measures.noon_temperature.daily_source`: `timeseries` or `hourly`.
- `measures.daily_precipitation.daily_source`: `timeseries`, `hourly`, or `daily_statistics`.
- `measures.daily_solar_radiation_energy.daily_source`: `timeseries` or `hourly`.

#### 4. Runtime Paths

```yaml
runtime_paths:
  cache_dir: era5_cache
  data_cache_dir: data_cache
  out_dir: output
  settings_file: geo_plot/settings.yaml

places_file: places.yaml
```

- `cache_dir`: default NetCDF cache directory for ERA5 files.
- `data_cache_dir`: default YAML cache directory.
- `out_dir`: default plot output directory.
- `settings_file`: default plot settings YAML file.
- `places_file`: path to external places configuration file.

#### 5. Plotting

```yaml
plotting:
  measures:
    noon_temperature:
      label: "Mid-Day Temperature"
      unit: "°C"
      colour_mode: y_value  # y_value, colour_value, or year
      y_value_column: "temp_C"
      y: {min: null, max: null, step: 10, max_steps: 4}
      range_text: "{min_temp_c:.1f}°C to {max_temp_c:.1f}°C; ({min_temp_f:.1f}°F to {max_temp_f:.1f}°F)"
    daily_precipitation:
      label: "Wet Hours per Day"
      unit: "h"
      colour_mode: colour_value
      y_value_column: "wet_hours_per_day"
      colour_value_column: "max_hourly_precip_mm"  # optional; colour by different column than y-axis
      colourbar_title: "mm/hr"                     # optional; colourbar title override
      y: {min: 0, max: 24, step: 2, max_steps: 4}
      plot_format: wedges
      wedge_width_scale: 1.5  # optional wedge-width multiplier (>1 makes wedges wider)
      range_text: "Max daily precipitation: {max_daily_precip_mm:.1f} mm ({max_daily_precip_in:.2f} in)"
    daily_solar_radiation_energy:
      label: "Daily Solar Radiation"
      unit: "MJ/m²"
      colour_mode: y_value
      y_value_column: "solar_energy_MJ_m2"
      y: {min: 0, max: null, step: 10, max_steps: 4}
      range_text: "{measure_label}: {min_value:.1f} to {max_value:.1f} {measure_unit}"
  valid_colormaps: [turbo, viridis, plasma, inferno, magma, cividis]  # first item is default fallback
  colormap: turbo           # must be one of valid_colormaps
```

- `measures.*.colour_mode`: per-measure point colouring mode (`y_value`, `colour_value`, or `year`)
- `measures.*.y`: compact y-axis configuration mapping with optional keys:
  - `min`: optional fixed lower bound
  - `max`: optional fixed upper bound (omit for automatic data-driven max)
  - `step`: optional radial ring interval (default is internal)
  - `max_steps`: optional cap on radial ring/label count (auto-coarsens interval when exceeded)
- `measures.*.colour_value_column`: optional column used for colour mapping (defaults to `y_value_column`)
- `measures.*.colourbar_title`: optional colourbar title override (for example `mm`)
- `measures.*.plot_format`: `points` (default scatter) or `wedges`; `radial_bars` is accepted as an alias and normalized to `wedges`
- `measures.*.wedge_width_scale`: optional wedge-width multiplier (`> 0`); use values above `1.0` for wider wedges
- `valid_colormaps`: controls which colormaps are allowed and sets default fallback by first item
- `colormap`: controls the active colour palette used by both modes

**Note:** The `--colour-mode` command-line option overrides per-measure `colour_mode` for a run.

#### 6. Plot Text (Titles and Filenames)

Customize plot titles, filenames, and attribution text using format placeholders:

```yaml
plot_text:
  # Title patterns (use {location}, {year_range}, {start_year}, {end_year}, {batch}, {total_batches}, {measure_label}, {measure_key}, {measure_unit})
  single_plot_title: "{location} {measure_label} ({year_range})"
  overall_title: "{measure_label} ({year_range})"
  overall_title_with_batch: "{measure_label} ({year_range}) - Part {batch}/{total_batches}"
  subplot_title: "{location}"
  
  # Filename patterns (location names automatically sanitized for filenames)
  single_plot_filename: "{location}_{measure_key}_{start_year}_{end_year}.png"
  subplot_filename: "{list_name}_{measure_key}_{start_year}_{end_year}.png"
  subplot_filename_with_batch: "{list_name}_{measure_key}_{start_year}_{end_year}_part{batch}of{total_batches}.png"
  
  # Credit and data source text
  credit: "Mid-Day Temperature Analysis & Visualisation by Colin Osborne"
  data_source: "Data from: ERA5 via CDS"
  single_plot_credit: "Analysis & visualisation by Colin Osborne"
```

**Available placeholders:**
- `{location}` - Place name (auto-sanitized in filenames: spaces→underscores, commas removed)
- `{list_name}` - Place list name (e.g., "default", "arctic", "all")
- `{start_year}` - Start year of data
- `{end_year}` - End year of data
- `{year_range}` - Compact year span (`YYYY` when start=end, otherwise `YYYY-YYYY`)
- `{batch}` - Current batch number (for multi-batch plots)
- `{total_batches}` - Total number of batches
- `{measure}` / `{measure_key}` - Measure key (for example: `noon_temperature`)
- `{measure_label}` - Human-readable measure label (for example: `Mid-Day Temperature`)
- `{measure_unit}` - Measure unit string (for example: `°C`, `mm`)

**Example customizations:**
```yaml
# Short titles
overall_title: "Noon Temps {year_range}"

# Different filename format
single_plot_filename: "{start_year}-{end_year}_{location}_noon.png"

# Internationalization
credit: "Analyse et visualisation par Colin Osborne"
data_source: "Données de: ERA5 via CDS"
```

#### 7. Places (External File)

Large places data is split into a separate file (default `places.yaml`) referenced from `config.yaml`:

```yaml
places_file: places.yaml
```

**default_place:** Used when no location is specified
```yaml
default_place: Cambridge, UK
```

**all_places:** All available locations (compact format)
```yaml
all_places:
  - {name: "Austin, TX", lat: 30.2672, lon: -97.7431}
  - {name: "Cambridge, UK", lat: 52.2053, lon: 0.1218}
    # Add more places...
```

**Note:** Timezones are **automatically detected** from coordinates using `timezonefinder`. Add `tz` field only to override.

**place_lists:** Named groups for convenience
```yaml
place_lists:
  default:  # Used when calling -l without argument
    - Austin, TX
    - Cambridge, UK
    - San Jose, CA
    - Bangalore, India
    - Trondheim, Norway
    - Beijing, China
  extreme_range:  # High annual temperature variation
    - Ulaanbaatar, Mongolia
    - Moscow, Russia
    - Montreal, Canada
  minimal_range:  # Stable equatorial climates
    - Singapore
    - Mumbai, India
    - Lima, Peru
  # ... plus additional thematic lists
```

**Available place lists:**
- `default`: Curated selection (used by `-l` without argument)
- `us_cities`, `international`: Regional selections
- `extreme_range`, `minimal_range`: Temperature variation comparison
- `arctic_circle`, `tropical`, `desert`, `mediterranean`: Climate zones
- `southern_hemisphere`: Southern hemisphere cities
- `tech_hubs`: Global technology centers
- `europe_north`, `europe_south`, `us_west`, `asia_pacific`: Regional groups
- `comparison_similar_lat`: Cities at similar latitude (~52°N)

### geo_plot/settings.yaml

Controls plot appearance and styling with automatic row-based scaling.

**Structure:**
- `polar_single`: Settings for single location plots
- `polar_subplot`: Settings for multi-location subplot grids

**Key features:**
- **Row-based dictionaries**: Many parameters automatically scale based on grid rows
  - Syntax: `{default: value_for_1-2_rows, 3: value_for_3_rows, 4: value_for_4+_rows}`
  - Examples: `marker_size`, `xtick_fontsize`, `hspace`, `top`, `bottom`
- **Fixed parameters**: Figure dimensions, DPI, colors remain constant
- **SettingsManager integration**: Automatically resolves correct values at runtime

**Customizable parameters:**
- Figure size and DPI (A3 landscape: 13.34" × 7.5")
- Row-scaled fonts (title, xticks, yticks, temperature labels)
- Row-scaled marker sizes and spacing
- Row-scaled subplot margins and dimensions
- Colors and colormaps

See comments in the file for detailed options and row-based configuration patterns.

---

## Output Files

**Plots** are saved in `output/` directory (configurable with `--out-dir`):
- Individual plots: `Austin_TX_noon_temperature_2020_2025.png`
- Combined subplot: `default_noon_temperature_2020_2025.png`

**Data cache files (YAML)** are cached in `data_cache/` directory (configurable with `--data-cache-dir`):
- Naming convention: `<Place_Name>.yaml` (for example: `Austin_TX.yaml`)
- Newly written files use schema v2 with place metadata, variable metadata, and data arrays
- Format: `schema_version` + place info + variables metadata + daily data organized by year/month/day
- Schema versions and keys are defined in `geo_data/schema.yaml` (currently includes v1 and v2; used for automatic migration, including v1→v2 field mappings)
- When an older cache file is loaded, geo automatically migrates it and saves it in the current schema before continuing
- Compact format: 1 line per month for 31% size reduction
- Git-friendly with clear diffs when adding new data

Schema registry notes (`geo_data/schema.yaml`):
- `required`: list of required key paths (dot notation) for a schema version
- `required_any_of`: list of alternative path groups where at least one path in each group must exist
- `primary_data_path` and `legacy_data_paths`: generic root-level data aliases used for legacy extraction/migration
- `migration.from_version` + `migration.field_mappings`: declarative mapping from older schema fields to current schema fields

Example YAML structure:
```yaml
schema_version: 2
place:
  name: "Austin, TX"
  lat: 30.27
  lon: -97.74
  timezone: "America/Chicago"
  grid_lat: 30.268
  grid_lon: -97.7435
variables:
  noon_temp_C:
    units: C
    source_variable: 2m_temperature
    source_dataset: reanalysis-era5-single-levels
    temporal_definition: daily_local_noon
    precision: 2
data:
  noon_temp_C:
    2025:
      1: {1: 12.74, 2: 13.60}
      2: {1: 18.12}
```

**NetCDF files** are cached in `era5_cache/` directory (configurable with `--cache-dir`):
- Raw ERA5 monthly data files for each location

---

## Testing

Run tests with pytest:

```bash
pytest                    # All tests
pytest tests/test_cli.py  # Specific application-layer module
pytest geo_core/tests/test_config.py  # Specific core-layer module
pytest geo_data/tests/test_data.py  # Specific retrieval/cache module
pytest -v                 # Verbose output
pytest -k "timezone"      # Run tests matching pattern
```

Pytest includes flake8 lint checks by default (`--flake8` is enabled in `pytest.ini`).
Use `pytest --no-flake8` to skip lint checks for a run.

Run lint checks with flake8:

```bash
python -m flake8 .
```

Lint settings are defined in `.flake8`.

### Local shell tip

If `flake8 .` fails in your shell but `python -m flake8 .` passes, your shell is likely not using the project virtual environment.

```bash
# Option 1: activate venv for the session
source .venv/bin/activate
flake8 .

# Option 2: always use module form (recommended)
python -m flake8 .

# Optional alias in your shell profile
alias flake8='python -m flake8'
```

### CI Commands

Use these commands in CI pipelines:

```bash
# Combined test + lint (default)
python -m pytest

# Lint only
python -m flake8 .

# Tests only (skip lint plugin)
python -m pytest --no-flake8
```

Tests are organized across dedicated modules:

| Module | Focus |
|--------|-------|
| test_cli.py | Argument parsing, place selection, validation, year condensing |
| test_config_manager.py | Config loading, saving, place management |
| test_progress.py | Progress handlers, place/year numbering |
| test_logging_config.py | Logging setup and configuration |

Core-layer test modules are under `geo_core/tests/`:

| Module | Focus |
|--------|-------|
| test_config.py | Shared config parsing/resolution helpers |
| test_formatting.py | Shared formatting helpers and year-range condensation |
| test_grid.py | Shared grid layout algorithm |
| test_grid_layout.py | Grid shape/capacity expectations |
| test_progress.py | Core progress manager/protocol behavior |

Plot-layer test modules are under `geo_plot/tests/`:

| Module | Focus |
|--------|-------|
| test_visualizer.py | Polar plots, colour modes, subplot layouts |
| test_orchestrator.py | Plot coordination, batching, integration |

Data-layer test modules are under `geo_data/tests/`:

| Module | Focus |
|--------|-------|
| test_data.py | Cache I/O, retrieval orchestration, YAML caching, schema migration |
| test_cds.py | CDS API, Location, timezone auto-detection |
| test_cds_live.py | Opt-in live CDS integration |

**Coverage highlights:**
- ✅ CLI argument parsing and validation (including mutually exclusive groups)
- ✅ Timezone auto-detection and explicit override
- ✅ Grid layout calculations (1x1, 2D, custom grids)
- ✅ Data retrieval and caching with YAML format (100% coverage)
- ✅ YAML error handling and data merging (corrupted files, key normalization)
- ✅ CDS retrieval summaries and progress display with place/year numbering
- ✅ Plot generation and orchestration (100% coverage)
- ✅ Progress reporting with callbacks (100% coverage)
- ✅ Configuration loading from YAML

---

## Advanced Features

### Automatic Grid Batching

When places exceed grid capacity, multiple images are automatically generated:

```bash
python geo.py -a -y 2024 --grid 4x4
# Outputs:
#   all_noon_temperature_2024_2024_part1of2.png (16 places)
#   all_noon_temperature_2024_2024_part2of2.png (remaining places)
```

### Smart Scaling

The plotting system automatically scales all visual parameters based on the number of rows in your grid:

**Row-based settings** (configured in `geo_plot/settings.yaml`):
- **1-2 rows:** Default values - larger markers (2.0), larger fonts (9-12pt), generous spacing
- **3 rows:** Medium scale - markers (0.5), fonts (7-9pt), tighter spacing (hspace=0.25)
- **4+ rows:** Compact scale - small markers (0.25), small fonts (6-7pt), optimized spacing (hspace=0.30)

**Automatically scaled parameters:**
- Marker size, tick fonts, title fonts, temperature label fonts
- Subplot spacing (hspace, wspace) and margins (top, bottom)
- Subplot dimensions (height_scale, width_scale)

All scaling is controlled via `geo_plot/settings.yaml` using row-based dictionaries:
```yaml
marker_size: {default: 2.0, 3: 0.5, 4: 0.25}
xtick_fontsize: {default: 9, 3: 7, 4: 6}
```

The `SettingsManager` class automatically selects the appropriate value based on your grid configuration. No manual scaling needed—works automatically.

### Dry-Run Mode

Preview operations without execution:

```bash
python geo.py -a -y 2024 --dry-run
# Shows: places, years, grid, directories
# Downloads/creates nothing
```

---

## Troubleshooting

**Issue:** `ImportError: No module named 'cdsapi'`  
**Solution:** Install dependencies: `pip install -r requirements.txt`

**Issue:** CDS API authentication error  
**Solution:** Ensure `~/.cdsapirc` is configured. See [CDS API setup](https://cds.climate.copernicus.eu/api).

**Issue:** Timezone detection fails  
**Solution:** Specify timezone explicitly: `--tz "America/New_York"`

**Issue:** Plots show as blank  
**Solution:** Use `-s`/`--show` to display on screen, or check `output/` directory for saved files.

**Issue:** `Unknown place list 'all'` in older versions  
**Solution:** Update to latest version where `--list all` works as an alias for `--all`, or use `--all` directly.

**Issue:** Tests failing  
**Solution:** Ensure Python 3.9+ and all dependencies installed. Run `pytest -v` for details.

---

## Version

**Current version:** 1.0.0

Check version: `python geo.py --version`

---

## License

MIT License
