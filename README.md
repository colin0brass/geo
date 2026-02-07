
# geo_temp

**geo_temp** is a Python package for downloading, caching, analyzing, and visualizing ERA5 temperature data, with a focus on daily local noon temperatures for specified locations. It supports batch processing, flexible configuration, and publication-quality polar plots.

---

## Features
- ✨ Download and cache ERA5 2m temperature data for any location
- 📊 Generate publication-quality polar plots of annual temperature cycles
- 🌍 **55 pre-configured global locations** with 13 thematic place lists
- 🕐 **Automatic timezone detection** from coordinates (no manual lookup needed)
- 🎯 Smart grid layout with automatic batching for large datasets
- ⚙️ Highly configurable plotting via YAML settings
- 🚀 **Two-tier caching system**: NetCDF files (raw) and YAML data files (processed, git-friendly)
- 💻 User-friendly CLI with short options and argument validation
- 🐍 Clean Python API for programmatic use
- 📊 Real-time progress bars with place/year numbering during data downloads
- ✅ Comprehensive test suite with 124 tests (90% coverage)

---

## Quickstart


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
python geo_temp.py -y 2024 -s

# List available places
python geo_temp.py -l

# Show help and version
python geo_temp.py --help
python geo_temp.py --version
```

**Single location:**
```bash
python geo_temp.py -p "Austin, TX" -y 2020-2025 -s main
```

**Predefined place list:**
```bash
# Use default list
python geo_temp.py -L -y 2024 -s

# Use specific list
python geo_temp.py -L extreme_range -y 2024 -s
```

**All locations:**
```bash
python geo_temp.py -a -y 2024 -s main
```

**Custom location (timezone auto-detected):**
```bash
python geo_temp.py -p "Custom Location" --lat 40.7128 --lon -74.0060 -y 2024
```

**Specify grid layout:**
```bash
# 4 columns by 3 rows = 12 places max per image
python geo_temp.py -L -y 2024 --grid 4x3

# If places exceed grid capacity, multiple images are generated
python geo_temp.py -a -y 2024 --grid 4x4
# Creates: Overall_noon_temps_polar_2024_2024_part1of2.png (16 places)
#          Overall_noon_temps_polar_2024_2024_part2of2.png (remaining places)
```

**Advanced options:**
```bash
# Add a new place to config (looks up coordinates automatically)
python geo_temp.py --add-place "Seattle, WA"

# Dry-run mode (preview without executing)
python geo_temp.py -a -y 2024 --dry-run

# Verbose logging
python geo_temp.py -p "Austin, TX" -y 2024 -v

# Quiet mode (errors only)
python geo_temp.py -a -y 2024 -q
```


### 4. Use as a Python module

```python
from datetime import date
from geo_temp import read_data_file, save_data_file
from cds import CDS, Location
from plot import Visualizer

# Timezone is auto-detected from coordinates
loc = Location(name="Austin, TX", lat=30.2672, lon=-97.7431)
# Or explicitly specify: loc = Location(name="Austin, TX", lat=30.2672, lon=-97.7431, tz="America/Chicago")
cds = CDS()
df = cds.get_noon_series(loc, start_d=date(2020,1,1), end_d=date(2020,12,31))
vis = Visualizer(df)
vis.plot_polar(title="Austin 2020 Noon Temps", save_file="output/austin_2020.png")
```

---

## Project Structure
- `geo_temp.py`: Main entry point and orchestration
- `cli.py`: Command-line argument parsing, configuration loading, and validation
- `config_manager.py`: Configuration file management and place geocoding
- `cds.py`: ERA5 data download, caching, and processing
- `plot.py`: Polar plotting and visualization (Visualizer class)
- `data.py`: Data retrieval and I/O operations
- `orchestrator.py`: Plot coordination and batching
- `progress.py`: Progress reporting system with callback handlers
- `logging_config.py`: Centralized logging configuration
- `config.yaml`: Application configuration (places, logging)
- `settings.yaml`: Plot styling configuration
- `era5_cache/`: Cached NetCDF files (auto-created)
- `data_cache/`: Cached YAML data files (auto-created)
- `output/`: Generated plots (auto-created)
- `tests/`: Test suite with 124 tests across 8 modules

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
python geo_temp.py --add-place "Seattle, WA"

# List all available places and place lists
python geo_temp.py -l

# Show version
python geo_temp.py --version

# Show help
python geo_temp.py --help
```

### Location Options (choose one)

| Option | Short | Description |
|--------|-------|-------------|
| `--place NAME` | `-p` | Single configured or custom place |
| `--list [NAME]` | `-L` | Predefined place list. Use `-L` for 'default' list, or `-L NAME` for specific list |
| `--all` | `-a` | All configured places |

### Custom Location

```bash
# Add a new place to config (looks up coordinates)
python geo_temp.py --add-place "Seattle, WA"

# Use custom coordinates without adding to config
python geo_temp.py -p "MyCity" --lat 40.7 --lon -74.0 -y 2024
# Timezone auto-detected; use --tz to override
```

### Time Period

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--years YEARS` | `-y` | Year or range (e.g., 2024 or 2020-2025) | Previous year |

### Display Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--show [MODE]` | `-s` | Display plots on screen: none/main/all | none |
| `--grid COLSxROWS` | | Manual grid (e.g., 4x3) | auto |

**Notes:**
- `-s` without argument defaults to "main" (opens the combined subplot)
- `-s all` opens all plots (combined + individual)
- Plots open in your system's default image viewer (not matplotlib windows)

### Output Options

| Option | Default | Description |
|--------|---------|-------------|
| `--out-dir DIR` | output | Output directory for plots |
| `--cache-dir DIR` | era5_cache | Cache directory for NetCDF files |
| `--data-cache-dir DIR` | data_cache | Cache directory for YAML data files |
| `--settings FILE` | settings.yaml | Plot settings YAML file |

### Advanced Options

| Option | Short | Description |
|--------|-------|-------------|
| `--dry-run` | | Preview without downloading/plotting |
| `--verbose` | `-v` | Show DEBUG messages on console (log file always at DEBUG) |
| `--quiet` | `-q` | Show only errors on console (log file unaffected) |

**Note:** The log file (`geo_temp.log`) always captures all DEBUG messages regardless of console verbosity. It's cleared at the start of each run.

---

## Configuration

### config.yaml

Stores application configuration with two main sections:

#### 1. Logging
```yaml
logging:
  log_file: geo_temp.log
  console_level: WARNING  # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

#### 2. Places

**default_place:** Used when no location is specified
```yaml
places:
  default_place: Cambridge, UK
```

**all_places:** All available locations (compact format)
```yaml
places:
  all_places:
    - {name: "Austin, TX", lat: 30.2672, lon: -97.7431}
    - {name: "Cambridge, UK", lat: 52.2053, lon: 0.1218}
    # Add more places...
```

**Note:** Timezones are **automatically detected** from coordinates using `timezonefinder`. Add `tz` field only to override.

**place_lists:** Named groups for convenience
```yaml
places:
  place_lists:
    default:  # Used when calling -L without argument
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
    # ... plus 10 more thematic lists (55 places total)
```

**Available place lists:**
- `default`: Curated selection (used by `-L` without argument)
- `us_cities`, `international`: Regional selections
- `extreme_range`, `minimal_range`: Temperature variation comparison
- `arctic_circle`, `tropical`, `desert`, `mediterranean`: Climate zones
- `southern_hemisphere`: Southern hemisphere cities
- `tech_hubs`: Global technology centers
- `europe_north`, `europe_south`, `us_west`, `asia_pacific`: Regional groups
- `comparison_similar_lat`: Cities at similar latitude (~52°N)

### settings.yaml

Controls plot appearance and styling. Edit to customize:
- Figure size and DPI
- Fonts (title, labels, radial text)
- Colors and colormaps
- Subplot spacing and margins

See comments in the file for detailed options.

---

## Output Files

**Plots** are saved in `output/` directory (configurable with `--out-dir`):
- Individual plots: `Austin_TX_noon_temps_polar_2020_2025.png`
- Combined subplot: `Overall_noon_temps_polar_2020_2025.png`

**Data cache files (YAML)** are cached in `data_cache/` directory (configurable with `--data-cache-dir`):
- `Austin_TX_noon_temps.yaml` - Hierarchical format with place metadata and temperatures
- Format: Place info (name, coordinates, timezone) + temperatures organized by year/month/day
- Compact format: 1 line per month for 31% size reduction
- Git-friendly with clear diffs when adding new data

Example YAML structure:
```yaml
place:
  name: "Austin, TX"
  lat: 30.27
  lon: -97.74
  timezone: "America/Chicago"
  grid_lat: 30.268
  grid_lon: -97.7435
temperatures:
  2025:
    1:  # January
      1: 12.74
      2: 13.60
      # ... continues for all days
    2:  # February
      1: 18.12
      # ... continues for all months
```

**NetCDF files** are cached in `era5_cache/` directory (configurable with `--cache-dir`):
- Raw ERA5 monthly data files for each location

---

## Testing

Run tests with pytest:

```bash
pytest                    # All tests
pytest tests/test_cli.py  # Specific module
pytest -v                 # Verbose output
pytest -k "timezone"      # Run tests matching pattern
```

**Total: 124 tests** with 90% code coverage across 8 modules:

| Module | Tests | Coverage |
|--------|-------|----------|
| test_cli.py | 40 | Argument parsing, grid layout, validation |
| test_orchestrator.py | 12 | Plot coordination, batching, integration |
| test_config_manager.py | 11 | Config loading, saving, place management |
| test_visualizer.py | 11 | Polar plots, single/multi-subplot layouts |
| test_progress.py | 13 | Progress handlers, place/year numbering |
| test_data.py | 20 | Data I/O, retrieval, YAML caching, CDS summary |
| test_logging_config.py | 10 | Logging setup and configuration |
| test_cds.py | 8 | CDS API, Location, timezone auto-detection |

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
python geo_temp.py -a -y 2024 --grid 4x4
# Outputs:
#   Overall_noon_temps_polar_2024_part1of2.png (16 places)
#   Overall_noon_temps_polar_2024_part2of2.png (remaining places)
```

### Smart Scaling

For grids with 3+ rows, automatic scaling prevents subplot overlap:
- **Subplot dimensions:** 62% height, 83% width
- **Spacing:** hspace=0.25, optimized margins
- **Fonts:** Scaled proportionally
- **Format:** Optimized for A3 landscape

No configuration needed—works automatically.

### Dry-Run Mode

Preview operations without execution:

```bash
python geo_temp.py -a -y 2024 --dry-run
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
**Solution:** Use `-s` or `-s main` to display on screen, or check `output/` directory for saved files.

**Issue:** Tests failing  
**Solution:** Ensure Python 3.9+ and all dependencies installed. Run `pytest -v` for details.

---

## Version

**Current version:** 1.0.0

Check version: `python geo_temp.py --version`

---

## License

MIT License
