
# geo_temp

**geo_temp** is a Python package for downloading, caching, analyzing, and visualizing ERA5 temperature data, with a focus on daily local noon temperatures for specified locations. It supports batch processing, flexible configuration, and publication-quality polar plots.

---

## Features
- Download and cache ERA5 2m temperature data for any location
- Analyze temperature trends and statistics for custom date ranges
- Generate polar plots and subplots for visualizing annual temperature cycles
- Smart grid layout automatically adjusts for 3+ rows to prevent subplot overlap
- Manual grid specification with automatic batching for large datasets
- Highly configurable plotting (YAML settings)
- Efficient caching to avoid redundant downloads
- Command-line interface and Python API
- Comprehensive test suite with 54 tests

---

## Quickstart


### 1. Register for CDS API and install API key

To download ERA5 data, you must register for a free account and install your API key. See the [CDS API registration and setup instructions](https://cds.climate.copernicus.eu/api).

### 2. Install dependencies

```bash
pip install -r requirements.txt
```


### 3. Download and plot data (CLI)

**Default usage** (uses Cambridge, UK from config.yaml):
```bash
python geo_temp.py --years 2024 --show main
```

**Single location:**
```bash
python geo_temp.py --place "Austin, TX" --years 2020-2025 --show main
```

**Predefined place list:**
```bash
python geo_temp.py --place-list preferred --years 2024 --show main
```

**All locations:**
```bash
python geo_temp.py --all --years 2024 --show main
```

**Custom location:**
```bash
python geo_temp.py --place "Custom Location" --lat 40.7128 --lon -74.0060 --tz "America/New_York" --years 2024
```

**Specify grid layout:**
```bash
# 4 columns by 3 rows = 12 places max per image
python geo_temp.py --place-list european_capitals --years 2024 --grid 4x3

# If places exceed grid capacity, multiple images are generated
python geo_temp.py --all --years 2024 --grid 4x4
# Creates: Overall_noon_temps_polar_2024_2024_part1of2.png (16 places)
#          Overall_noon_temps_polar_2024_2024_part2of2.png (remaining places)
```


### 4. Use as a Python module

```python
from datetime import date
from geo_temp import read_data_file, save_data_file
from cds import CDS, Location
from plot import Visualizer

loc = Location(name="Austin, TX", lat=30.2672, lon=-97.7431, tz="America/Chicago")
cds = CDS()
df = cds.get_noon_series(loc, start_d=date(2020,1,1), end_d=date(2020,12,31))
vis = Visualizer(df)
vis.plot_polar(title="Austin 2020 Noon Temps", save_file="output/austin_2020.png")
```

---

## Project Structure
- `geo_temp.py`: Main CLI and orchestration module
- `cds.py`: ERA5 data download, caching, and processing
- `plot.py`: Polar plotting and visualization utilities
- `cli.py`: Command-line argument parsing and configuration loading
- `data.py`: Data retrieval and I/O operations
- `orchestrator.py`: Plot coordination and batching
- `logging_config.py`: Centralized logging configuration
- `config.yaml`: Application configuration (places, logging)
- `settings.yaml`: Plotting configuration (YAML)
- `era5_cache/`: Cached NetCDF files
- `output/`: Output plots and CSVs
- `tests/`: Unit tests (test_cds.py, test_visualizer.py, test_cli.py)

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

## Plotting & Outputs

- Generates polar plots of daily noon temperatures for each location and overall subplots
- Output files are saved in the `output/` directory
- Plot appearance is controlled by `settings.yaml`

Example output files:
- `output/Austin_TX_noon_temps_2020_2025.csv`
- `output/Austin_TX_noon_temps_polar_2020_2025.png`
- `output/Overall_noon_temps_polar_2020_2025.png`

---

## Configuration

### settings.yaml
Edit `settings.yaml` to customize plot size, fonts, colorbars, and layout. See comments in the file for details.

### config.yaml

Application configuration is stored in `config.yaml` with two main sections:

#### Logging Configuration
```yaml
logging:
  log_file: geo_temp.log
  console_level: WARNING  # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

#### Places Configuration

Locations are configured in the `places` section with three main subsections:

#### Places Configuration

Locations are configured in the `places` section with three main subsections:

**1. default_place:** The location used when no CLI arguments are provided
```yaml
places:
  default_place: Cambridge, UK
```

**2. all_places:** All available locations with coordinates and timezones
```yaml
places:
  all_places:
    - name: Your City, Country
      lat: 40.7128
      lon: -74.0060
      tz: America/New_York
```

**3. place_lists:** Predefined groups of places for convenience
```yaml
places:
  place_lists:
    preferred:
      - Austin, TX
      - Bangalore, India
      - Cambridge, UK
      - San Jose, CA
      - Trondheim, Norway
      - Beijing, China
    us_cities:
      - Austin, TX
      - San Diego, CA
      - San Jose, CA
    european_capitals:
      - Paris, France
      - Berlin, Germany
      - Madrid, Spain
      - Rome, Italy
      - Amsterdam, Netherlands
      - Vienna, Austria
      - Stockholm, Sweden
      - Copenhagen, Denmark
```

**Usage examples:**
```bash
# Use default place
python geo_temp.py --years 2024

# Use a specific place
python geo_temp.py --place "Cambridge, UK" --years 2024

# Use a predefined place list
python geo_temp.py --place-list preferred --years 2024

# Use all places
python geo_temp.py --all --years 2024
```

---

## Testing

Run all tests with:

```bash
pytest
# or if pytest is not in PATH:
python -m pytest
```

Tests are organized into three modules:
- `tests/test_cds.py`: 6 tests for CDS class, Location dataclass, and data retrieval
- `tests/test_visualizer.py`: 7 tests for Visualizer, temperature conversion, and data handling
- `tests/test_cli.py`: 41 tests for CLI argument parsing, grid layout, place list handling, and file I/O

**Total: 54 tests** with comprehensive coverage of all CLI options (`--place`, `--place-list`, `--all`, `--years`, `--show`, `--grid`, `--scale-height`) and error handling.

---

## Advanced Options

### Grid Layout Control

By default, geo_temp automatically calculates optimal grid layouts (e.g., 3×3 for 9 places, 3×4 for 12 places). For 3+ rows, smart adjustments prevent subplot overlap while maintaining A3 landscape format.

**Manual grid specification:**
```bash
--grid COLSxROWS
```
Examples:
- `--grid 4x3`: 4 columns by 3 rows (max 12 places per image)
- `--grid 5x3`: 5 columns by 3 rows (max 15 places per image)

When the number of selected places exceeds grid capacity, multiple sequential images are automatically generated.

**Smart scaling (default):**
```bash
--scale-height  # Enable smart subplot sizing for 3+ rows (default)
--no-scale-height  # Disable smart scaling (may cause overlap)
```

The smart scaling adjusts:
- Subplot dimensions (62% height, 83% width for 3+ rows)
- Font sizes (title, labels, radial labels)
- Vertical spacing (hspace = 0.25)
- Margins (top = 0.92, bottom = 0.08)

---

## License

MIT License
