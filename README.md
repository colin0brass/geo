
# geo_temp

**geo_temp** is a Python package for downloading, caching, analyzing, and visualizing ERA5 temperature data, with a focus on daily local noon temperatures for specified locations. It supports batch processing, flexible configuration, and publication-quality polar plots.

---

## Features
- Download and cache ERA5 2m temperature data for any location
- Analyze temperature trends and statistics for custom date ranges
- Generate polar plots and subplots for visualizing annual temperature cycles
- Highly configurable plotting (YAML settings)
- Efficient caching to avoid redundant downloads
- Command-line interface and Python API
- Unit tests for core functionality

---

## Quickstart

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Download and plot data (CLI)

Example: Plot Austin, TX for 2020-2025

```bash
python geo_temp.py --place "Austin" --years 2020-2025 --show main
```

### 3. Use as a Python module

```python
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
- `settings.yaml`: Plotting configuration (YAML)
- `era5_cache/`: Cached NetCDF files
- `output/`: Output plots and CSVs
- `tests/`: Unit tests

---

## Dependencies

- cdsapi
- matplotlib
- numpy
- pandas
- pyyaml
- pytest
- xarray

Install with:

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

Edit `settings.yaml` to customize plot size, fonts, colorbars, and layout. See comments in the file for details.

---

## Testing

Run all tests with:

```bash
pytest
```

---

## License

MIT License
