from __future__ import annotations

import argparse
import sys

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from cds import CDS, Location
from plot import Visualizer

# Parameters
AUSTIN = Location(name="Austin, TX", lat=30.2672, lon=-97.7431, tz="America/Chicago")
BANGALORE = Location(name="Bangalore, India", lat=12.9716, lon=77.5946, tz="Asia/Kolkata")
CAMBRIDGE = Location(name="Cambridge, UK", lat=52.2053, lon=0.1218, tz="Europe/London")
SAN_JOSE = Location(name="San Jose, CA", lat=37.3382, lon=-121.8863, tz="America/Los_Angeles")
TRONDHEIM = Location(name="Trondheim, Norway", lat=63.4305, lon=10.3951, tz="Europe/Oslo")
SINGAPORE = Location(name="Singapore", lat=1.3521, lon=103.8198, tz="Asia/Singapore")
BEIJING = Location(name="Beijing, China", lat=39.9042, lon=116.4074, tz="Asia/Shanghai")

places = {
    "Austin": AUSTIN,
    "Bangalore": BANGALORE,
    # "Singapore": SINGAPORE,
    "Beijing": BEIJING,
    "SanJose": SAN_JOSE,
    "Cambridge": CAMBRIDGE,
    "Trondheim": TRONDHEIM,
}

def read_data_file(in_file: Path) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame, parsing date columns.
    
    Args:
        in_file: Path to the CSV file.
    Returns:
        DataFrame with parsed dates.
    """
    df = pd.read_csv(in_file, parse_dates=['date', 'utc_time_used', 'local_noon'])
    return df

def save_data_file(df: pd.DataFrame, out_file: Path) -> None:
    """
    Save a DataFrame to a CSV file, creating parent directories if needed.
    
    Args:
        df: DataFrame to save.
        out_file: Output file path.
    """
    out_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file)
    print(f"Saved data to {out_file}")

def main() -> None:
    """
    Main entry point for geo_temp.py.
    Handles argument parsing, data loading, and plotting of geographic temperature data.
    """
    arg_parse = argparse.ArgumentParser(description="Generate geographic temperature plots from ERA5 data.")
    arg_parse.add_argument("--place", type=str, default="Austin, TX", help="Name of the place to retrieve data for.")
    arg_parse.add_argument("--lat", type=float, required=False, help="Latitude of the location.")
    arg_parse.add_argument("--lon", type=float, required=False, help="Longitude of the location.")
    arg_parse.add_argument("--tz", type=str, required=False, help="Timezone of the location.")
    arg_parse.add_argument("--all", action="store_true", help="Retrieve data for all locations.")
    # Removed --start-year and --end-year; use --years only
    arg_parse.add_argument(
        "--years",
        type=str,
        required=False,
        default=str(datetime.now().year - 1),
        help="Year or year range (e.g. 2025 or 2020-2025). Overrides --start-year and --end-year if provided."
    )
    arg_parse.add_argument("--cache-dir", type=Path, default=Path("era5_cache"), help="Cache directory for NetCDF files.")
    arg_parse.add_argument("--out-dir", type=Path, default=Path("output"), help="Output directory for plots.")
    arg_parse.add_argument("--settings", type=Path, default=Path("settings.yaml"), help="Path to plot settings YAML file.")
    arg_parse.add_argument(
        "--show",
        type=str,
        choices=["none", "main", "all"],
        default="none",
        help="Which plots to display: 'none' (default, show no plots), 'main' (show only the main/all subplot), 'all' (show all individual and main plots)"
    )

    args = arg_parse.parse_args()

    # Parse --years argument (now required)
    if not args.years:
        print("Error: --years argument is required (e.g. --years 2025 or --years 2020-2025)", file=sys.stderr)
        sys.exit(1)
    if '-' in args.years:
        try:
            start, end = args.years.split('-')
            start_year = int(start)
            end_year = int(end)
        except Exception:
            print(f"Invalid --years format: {args.years}. Use YYYY or YYYY-YYYY.", file=sys.stderr)
            sys.exit(1)
    else:
        try:
            year = int(args.years)
            start_year = year
            end_year = year
        except Exception:
            print(f"Invalid --years format: {args.years}. Use YYYY or YYYY-YYYY.", file=sys.stderr)
            sys.exit(1)

    if args.all:
        place_list = list(places.values())
    else:
        if args.place not in places and (args.lat is None or args.lon is None or args.tz is None):
            print(f"Error: Unknown place '{args.place}'. Please provide lat, lon, and tz.", file=sys.stderr)
            sys.exit(1)
        if args.place in places:
            place_list = [places[args.place]]
        else:
            place_list = [Location(name=args.place, lat=args.lat, lon=args.lon, tz=args.tz)]

    df_overall = pd.DataFrame()
    for loc in place_list:
        print(f"Retrieving data for {loc.name} ({loc.lat}, {loc.lon}) in timezone {loc.tz}...")
        cds = CDS(cache_dir=args.cache_dir)
        start_d = date(start_year, 1, 1)
        end_d = date(end_year, 12, 31)

        file_leaf_name = f"{loc.name.replace(' ', '_').replace(',', '')}_noon_temps_{start_year}_{end_year}.csv"
        data_file = args.out_dir / file_leaf_name

        if data_file.exists():
            print(f"Data file {data_file} already exists. Loading existing data.")
            df = read_data_file(data_file)
        else:
            df = cds.get_noon_series(loc, start_d, end_d)
            save_data_file(df, data_file)

        df_overall = pd.concat([df_overall, df], ignore_index=True)

    t_min_c = df_overall["temp_C"].min()
    t_max_c = df_overall["temp_C"].max()
    print(f"Overall temperature range across all locations: {t_min_c:.2f} °C to {t_max_c:.2f} °C")

    vis = Visualizer(df_overall, out_dir=args.out_dir, t_min_c = t_min_c, t_max_c = t_max_c, settings_file=args.settings)
    plot_file = args.out_dir / f"Overall_noon_temps_polar_{start_year}_{end_year}.png"
    title = f"Mid-Day Temperatures ({start_year}-{end_year})"
    credit = "Analysis & visualisation by Colin Osborne"
    data_source = "Data from: ERA5 via CDS"

    show_main = args.show.lower() in ("main", "all")
    show_individual = args.show.lower() == "all"

    vis.plot_polar_subplots(
        title=title,
        subplot_field="place_name",
        num_rows=2,
        credit=credit,
        data_source=data_source,
        save_file=str(plot_file),
        layout="polar_subplot",
        show_plot=show_main)
    print(f"Saved overall plot to {plot_file}")

    for loc in place_list:
        df = df_overall[df_overall['place_name'] == loc.name]
        vis = Visualizer(df, out_dir=args.out_dir, t_min_c = t_min_c, t_max_c = t_max_c, settings_file=args.settings)
        plot_file = args.out_dir / f"{loc.name.replace(' ', '_').replace(',', '')}_noon_temps_polar_{start_year}_{end_year}.png"
        title = f"{loc.name} Mid-Day Temperatures ({start_year}-{end_year})"
        credit = "Analysis & visualisation by Colin Osborne"
        data_source = "Data from: ERA5 via CDS"
        vis.plot_polar(
            title=title,
            credit=credit,
            data_source=data_source,
            save_file=str(plot_file),
            layout="polar_single",
            show_plot=show_individual)
        print(f"Saved plot to {plot_file}")

if __name__ == "__main__":
    main()
