# Snow Depth Sensors CLI

A Python command-line application that fetches snow depth sensor data from the South Tyrol weather service and processes it using pandas DataFrames.

## Features

- Fetches real-time snow depth data from South Tyrol weather sensors
- Processes data into pandas DataFrame with automatic datetime conversion
- Command-line interface with multiple output formats
- Data summary statistics and visualization
- Export to CSV, JSON, or Excel formats

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Install the package in development mode:
```bash
pip install -e .
```

## Usage

### Command Line Interface

Once installed, you can use the `snow-sensors` command:

```bash
# Fetch and display data
snow-sensors

# Save data to CSV file
snow-sensors --output data.csv

# Save as JSON with verbose output
snow-sensors --output data.json --format json --verbose

# Save as Excel without summary
snow-sensors --output data.xlsx --format excel --no-summary
```

### Options

- `--output`, `-o`: Output file path to save the data
- `--format`, `-f`: Output format (csv, json, excel) - default: csv
- `--show-summary/--no-summary`: Show/hide data summary statistics - default: True
- `--timeout`: Request timeout in seconds - default: 30
- `--verbose`, `-v`: Enable verbose output

### Python Usage

You can also use the package directly in Python:

```python
from snow_sensors.data_fetcher import SensorDataFetcher

# Create fetcher instance
fetcher = SensorDataFetcher()

# Fetch data as pandas DataFrame
df = fetcher.fetch_snow_depth_data()

# Get summary statistics
summary = fetcher.get_data_summary(df)

print(f"Fetched {len(df)} records from {summary['unique_sensors']} sensors")
print(df.head())
```

## Data Format

The application fetches data with the following columns (depending on the data source):

- `hs`: Snow depth (cm)
- `hn`: New snow (cm, Trentino only)
- `date`: Timestamp of the measurement (datetime)
- `name_it`: Station name (Italian)
- `name_de`: Station name (German)
- `name_en`: Station name (English)
- `longitude`: Station longitude (WGS84 for AltoAdige, UTM for Trentino)
- `latitude`: Station latitude (WGS84 for AltoAdige, UTM for Trentino)

Some fields may be present only for specific regions. See the source code for details.

## Requirements

- Python 3.8+
- pandas >= 1.5.0
- requests >= 2.25.0
- click >= 8.0.0