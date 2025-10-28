# Oil Rigs - Industrial IoT Sensor Monitoring Pipeline

## Overview

Oil Rigs is an industrial IoT sensor monitoring and analytics system built using Spark Declarative Pipelines (SDP). It demonstrates realistic sensor data generation with operational patterns, materialized view transformations, and interactive time-series visualizations for temperature, pressure, and water level monitoring from Texas oil fields.

## Project Structure

```
oil_rigs/
├── README.md                       # This file
├── pipeline.yml                    # SDP pipeline configuration
├── run_pipeline.sh                 # Pipeline execution script
├── __init__.py                     # Python package initialization
├── transformations/                # Data transformation definitions
│   ├── oil_rig_events_mv.py        # Base sensor events (Python)
│   ├── temperature_events_mv.sql   # Temperature readings filter (SQL)
│   ├── pressure_events_mv.sql      # Pressure readings filter (SQL)
│   └── water_level_events_mv.sql   # Water level readings filter (SQL)
├── scripts/                        # Analysis and visualization scripts
│   ├── plot_sensors.py             # Unified CLI for generating plots (PySpark 4.0+ native)
│   └── plotting_lib.py             # Shared plotting library
├── artifacts/                      # Generated visualization outputs
│   ├── permian_rig_temperature_plot.png    # Permian rig temperature chart
│   ├── permian_rig_pressure_plot.png       # Permian rig pressure chart
│   ├── permian_rig_water_level_plot.png    # Permian rig water level chart
│   ├── eagle_ford_rig_temperature_plot.png # Eagle Ford rig temperature chart
│   ├── eagle_ford_rig_pressure_plot.png    # Eagle Ford rig pressure chart
│   └── eagle_ford_rig_water_level_plot.png # Eagle Ford rig water level chart
├── query_oil_rigs_tables.py        # Query and display sensor data
├── spark-warehouse/                # Generated Spark warehouse data (auto-generated)
└── metastore_db/                   # Derby database files (auto-generated)
```

## Features

- **Realistic Sensor Data Generation**: Creates sensor data with meaningful operational patterns
  - Daily temperature cycles with ambient variations
  - Drilling operations with correlated pressure increases
  - Water accumulation and pump-out cycles
  - 5% anomaly rate for alerting demonstrations
- **Materialized Views**: Declarative transformations using Python and SQL
- **Sensor Type Filtering**: Separate views for temperature, pressure, and water level
- **PySpark 4.0+ Native Plotting**: Uses PySpark DataFrame.plot API - no Pandas conversion required!
- **PNG Visualizations**: High-quality time-series charts (~250KB each) with automatic display
- **Multi-Location Monitoring**: Data from Permian Basin and Eagle Ford Shale regions

## Running the Pipeline

### Option 1: Using the Shell Script

```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/oil_rigs
./run_pipeline.sh
```

### Option 2: Using Python Directly

```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp
python main.py oil-rigs
```

### Option 3: Using SDP CLI

```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/oil_rigs
spark-pipelines run --conf spark.sql.catalogImplementation=hive --conf spark.sql.warehouse.dir=spark-warehouse
```

## Data Transformations

### 1. Oil Rig Events Materialized View (oil_rig_events_mv.py)

The base materialized views that generate realistic sensor data for two oil rigs:

- **Names**: `permian_rig_mv`, `eagle_ford_rig_mv`
- **Type**: Python transformation with `@sdp.materialized_view` decorator
- **Data Source**: Dynamically generated using `oil_gen_util.create_oil_rig_events_dataframe()`
- **Schema**:
  - `event_id`: Unique event identifier (UUID)
  - `rig_name`: Oil rig identifier
  - `location`: Geographic location
  - `region`: Oil field region
  - `sensor_type`: Type of sensor (temperature/pressure/water_level)
  - `sensor_value`: Sensor reading value
  - `timestamp`: Event timestamp
  - `latitude`, `longitude`: GPS coordinates

### 2. Temperature Events View (temperature_events_mv.sql)

SQL-based materialized view combining temperature readings from both rigs:

```sql
CREATE MATERIALIZED VIEW temperature_events_mv AS
SELECT
    event_id, rig_name, location, region,
    sensor_value as temperature_f,
    timestamp, latitude, longitude
FROM permian_rig_mv
WHERE sensor_type = 'temperature'
UNION ALL
SELECT
    event_id, rig_name, location, region,
    sensor_value as temperature_f,
    timestamp, latitude, longitude
FROM eagle_ford_rig_mv
WHERE sensor_type = 'temperature';
```

### 3. Pressure Events View (pressure_events_mv.sql)

SQL-based materialized view for pressure readings from both rigs:

```sql
CREATE MATERIALIZED VIEW pressure_events_mv AS
SELECT
    event_id, rig_name, location, region,
    sensor_value as pressure_psi,
    timestamp, latitude, longitude
FROM permian_rig_mv
WHERE sensor_type = 'pressure'
UNION ALL
SELECT
    event_id, rig_name, location, region,
    sensor_value as pressure_psi,
    timestamp, latitude, longitude
FROM eagle_ford_rig_mv
WHERE sensor_type = 'pressure';
```

### 4. Water Level Events View (water_level_events_mv.sql)

SQL-based materialized view for water level readings from both rigs:

```sql
CREATE MATERIALIZED VIEW water_level_events_mv AS
SELECT
    event_id, rig_name, location, region,
    sensor_value as water_level_feet,
    timestamp, latitude, longitude
FROM permian_rig_mv
WHERE sensor_type = 'water_level'
UNION ALL
SELECT
    event_id, rig_name, location, region,
    sensor_value as water_level_feet,
    timestamp, latitude, longitude
FROM eagle_ford_rig_mv
WHERE sensor_type = 'water_level';
```

## Visualization Scripts

### PySpark Native Plotting (Spark 4.0+)

The pipeline uses **PySpark 4.0+ native DataFrame.plot API** for generating visualizations - no Pandas conversion required! This provides:

- **Zero Pandas Overhead**: Direct plotting from Spark DataFrames
- **Better Performance**: Automatic sampling for large datasets
- **Native Integration**: Leverages Plotly backend built into PySpark
- **Simplified Code**: ~40% less code compared to Pandas-based approach

**Key Features:**
- Individual rig plots only (no combined multi-rig plots)
- Generate specific metrics or all metrics at once
- PNG output with automatic display (~250KB per chart)
- Modular architecture with shared plotting library

### Quick Start

```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/oil_rigs

# Generate temperature plot for Permian rig
uv run python scripts/plot_sensors.py --metric temperature --rig permian_rig

# Generate all metrics for Eagle Ford rig
uv run python scripts/plot_sensors.py --all-metrics --rig eagle_ford_rig

# Generate pressure plots for each rig separately
uv run python scripts/plot_sensors.py --metric pressure --rig each

# Generate all metrics for all rigs (6 separate PNG plots)
uv run python scripts/plot_sensors.py --all-metrics --rig each
```

### Command-Line Options

```bash
# Show help and usage examples
uv run python scripts/plot_sensors.py --help

# List available rig names
uv run python scripts/plot_sensors.py --list-rigs
```

**Options:**
- `--metric {temperature,pressure,water_level}` - Specific sensor metric to plot
- `--all-metrics` - Generate plots for all sensor metrics
- `--rig RIG_NAME` - Rig to plot:
  - Specific rig name (e.g., `permian_rig`, `eagle_ford_rig`)
  - `each` - Separate plots for each rig (default)
- `--output-dir DIR` - Output directory (default: `../artifacts`)
- `--warehouse-dir DIR` - Spark warehouse directory (default: `../spark-warehouse`)
- `--list-rigs` - List available rig names and exit

### Usage Examples

#### Single Rig, Single Metric
```bash
# Generate temperature plot for Permian Basin rig
uv run python scripts/plot_sensors.py --metric temperature --rig permian_rig

# Output: artifacts/permian_rig_temperature_plot.png (auto-displayed)
```

#### Single Rig, All Metrics
```bash
# Generate all sensor plots for Eagle Ford rig
uv run python scripts/plot_sensors.py --all-metrics --rig eagle_ford_rig

# Output (3 PNG files, auto-displayed):
#   artifacts/eagle_ford_rig_temperature_plot.png
#   artifacts/eagle_ford_rig_pressure_plot.png
#   artifacts/eagle_ford_rig_water_level_plot.png
```

#### All Rigs, Single Metric
```bash
# Generate water level plots for each rig separately
uv run python scripts/plot_sensors.py --metric water_level --rig each

# Output (2 PNG files):
#   artifacts/permian_rig_water_level_plot.png
#   artifacts/eagle_ford_rig_water_level_plot.png
```

#### Generate Everything
```bash
# Generate all metrics for each rig (6 total PNG plots)
uv run python scripts/plot_sensors.py --all-metrics --rig each

# Output (6 PNG files):
#   artifacts/permian_rig_temperature_plot.png
#   artifacts/permian_rig_pressure_plot.png
#   artifacts/permian_rig_water_level_plot.png
#   artifacts/eagle_ford_rig_temperature_plot.png
#   artifacts/eagle_ford_rig_pressure_plot.png
#   artifacts/eagle_ford_rig_water_level_plot.png
```

### Output File Naming Convention

**Format:** `{rig_name}_{metric}_plot.png`

**Examples:**
- `permian_rig_temperature_plot.png`
- `eagle_ford_rig_pressure_plot.png`
- `permian_rig_water_level_plot.png`

**File Size:** ~250KB per PNG (high resolution: 1200×800, scale 2.0)

### Expected Visualization Patterns

#### Temperature Plots
- **Range**: 150-350°F
- **Patterns**: Clear daily sinusoidal patterns
- **Drilling Operations**: Higher temperatures (~260-270°F) during 8-hour cycles
- **Idle Periods**: Lower temperatures (~200-210°F) during 4-hour rest

#### Pressure Plots
- **Range**: 2,000-5,000 PSI
- **Patterns**: Progressive increases during drilling, gradual decreases during idle
- **Peak Pressure**: Up to 4,000 PSI during active drilling
- **Baseline**: ~2,500 PSI during idle periods

**Sample Statistics:**
```
Pressure Statistics - permian_rig:
==========================================
+-----------+-----------------+
|rig_name   |avg_pressure     |
+-----------+-----------------+
|permian_rig|3248.95 PSI      |
+-----------+-----------------+
```

#### Water Level Plots
- **Range**: 80-450 feet
- **Patterns**: Saw-tooth accumulation and pump-out cycles
- **Accumulation**: 3-6 ft per 15 min during drilling
- **Pump-out**: Aggressive removal (~20 ft/15min) tapering to ~8 ft/15min
- **Cycle**: Repeats every 12 hours

**Sample Statistics:**
```
Water_Level Statistics - eagle_ford_rig:
==========================================
+--------------+------------------+
|rig_name      |avg_water_level   |
+--------------+------------------+
|eagle_ford_rig|345.37 feet       |
+--------------+------------------+
```

### Technical Implementation

**PySpark Native Plotting:**
- Uses `DataFrame.plot.line()` API introduced in Spark 4.0+
- Direct Plotly figure generation - no intermediate Pandas DataFrame
- Automatic display using system image viewer (macOS/Linux/Windows)
- Configured with `spark.sql.pyspark.plotting.max_rows=10000` for sampling

**Known Limitation:**
- X-axis timestamps display as Unix milliseconds in exponential notation (e.g., `1.765×10¹⁸`)
- This is expected behavior of PySpark native plotting API
- Timestamps are correct, just displayed as large numbers
- Trade-off accepted for zero Pandas conversion and better performance

## Query Scripts

### Query Oil Rigs Tables (query_oil_rigs_tables.py)

Queries the sensor events and displays data from both oil rigs.

**Run the script:**
```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/oil_rigs
python query_oil_rigs_tables.py
```

## Realistic Data Generation

The pipeline uses advanced data generation logic in `utils/oil_gen_util.py` to create sensor data with meaningful operational patterns:

### Temperature Generation
- **Base temperature**: 180°F with operational variations
- **Daily cycle**: ±15°F sinusoidal variation (peak at 3 PM, low at 5 AM)
- **Drilling operations**: +80°F increase during 8-hour drilling cycles
- **Idle periods**: +20°F increase during 4-hour rest periods
- **Noise**: ±5°F random measurement variation
- **Anomalies**: 5% chance of +10-30°F spikes

### Pressure Generation
- **Base pressure**: 2,500 PSI
- **Drilling correlation**: Progressive increase to 4,000 PSI during drilling
- **Idle periods**: Gradual decrease back to baseline
- **Drilling progress**: Pressure proportional to time into drilling cycle
- **Noise**: ±100-150 PSI random variation
- **Anomalies**: 5% chance of +200-400 PSI spikes

### Water Level Generation
- **Starting level**: 120 feet
- **Accumulation**: +3-6 feet per 15 minutes during drilling
- **Pump-out**: Active removal during first 60% of idle periods (2.4 hours)
- **Pump rate**: Starts at ~20 ft/15min, tapers to ~8 ft/15min
- **Late idle**: Minimal accumulation (+0.5-1.5 ft/15min) in last 40% of idle
- **Range**: 80-450 feet with realistic bounds

### Operational Cycles
- **Drilling cycle**: 8 hours of active operations
- **Idle cycle**: 4 hours of rest and maintenance
- **Total cycle**: 12 hours (2 cycles per day)
- **Reading frequency**: Every 15 minutes
- **Correlated metrics**: Temperature, pressure, and water level all respond to operational state

## Oil Rig Locations

### 1. Permian Basin Rig
- **Location**: Midland, Texas
- **Region**: Permian Basin
- **Coordinates**: 31.9973°N, 102.0779°W
- **Average Pressure**: 3,248.95 PSI
- **Average Water Level**: 176.37 feet

### 2. Eagle Ford Shale Rig
- **Location**: Karnes City, Texas
- **Region**: Eagle Ford Shale
- **Coordinates**: 28.8851°N, 97.9006°W
- **Average Pressure**: 3,284.86 PSI
- **Average Water Level**: 345.37 feet (higher accumulation rate)

## Key Insights from Sensor Data

Based on the realistic data patterns:

- **Drilling Efficiency**: Both rigs follow consistent 8-hour drilling, 4-hour idle cycles
- **Pressure Management**: Pressure increases linearly during drilling, indicating controlled operations
- **Water Management**: Eagle Ford rig shows higher water accumulation (345 ft vs 176 ft), suggesting different geological conditions or drainage characteristics
- **Temperature Correlation**: Temperature spikes correlate with drilling activity, validating equipment heat generation
- **Anomaly Detection**: 5% anomaly rate provides realistic data for alerting system testing

## Technical Details

### Data Generation Patterns

The pipeline demonstrates advanced synthetic data generation with:
- **Time-series correlation**: All metrics respond to operational state
- **Daily cycles**: Temperature affected by time-of-day
- **Progressive patterns**: Pressure builds gradually during operations
- **Saw-tooth patterns**: Water accumulation and pump-out cycles
- **Realistic noise**: Random variations mimicking real sensor measurements
- **Anomaly injection**: Occasional spikes for monitoring demonstrations

### Materialized View Pattern

The pipeline demonstrates the SDP framework's hybrid approach:
- **Python transformations**: Base sensor data generation using decorators
- **SQL transformations**: Downstream filtering by sensor type
- **Dynamic module loading**: Cross-pipeline code sharing via importlib
- **Union operations**: Combining data from multiple rigs into unified views

### Pipeline Configuration

The `pipeline.yml` file uses glob patterns to auto-discover transformations:
```yaml
name: oil_rigs
storage: storage-root
libraries:
  - glob:
      include: transformations/**
```

### Visualization Technology

- **Plotly Express**: Interactive charts with hover, zoom, pan
- **Spark Integration**: Native Plotly backend for DataFrame visualization
- **Modular Architecture**: Shared plotting library (`plotting_lib.py`) with DRY principles
- **Export Formats**: HTML (default), PNG, SVG (via kaleido)
- **Comparison Modes**: Overlay and side-by-side subplot layouts
- **File Size**: ~4.9MB per HTML chart (includes full Plotly library)

### Testing

The plotting library includes comprehensive unit tests:

```bash
# Run all tests
cd /path/to/spark-declarative-pipelines/src/py/sdp/oil_rigs
uv run pytest tests/test_plotting_lib.py -v

# Run specific test class
uv run pytest tests/test_plotting_lib.py::TestSensorConfigs -v
```

**Test Coverage:**
- ✅ **20 unit tests** covering configuration, filtering, and validation
- ✅ **SensorPlotConfig** dataclass creation and defaults
- ✅ **SENSOR_CONFIGS** dictionary validation
- ✅ **DataFrame filtering** by rig name
- ✅ **Rig name extraction** from data
- ✅ **Filename and title generation** logic
- ✅ **Configuration consistency** checks

All tests pass without requiring a full Spark cluster or database connection.

## Dependencies

- PySpark 4.1.0.dev3
- Plotly 6.3.0+ (for interactive visualizations)
- Python 3.12+
- kaleido (optional, for PNG/SVG export)
- pytest (dev dependency, for running tests)

## Next Steps

1. **Real-time Monitoring**: Add streaming data ingestion using Spark Structured Streaming
2. **Alerting System**: Implement threshold-based alerts for anomalies
3. **Predictive Maintenance**: Build ML models to predict equipment failures
4. **Multi-Rig Dashboard**: Create comprehensive dashboard combining all metrics
5. **Historical Analysis**: Add long-term trend analysis and seasonality detection
6. **Comparative Analytics**: Cross-rig performance comparison and benchmarking

## Related Pipelines

- **BrickFood Pipeline**: E-commerce order processing example ([../brickfood/](../brickfood/))
- **Music Analytics Pipeline**: Lakeflow Spark Declarative Pipelines example ([../../lsdp/music_analytics/](../../lsdp/music_analytics/))

## Troubleshooting

### Issue: Plots not generating

**Solution**: Ensure you're running from the `oil_rigs` directory:
```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/oil_rigs
uv run python scripts/plot_sensors.py --metric temperature --rig all
```

### Issue: Table or view not found

**Solution**: Run the pipeline first to create materialized views:
```bash
./run_pipeline.sh
```

### Issue: Warehouse directory warnings

**Note**: The warnings about `spark.sql.warehouse.dir` configuration are expected when using Spark Connect and can be safely ignored. The pipeline uses the local `spark-warehouse/` directory.

## License

Apache 2.0
