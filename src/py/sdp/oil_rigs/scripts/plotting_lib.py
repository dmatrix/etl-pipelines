"""
Shared plotting library for oil rig sensor data visualization.

This module provides reusable components for creating time-series plots
of sensor data from oil rigs, following DRY principles.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import plotly.graph_objects as go
import os
import platform
import subprocess


# Singleton Spark session
_spark_session: Optional[SparkSession] = None


def display_image(image_path: str):
    """
    Display an image using the default system image viewer.

    Args:
        image_path: Path to the image file to display
    """
    if not os.path.exists(image_path):
        print(f"Warning: Image file not found: {image_path}")
        return

    try:
        system = platform.system()

        if system == "Darwin":  # macOS
            subprocess.run(["open", image_path], check=True)
        elif system == "Windows":
            os.startfile(image_path)
        elif system == "Linux":
            subprocess.run(["xdg-open", image_path], check=True)
        else:
            print(f"Unsupported platform: {system}. Please open {image_path} manually.")

    except Exception as e:
        print(f"Could not automatically open image: {str(e)}")
        print(f"Please open manually: {image_path}")


@dataclass
class SensorPlotConfig:
    """Configuration for a specific sensor type plot."""

    metric_name: str          # e.g., "temperature", "pressure", "water_level"
    table_name: str           # e.g., "temperature_events_mv"
    y_column: str             # DataFrame column name for Y-axis
    y_label: str              # Display label for Y-axis
    title_prefix: str         # Prefix for plot title
    value_format: str         # Unit format (e.g., "°F", "PSI", "feet")
    hover_template: str       # Hover text template

    # Aggregation column names (for renaming in stats)
    min_col_name: str = None
    max_col_name: str = None
    avg_col_name: str = None

    def __post_init__(self):
        """Set default aggregation column names if not provided."""
        if self.min_col_name is None:
            self.min_col_name = f"min_{self.metric_name}"
        if self.max_col_name is None:
            self.max_col_name = f"max_{self.metric_name}"
        if self.avg_col_name is None:
            self.avg_col_name = f"avg_{self.metric_name}"


# Sensor configurations for all supported metrics
SENSOR_CONFIGS: Dict[str, SensorPlotConfig] = {
    "temperature": SensorPlotConfig(
        metric_name="temperature",
        table_name="temperature_events_mv",
        y_column="temperature_f",
        y_label="Temperature (°F)",
        title_prefix="Temperature",
        value_format="°F",
        hover_template="Time: %{x}<br>Temperature: %{y:.1f}°F"
    ),
    "pressure": SensorPlotConfig(
        metric_name="pressure",
        table_name="pressure_events_mv",
        y_column="pressure_psi",
        y_label="Pressure (PSI)",
        title_prefix="Pressure",
        value_format="PSI",
        hover_template="Time: %{x}<br>Pressure: %{y:.1f} PSI"
    ),
    "water_level": SensorPlotConfig(
        metric_name="water_level",
        table_name="water_level_events_mv",
        y_column="water_level_feet",
        y_label="Water Level (feet)",
        title_prefix="Water Level",
        value_format="feet",
        hover_template="Time: %{x}<br>Water Level: %{y:.1f} feet"
    )
}


def get_spark_session(warehouse_dir: Optional[str] = None) -> SparkSession:
    """
    Get or create a Spark session (singleton pattern).

    Args:
        warehouse_dir: Optional warehouse directory path (defaults to ../spark-warehouse)

    Returns:
        SparkSession instance
    """
    global _spark_session

    if _spark_session is None:
        # Default warehouse directory relative to scripts directory
        if warehouse_dir is None:
            warehouse_dir = "../spark-warehouse"

        builder = SparkSession.builder.appName("OilRigSensorPlots")
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

        _spark_session = (builder
                         .config("spark.sql.plotting.backend", "plotly")
                         .config("spark.sql.pyspark.plotting.max_rows", 10000)
                         .enableHiveSupport()
                         .getOrCreate())

    return _spark_session


def get_available_rigs(df: DataFrame) -> List[str]:
    """
    Get list of unique rig names from the DataFrame.

    Args:
        df: DataFrame containing rig data

    Returns:
        List of unique rig names
    """
    rigs = df.select("rig_name").distinct().collect()
    return sorted([row.rig_name for row in rigs])


def filter_by_rig(df: DataFrame, rig_name: str) -> DataFrame:
    """
    Filter DataFrame to only include data from specified rig.

    Args:
        df: Source DataFrame
        rig_name: Name of rig to filter by

    Returns:
        Filtered DataFrame
    """
    return df.filter(col("rig_name") == rig_name)


def calculate_statistics(df: DataFrame, config: SensorPlotConfig,
                         rig_name: Optional[str] = None) -> DataFrame:
    """
    Calculate statistics for the sensor data.

    Args:
        df: Source DataFrame
        config: Sensor plot configuration
        rig_name: Optional rig name filter

    Returns:
        DataFrame with statistics
    """
    # Filter by rig if specified
    if rig_name and rig_name != "all":
        df = filter_by_rig(df, rig_name)

    # Calculate aggregations
    stats_df = df.groupBy("rig_name").agg({
        config.y_column: "min",
        config.y_column: "max",
        config.y_column: "avg"
    }).withColumnRenamed(f"min({config.y_column})", config.min_col_name) \
      .withColumnRenamed(f"max({config.y_column})", config.max_col_name) \
      .withColumnRenamed(f"avg({config.y_column})", config.avg_col_name)

    return stats_df


def print_statistics(stats_df: DataFrame, metric_name: str,
                     rig_name: Optional[str] = None):
    """
    Print formatted statistics.

    Args:
        stats_df: Statistics DataFrame
        metric_name: Name of the metric
        rig_name: Optional rig name filter
    """
    rig_label = rig_name if rig_name and rig_name != "all" else "All Rigs"
    print(f"\n{metric_name.title()} Statistics - {rig_label}:")
    print("=" * 70)
    stats_df.show(truncate=False)


def generate_plot(config: SensorPlotConfig,
                  rig_name: str,
                  output_dir: str = "../artifacts",
                  spark: Optional[SparkSession] = None) -> tuple[go.Figure, DataFrame]:
    """
    Generate a time-series plot for a specific sensor metric and individual rig.

    Uses PySpark 4.0+ native plotting API - no Pandas conversion required!

    Args:
        config: Sensor plot configuration
        rig_name: Specific rig name to plot (e.g., "permian_rig")
        output_dir: Directory to save output file
        spark: Optional Spark session (will create if not provided)

    Returns:
        Tuple of (Plotly figure, statistics DataFrame)
    """
    # Get Spark session
    if spark is None:
        spark = get_spark_session()

    # Read and filter data for the specific rig
    df = spark.read.table(config.table_name)
    plot_df = filter_by_rig(df, rig_name)

    # Generate title and filename
    title = f"{config.title_prefix} Readings - {rig_name.replace('_', ' ').title()}"
    output_filename = f"{rig_name}_{config.metric_name}_plot.png"

    # Use PySpark native plotting API (Spark 4.0+) - NO .toPandas() conversion!
    fig = plot_df.plot.line(x="timestamp", y=config.y_column)

    # Customize layout
    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title=config.y_label,
        hovermode='x',
        template="plotly_white"
    )

    # Note: X-axis shows Unix timestamps in milliseconds (exponential notation)
    # This is a known behavior of PySpark native plotting API
    # The timestamps are correct but displayed as large numbers

    # Add hover data formatting
    fig.update_traces(
        hovertemplate=config.hover_template
    )

    # Save the plot as PNG
    output_path = f"{output_dir}/{output_filename}"
    fig.write_image(output_path, format="png", width=1200, height=800, scale=2.0)
    print(f"Plot saved as {output_path}")

    # Display the image
    display_image(output_path)

    # Calculate and return statistics
    stats_df = calculate_statistics(df, config, rig_name)

    return fig, stats_df


def close_spark_session():
    """Close the Spark session."""
    global _spark_session
    if _spark_session:
        _spark_session.stop()
        _spark_session = None
