#!/usr/bin/env python3
"""
Simplified CLI for generating individual oil rig sensor data visualizations.

This script creates time-series plots for individual rigs only,
using PySpark native plotting (Spark 4.0+).

Usage Examples:
    # List available metrics
    python plot_sensors.py --list-metrics

    # List available rigs
    python plot_sensors.py --list-rigs

    # Generate temperature plot for Permian rig
    python plot_sensors.py --metric temperature --rig permian_rig

    # Generate temperature plots for each rig
    python plot_sensors.py --metric temperature --rig each
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from plotting_lib import (
    SENSOR_CONFIGS,
    get_spark_session,
    generate_plot,
    print_statistics,
    close_spark_session,
    get_available_rigs
)


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate time-series plots for individual oil rig sensor data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available metrics
  %(prog)s --list-metrics

  # List available rigs
  %(prog)s --list-rigs

  # Single metric, single rig
  %(prog)s --metric temperature --rig permian_rig

  # Single metric, each rig separately
  %(prog)s --metric pressure --rig each

Note: This script generates ONE metric at a time. Use --list-metrics
to see all available metrics, then run the script multiple times for
different metrics if needed.
        """
    )

    parser.add_argument(
        "--metric",
        choices=["temperature", "pressure", "water_level"],
        help="Sensor metric to plot (use --list-metrics to see all available)"
    )

    parser.add_argument(
        "--list-metrics",
        action="store_true",
        help="List available sensor metrics and exit"
    )

    parser.add_argument(
        "--rig",
        default="each",
        help="Rig name to plot: specific rig name or 'each' (all rigs separately)"
    )

    parser.add_argument(
        "--output-dir",
        default="../artifacts",
        help="Output directory for generated plots (default: ../artifacts)"
    )

    parser.add_argument(
        "--warehouse-dir",
        default="../spark-warehouse",
        help="Spark warehouse directory (default: ../spark-warehouse)"
    )

    parser.add_argument(
        "--list-rigs",
        action="store_true",
        help="List available rig names and exit"
    )

    # If no arguments provided, print help and exit
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    return parser.parse_args()


def main():
    """Main execution function."""
    # Parse arguments
    args = parse_arguments()

    # List metrics if requested (no Spark session needed)
    if args.list_metrics:
        print("Available metrics:")
        for metric in SENSOR_CONFIGS.keys():
            print(f"  - {metric}")
        return 0

    # Validate arguments before starting Spark session
    if not args.metric and not args.list_rigs:
        print("Error: Must specify --metric")
        print("Use --list-metrics to see available metrics")
        print("Use --help for usage information")
        return 1

    # Get Spark session (only if we need it)
    spark = get_spark_session(warehouse_dir=args.warehouse_dir)

    # List rigs if requested
    if args.list_rigs:
        print("Available rigs:")
        rigs = get_available_rigs(spark.read.table(list(SENSOR_CONFIGS.values())[0].table_name))
        for rig in rigs:
            print(f"  - {rig}")
        return 0

    # Print header
    print("=" * 70)
    print("Oil Rig Sensor Data Visualization")
    print("=" * 70)
    print()

    # Determine which rigs to process
    if args.rig == "each":
        # Get all available rigs
        first_config = SENSOR_CONFIGS[list(SENSOR_CONFIGS.keys())[0]]
        df = spark.read.table(first_config.table_name)
        rig_names = get_available_rigs(df)
        print(f"Mode: Generating plots for each rig")
        print(f"Rigs: {', '.join(rig_names)}")
    else:
        # Single specific rig
        rig_names = [args.rig]
        print(f"Mode: Generating plots for specific rig")
        print(f"Rig: {args.rig}")

    print()
    print(f"Metric: {args.metric}")
    print()

    # Get the metric configuration
    config = SENSOR_CONFIGS[args.metric]

    # Generate plots for each rig
    total_plots = 0

    for rig in rig_names:
        if len(rig_names) > 1:
            print("=" * 70)
            print(f"Processing: {args.metric} - {rig}")
            print("=" * 70)

        # Generate the plot
        fig, stats = generate_plot(
            config=config,
            rig_name=rig,
            output_dir=args.output_dir,
            spark=spark
        )

        # Print statistics
        print_statistics(stats, args.metric, rig)
        print()

        total_plots += 1

    # Summary
    print("=" * 70)
    print("Generation Complete!")
    print("=" * 70)
    print(f"Total plots generated: {total_plots}")
    print(f"Output directory: {args.output_dir}")

    # Clean up
    close_spark_session()

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
