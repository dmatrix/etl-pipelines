from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import random
import math
from datetime import datetime, timedelta
import uuid

# Define oil rig configurations
OIL_RIGS = {
    'permian_rig': {
        'location': 'Midland, Texas',
        'region': 'Permian Basin',
        'lat': 31.9973,
        'lon': -102.0779
    },
    'eagle_ford_rig': {
        'location': 'Karnes City, Texas',
        'region': 'Eagle Ford Shale',
        'lat': 28.8851,
        'lon': -97.9006
    }
}

def generate_sensor_data(rig_name: str, start_date: datetime, num_events: int = 100) -> list:
    """
    Generates realistic sensor data for oil rig monitoring with meaningful patterns:
    - Temperature: Daily cycles with operational patterns (higher during drilling)
    - Pressure: Correlated with drilling depth and operations (increases during drilling)
    - Water Level: Gradual accumulation with periodic pump-outs

    Args:
        rig_name (str): Name of the oil rig (must be a key in OIL_RIGS)
        start_date (datetime): Starting timestamp for the sensor data
        num_events (int): Number of events to generate. Defaults to 100.

    Returns:
        list: List of dictionaries containing sensor data
    """
    data = []

    # Initialize baseline values for realistic trends
    base_temp = 180.0  # Base temperature in Fahrenheit
    base_pressure = 2500.0  # Base pressure in PSI
    water_level = 120.0  # Starting water level in feet

    # Operation states: simulate drilling cycles
    # Drilling increases temp and pressure, idle periods decrease them
    drilling_cycle_hours = 8  # Hours per drilling cycle
    idle_cycle_hours = 4  # Hours of idle time between drilling

    current_time = start_date
    hours_elapsed = 0

    for _ in range(num_events):
        # Calculate time of day for daily temperature cycle (ambient influence)
        hour_of_day = current_time.hour + current_time.minute / 60.0

        # Daily temperature cycle (ambient temperature effect)
        # Peak around 3 PM (15:00), lowest around 5 AM (5:00)
        daily_temp_variation = 15 * math.sin((hour_of_day - 5) * math.pi / 12)

        # Determine operational state (drilling vs idle)
        cycle_position = hours_elapsed % (drilling_cycle_hours + idle_cycle_hours)
        is_drilling = cycle_position < drilling_cycle_hours

        # Temperature calculation
        if is_drilling:
            # Higher temperature during drilling operations
            operational_temp = base_temp + 80 + random.uniform(-10, 15)
        else:
            # Lower temperature during idle periods
            operational_temp = base_temp + 20 + random.uniform(-5, 10)

        temperature = operational_temp + daily_temp_variation + random.uniform(-5, 5)
        temperature = max(150, min(350, temperature))  # Clamp to realistic range

        # Pressure calculation (correlated with drilling activity)
        if is_drilling:
            # Pressure increases during drilling, with gradual buildup
            drilling_progress = cycle_position / drilling_cycle_hours
            pressure = base_pressure + (1500 * drilling_progress) + random.uniform(-100, 150)
        else:
            # Pressure gradually decreases during idle time
            idle_progress = (cycle_position - drilling_cycle_hours) / idle_cycle_hours
            pressure = base_pressure + (1500 * (1 - idle_progress)) + random.uniform(-80, 100)

        pressure = max(2000, min(5000, pressure))  # Clamp to realistic range

        # Water level calculation (gradual accumulation with periodic pump-outs)
        # Creates realistic saw-tooth pattern
        if is_drilling:
            # Water accumulates steadily during drilling
            water_level += random.uniform(3, 6)
        else:
            # Pump out water during idle periods - gradual throughout idle time
            idle_progress = (cycle_position - drilling_cycle_hours) / idle_cycle_hours

            if idle_progress < 0.6:  # First 60% of idle period (2.4 hours) - active pump-out
                # Pump out more aggressively at the start, then taper off
                pump_rate = 20 * (1 - idle_progress)  # Starts at ~20, decreases to ~8
                water_level -= random.uniform(pump_rate * 0.8, pump_rate * 1.2)
            else:
                # Last 40% of idle - minimal accumulation
                water_level += random.uniform(0.5, 1.5)

        # Keep water level in realistic range with wider bounds
        water_level = max(80, min(450, water_level))

        # Add occasional anomalies (5% chance)
        if random.random() < 0.05:
            # Simulate equipment issues or unusual conditions
            temperature += random.uniform(10, 30)
            pressure += random.uniform(200, 400)

        # Create sensor readings
        for sensor_type, sensor_value in [
            ('temperature', temperature),
            ('pressure', pressure),
            ('water_level', water_level)
        ]:
            data.append({
                'event_id': str(uuid.uuid4()),
                'rig_name': rig_name,
                'location': OIL_RIGS[rig_name]['location'],
                'region': OIL_RIGS[rig_name]['region'],
                'sensor_type': sensor_type,
                'sensor_value': round(sensor_value, 2),
                'timestamp': current_time,
                'latitude': OIL_RIGS[rig_name]['lat'],
                'longitude': OIL_RIGS[rig_name]['lon']
            })

        # Increment time by 15 minutes
        current_time += timedelta(minutes=15)
        hours_elapsed += 0.25  # 15 minutes = 0.25 hours

    return data

def create_oil_rig_events_dataframe(rig_name: str, start_date: datetime = None, num_events: int = 100) -> DataFrame:
    """
    Creates a Spark DataFrame with oil rig sensor events.

    Args:
        rig_name (str): Name of the oil rig (must be a key in OIL_RIGS)
        start_date (datetime): Starting timestamp for the sensor data. Defaults to 2 days ago.
        num_events (int): Number of events to generate. Defaults to 100.

    Returns:
        DataFrame: Spark DataFrame containing oil rig sensor events
    """
    # Initialize Spark session - create one if it doesn't exist
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder \
                .appName("OilRigSensorDataGenerator") \
                .getOrCreate()
    except Exception:
        spark = SparkSession.builder \
            .appName("OilRigSensorDataGenerator") \
            .getOrCreate()
    
    # Define schema
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("rig_name", StringType(), False),
        StructField("location", StringType(), False),
        StructField("region", StringType(), False),
        StructField("sensor_type", StringType(), False),
        StructField("sensor_value", FloatType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False)
    ])
    
    # Default start date if not provided
    if start_date is None:
        start_date = datetime.now() - timedelta(days=2)
    
    # Generate sensor data
    data = generate_sensor_data(rig_name, start_date, num_events)
    
    # Create and return DataFrame
    return spark.createDataFrame(data, schema)

def get_available_rigs() -> list:
    """
    Returns a list of available oil rig names.
    
    Returns:
        list: List of available oil rig names
    """
    return list(OIL_RIGS.keys())

def get_rig_info(rig_name: str) -> dict:
    """
    Returns information about a specific oil rig.
    
    Args:
        rig_name (str): Name of the oil rig
    
    Returns:
        dict: Dictionary containing rig information
    """
    return OIL_RIGS.get(rig_name, {})

def main():
    """Test function to demonstrate the oil rig data generation utilities."""
    print("Testing oil rig sensor data generation:")
    
    # Test with permian rig - 50 events
    print("\n1. Testing Permian Rig with 50 events:")
    permian_df = create_oil_rig_events_dataframe('permian_rig', num_events=50)
    print(f"Generated {permian_df.count()} rows for Permian Rig")
    permian_df.show(5)  # Show first 5 rows
    
    # Test with eagle ford rig - 30 events
    print("\n2. Testing Eagle Ford Rig with 30 events:")
    eagle_ford_df = create_oil_rig_events_dataframe('eagle_ford_rig', num_events=30)
    print(f"Generated {eagle_ford_df.count()} rows for Eagle Ford Rig")
    eagle_ford_df.show(5)  # Show first 5 rows
    
    # Show available rigs
    print(f"\n3. Available rigs: {get_available_rigs()}")
    
    # Show rig info
    print(f"\n4. Permian rig info: {get_rig_info('permian_rig')}")

if __name__ == "__main__":
    main()
