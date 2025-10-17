from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import importlib.util
import sys
from pathlib import Path


# Get the path to the utility module
util_path = Path(__file__).parent.parent.parent / "utils" / "order_gen_util.py"

# Load the module using importlib.util
spec = importlib.util.spec_from_file_location("order_gen_util", util_path)
order_gen_util = importlib.util.module_from_spec(spec)
sys.modules["order_gen_util"] = order_gen_util
spec.loader.exec_module(order_gen_util)


@dp.materialized_view(
    name="orders_mv",
    comment = "Materialized view that generates random order items using the utility function."
)
def orders_mv() -> DataFrame:
    """
    Materialized view that generates random order items using the utility function.
    
    Returns:
        DataFrame: DataFrame containing random order items.
    """
    return order_gen_util.create_random_order_items()

