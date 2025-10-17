"""Tests for querying BrickFood materialized views."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing materialized views."""
    spark = (SparkSession.builder
             .appName("BrickFood-MV-Tests")
            #  .config("spark.sql.catalogImplementation", "hive")
            #  .config("spark.sql.warehouse.dir", "file:///./spark-warehouse/")
             .enableHiveSupport()
             .getOrCreate())

    yield spark

    spark.stop()


def test_query_orders_mv(spark):
    """Test querying the orders_mv materialized view."""
    # Query the orders materialized view
    df = spark.read.table("orders_mv")

    # Check that the view exists and has data
    assert df is not None, "orders_mv should exist"

    row_count = df.count()
    assert row_count > 0, f"orders_mv should have data, found {row_count} rows"

    # Check expected columns
    expected_columns = ["order_id", "order_item", "price", "items_ordered", "status", "date_ordered"]
    assert df.columns == expected_columns, f"orders_mv should have columns: {expected_columns}"

    # Show sample data
    print("\n=== orders_mv Sample Data ===")
    df.show(5, truncate=False)
    print(f"Total rows: {row_count}")


def test_query_approved_orders_mv(spark):
    """Test querying the approved_orders_mv materialized view."""
    # Query the approved orders materialized view
    df = spark.read.table("approved_orders_mv")

    # Check that the view exists
    assert df is not None, "approved_orders_mv should exist"

    row_count = df.count()
    print(f"\n=== approved_orders_mv ===")
    print(f"Total approved orders: {row_count}")

    # If there are rows, verify all have status = 'approved'
    if row_count > 0:
        statuses = df.select("status").distinct().collect()
        assert len(statuses) == 1, "approved_orders_mv should only have 'approved' status"
        assert statuses[0].status == "approved", "All orders should have status = 'approved'"

        # Show sample data
        print("\nSample approved orders:")
        df.show(5, truncate=False)
    else:
        print("No approved orders found (this is okay if data is random)")


def test_query_fulfilled_orders_mv(spark):
    """Test querying the fulfilled_orders_mv materialized view."""
    # Query the fulfilled orders materialized view
    df = spark.read.table("fulfilled_orders_mv")

    # Check that the view exists
    assert df is not None, "fulfilled_orders_mv should exist"

    row_count = df.count()
    print(f"\n=== fulfilled_orders_mv ===")
    print(f"Total fulfilled orders: {row_count}")

    # If there are rows, verify all have status = 'fulfilled'
    if row_count > 0:
        statuses = df.select("status").distinct().collect()
        assert len(statuses) == 1, "fulfilled_orders_mv should only have 'fulfilled' status"
        assert statuses[0].status == "fulfilled", "All orders should have status = 'fulfilled'"

        # Show sample data
        print("\nSample fulfilled orders:")
        df.show(5, truncate=False)
    else:
        print("No fulfilled orders found (this is okay if data is random)")


def test_query_pending_orders_mv(spark):
    """Test querying the pending_orders_mv materialized view."""
    # Query the pending orders materialized view
    df = spark.read.table("pending_orders_mv")

    # Check that the view exists
    assert df is not None, "pending_orders_mv should exist"

    row_count = df.count()
    print(f"\n=== pending_orders_mv ===")
    print(f"Total pending orders: {row_count}")

    # If there are rows, verify all have status = 'pending'
    if row_count > 0:
        statuses = df.select("status").distinct().collect()
        assert len(statuses) == 1, "pending_orders_mv should only have 'pending' status"
        assert statuses[0].status == "pending", "All orders should have status = 'pending'"

        # Show sample data
        print("\nSample pending orders:")
        df.show(5, truncate=False)
    else:
        print("No pending orders found (this is okay if data is random)")


def test_verify_status_distribution(spark):
    """Test that orders are properly distributed across all status views."""
    # Get counts from each view
    orders_total = spark.read.table("orders_mv").count()
    approved_count = spark.read.table("approved_orders_mv").count()
    fulfilled_count = spark.read.table("fulfilled_orders_mv").count()
    pending_count = spark.read.table("pending_orders_mv").count()

    # Sum of status-specific views should equal total orders
    sum_of_statuses = approved_count + fulfilled_count + pending_count

    print(f"\n=== Status Distribution ===")
    print(f"Total orders: {orders_total}")
    print(f"Approved: {approved_count}")
    print(f"Fulfilled: {fulfilled_count}")
    print(f"Pending: {pending_count}")
    print(f"Sum: {sum_of_statuses}")

    assert sum_of_statuses == orders_total, \
        f"Sum of status views ({sum_of_statuses}) should equal total orders ({orders_total})"


def test_verify_column_consistency(spark):
    """Test that all materialized views have consistent columns."""
    orders_df = spark.read.table("orders_mv")
    approved_df = spark.read.table("approved_orders_mv")
    fulfilled_df = spark.read.table("fulfilled_orders_mv")
    pending_df = spark.read.table("pending_orders_mv")

    # All views should have the same columns
    expected_columns = orders_df.columns

    assert approved_df.columns == expected_columns, \
        "approved_orders_mv should have same columns as orders_mv"
    assert fulfilled_df.columns == expected_columns, \
        "fulfilled_orders_mv should have same columns as orders_mv"
    assert pending_df.columns == expected_columns, \
        "pending_orders_mv should have same columns as orders_mv"

    print(f"\n=== Column Consistency Check ===")
    print(f"All views have consistent columns: {expected_columns}")


def test_query_orders_by_price_range(spark):
    """Test querying orders in different price ranges across all views."""
    from pyspark.sql.functions import col

    orders_df = spark.read.table("orders_mv")

    # Test different price ranges
    low_price = orders_df.filter(col("price") < 100).count()
    mid_price = orders_df.filter((col("price") >= 100) & (col("price") < 500)).count()
    high_price = orders_df.filter(col("price") >= 500).count()

    print(f"\n=== Price Range Distribution ===")
    print(f"Low price (< $100): {low_price}")
    print(f"Mid price ($100-$500): {mid_price}")
    print(f"High price (>= $500): {high_price}")

    # All orders should be categorized
    total = low_price + mid_price + high_price
    assert total == orders_df.count(), "All orders should be in a price range"


def test_query_orders_by_item(spark):
    """Test querying orders grouped by order_item."""
    from pyspark.sql.functions import count, sum as spark_sum, avg, round as spark_round

    orders_df = spark.read.table("orders_mv")

    # Group by order_item and get statistics
    item_stats = (orders_df
                  .groupBy("order_item")
                  .agg(
                      count("*").alias("order_count"),
                      spark_round(spark_sum("price"), 2).alias("total_value"),
                      spark_round(avg("price"), 2).alias("avg_price")
                  )
                  .orderBy("order_count", ascending=False))

    print(f"\n=== Order Statistics by Item ===")
    item_stats.show(10, truncate=False)

    assert item_stats.count() > 0, "Should have order items"


def test_query_orders_by_date_range(spark):
    """Test querying orders by date range."""
    from pyspark.sql.functions import col, min as spark_min, max as spark_max

    orders_df = spark.read.table("orders_mv")

    # Get date range
    date_range = orders_df.select(
        spark_min("date_ordered").alias("earliest_order"),
        spark_max("date_ordered").alias("latest_order")
    ).first()

    print(f"\n=== Order Date Range ===")
    print(f"Earliest order: {date_range.earliest_order}")
    print(f"Latest order: {date_range.latest_order}")

    assert date_range.earliest_order is not None, "Should have earliest order date"
    assert date_range.latest_order is not None, "Should have latest order date"
