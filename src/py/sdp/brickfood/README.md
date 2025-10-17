# BrickFood - E-commerce Order Processing Pipeline

## Overview

BrickFood is a complete e-commerce order processing and analytics system built using Spark Declarative Pipelines (SDP). It demonstrates synthetic order data generation, materialized view transformations, and comprehensive sales analytics including tax calculations.

## Project Structure

```
brickfood/
├── README.md                       # This file
├── pipeline.yml                    # SDP pipeline configuration
├── run_pipeline.sh                 # Pipeline execution script
├── __init__.py                     # Python package initialization
├── transformations/                # Data transformation definitions
│   ├── orders_mv.py                # Main orders materialized view (Python)
│   ├── approved_orders_mv.sql      # Approved orders filter (SQL)
│   ├── fulfilled_orders_mv.sql     # Fulfilled orders filter (SQL)
│   └── pending_orders_mv.sql       # Pending orders filter (SQL)
├── query_tables.py                 # Query and display order data
├── calculate_sales_tax.py          # Sales tax calculations and analytics
├── artifacts/                      # Build artifacts
│   └── utils/                      # Pipeline-specific utilities
│       └── order_gen_util.py       # Order data generation utilities
├── spark-warehouse/                # Generated Spark warehouse data (auto-generated)
└── metastore_db/                   # Derby database files (auto-generated)
```

## Features

- **Synthetic Data Generation**: Creates realistic order data using Faker library
- **Materialized Views**: Declarative transformations using Python and SQL
- **Order Status Filtering**: Separate views for approved, fulfilled, and pending orders
- **Sales Tax Calculations**: Comprehensive tax computation and analytics
- **Analytics Queries**: Ready-to-use query scripts for data analysis

## Running the Pipeline

### Option 1: Using the Shell Script

```bash
cd /path/to/etl-pipelines/src/py/sdp/brickfood
./run_pipeline.sh
```

### Option 2: Using Python Directly

```bash
cd /path/to/etl-pipelines/src/py/sdp
python main.py brickfood
```

### Option 3: Using SDP CLI

```bash
cd /path/to/etl-pipelines/src/py/sdp/brickfood
spark-pipelines run --conf spark.sql.catalogImplementation=hive --conf spark.sql.warehouse.dir=spark-warehouse
```

## Data Transformations

### 1. Orders Materialized View (orders_mv.py)

The base materialized view that generates random order items using the order generation utility:

- **Name**: `orders_mv`
- **Type**: Python transformation with `@dp.materialized_view` decorator
- **Data Source**: Dynamically generated using `order_gen_util.create_random_order_items()`
- **Schema**:
  - `order_id`: Unique order identifier (UUID)
  - `order_item`: Product name
  - `price`: Item price
  - `items_ordered`: Quantity ordered
  - `status`: Order status (approved/pending/fulfilled)
  - `date_ordered`: Order date

### 2. Approved Orders View (approved_orders_mv.sql)

SQL-based materialized view filtering for approved orders:

```sql
CREATE MATERIALIZED VIEW approved_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'approved';
```

### 3. Fulfilled Orders View (fulfilled_orders_mv.sql)

SQL-based materialized view for fulfilled orders:

```sql
CREATE MATERIALIZED VIEW fulfilled_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'fulfilled';
```

### 4. Pending Orders View (pending_orders_mv.sql)

SQL-based materialized view for pending orders:

```sql
CREATE MATERIALIZED VIEW pending_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'pending';
```

## Query Scripts

### 1. Query Tables (query_tables.py)

Queries the orders materialized view and displays approved orders with selected fields.

**Run the script:**
```bash
cd /path/to/etl-pipelines/src/py/sdp/brickfood
python query_tables.py
```

**Sample Output:**
```
+------------------------------------+--------+-------------+------+
|order_id                            |status  |order_item   |price |
+------------------------------------+--------+-------------+------+
|8552e9db-0c10-4fe6-92d5-a0c950a0975a|approved|Scooter      |905.52|
|9346dcad-41c2-43ed-abe5-10b2749e23e3|approved|Headphones   |491.98|
|efebeee5-1cb6-42fc-a67a-9e46e416ca8b|approved|Board Game   |694.2 |
|a6197d4f-7b1b-44a7-bfef-5d74cdde1967|approved|Tennis Racket|185.34|
|91a83429-47be-48f8-92d2-d80ea960e313|approved|Headphones   |232.26|
|0d280b01-55ba-4674-ac71-98ffcae76708|approved|Video Game   |250.04|
|4b8e8c2f-c405-49b1-a26e-b9190a723cbf|approved|Basketball   |525.96|
|d8992d6f-ee64-42ea-8008-f31c03e3618d|approved|Action Figure|12.09 |
|daea3b08-d9c8-4276-bc7c-c7bc6d7e2c1e|approved|Video Game   |114.82|
|bc2bcb5c-ea2e-4f8e-b32c-978445f7a9fa|approved|Tennis Racket|726.08|
+------------------------------------+--------+-------------+------+
only showing top 10 rows
```

### 2. Calculate Sales Tax (calculate_sales_tax.py)

Calculates total order prices and applies 15% sales tax to approved orders. Provides detailed analytics including:
- Individual order totals with tax
- Summary statistics (total sales, tax collected, grand total)
- Breakdown by order item

**Run the script:**
```bash
cd /path/to/etl-pipelines/src/py/sdp/brickfood
python calculate_sales_tax.py
```

**Sample Output:**

#### Approved Orders with Total Prices and Sales Tax (15%)
```
+------------------------------------+-------------+------+-------------+------------+-----------+---------+--------------+
|order_id                            |order_item   |price |items_ordered|date_ordered|total_price|sales_tax|total_with_tax|
+------------------------------------+-------------+------+-------------+------------+-----------+---------+--------------+
|8552e9db-0c10-4fe6-92d5-a0c950a0975a|Scooter      |905.52|6            |2025-09-19  |5433.12    |814.97   |6248.09       |
|9346dcad-41c2-43ed-abe5-10b2749e23e3|Headphones   |491.98|4            |2025-09-30  |1967.92    |295.19   |2263.11       |
|efebeee5-1cb6-42fc-a67a-9e46e416ca8b|Board Game   |694.2 |3            |2025-10-15  |2082.6     |312.39   |2394.99       |
|a6197d4f-7b1b-44a7-bfef-5d74cdde1967|Tennis Racket|185.34|2            |2025-10-06  |370.68     |55.6     |426.28        |
|91a83429-47be-48f8-92d2-d80ea960e313|Headphones   |232.26|9            |2025-10-16  |2090.34    |313.55   |2403.89       |
|0d280b01-55ba-4674-ac71-98ffcae76708|Video Game   |250.04|4            |2025-09-21  |1000.16    |150.02   |1150.18       |
|4b8e8c2f-c405-49b1-a26e-b9190a723cbf|Basketball   |525.96|10           |2025-09-25  |5259.6     |788.94   |6048.54       |
|d8992d6f-ee64-42ea-8008-f31c03e3618d|Action Figure|12.09 |7            |2025-09-23  |84.63      |12.69    |97.32         |
|daea3b08-d9c8-4276-bc7c-c7bc6d7e2c1e|Video Game   |114.82|3            |2025-09-29  |344.46     |51.67    |396.13        |
|bc2bcb5c-ea2e-4f8e-b32c-978445f7a9fa|Tennis Racket|726.08|5            |2025-09-28  |3630.4     |544.56   |4174.96       |
+------------------------------------+-------------+------+-------------+------------+-----------+---------+--------------+
only showing top 10 rows
```

#### Summary Statistics
```
+----------------------+-------------------+--------------------+
|total_sales_before_tax|total_tax_collected|total_sales_with_tax|
+----------------------+-------------------+--------------------+
|83816.45              |12572.47           |96388.92            |
+----------------------+-------------------+--------------------+
```

#### Breakdown by Order Item
```
+---------------+----------------------+-------------------+--------------------+
|order_item     |total_sales_before_tax|total_tax_collected|total_sales_with_tax|
+---------------+----------------------+-------------------+--------------------+
|Scooter        |12917.17              |1937.57            |14854.74            |
|Basketball     |12726.2               |1908.94            |14635.14            |
|Headphones     |11474.02              |1721.1             |13195.12            |
|Action Figure  |7531.81               |1129.76            |8661.57             |
|Video Game     |7306.82               |1096.02            |8402.84             |
|Drone          |7280.52               |1092.08            |8372.6              |
|Toy Car        |4912.7                |736.91             |5649.61             |
|Camera         |4099.16               |614.88             |4714.04             |
|Tennis Racket  |4001.08               |600.16             |4601.24             |
|Laptop         |3585.0                |537.75             |4122.75             |
|Electric Guitar|3459.2                |518.88             |3978.08             |
|Board Game     |2082.6                |312.39             |2394.99             |
|Smartwatch     |1906.84               |286.03             |2192.87             |
|Tablet         |285.65                |42.85              |328.5               |
|Puzzle         |247.68                |37.15              |284.83              |
+---------------+----------------------+-------------------+--------------------+
```

## Key Insights from Sales Data

Based on the sample output above:

- **Total Revenue**: $83,816.45 in sales before tax
- **Tax Revenue**: $12,572.47 collected at 15% tax rate
- **Grand Total**: $96,388.92 including tax
- **Top Selling Items**:
  1. Scooter: $14,854.74 (with tax)
  2. Basketball: $14,635.14 (with tax)
  3. Headphones: $13,195.12 (with tax)

## Technical Details

### Data Generation

The pipeline uses the `order_gen_util.create_random_order_items()` function to generate synthetic order data with realistic attributes:
- Random product selection from a predefined catalog
- Randomized pricing and quantities
- Varied order statuses (approved, pending, fulfilled)
- Date generation for temporal analysis

### Materialized View Pattern

The pipeline demonstrates the SDP framework's hybrid approach:
- **Python transformations**: Base data generation using decorators
- **SQL transformations**: Downstream filtering and aggregations
- **Dynamic module loading**: Cross-pipeline code sharing via importlib

### Pipeline Configuration

The `pipeline.yml` file uses glob patterns to auto-discover transformations:
```yaml
name: brickfood
storage: storage-root
libraries:
  - glob:
      include: transformations/**
```

## Dependencies

- PySpark 4.1.0.dev1
- Faker (for data generation)
- Plotly (for visualization capabilities)

## Next Steps

1. **Extend Analytics**: Add more complex aggregations and time-series analysis
2. **Add Visualizations**: Create Plotly charts for sales trends
3. **Customer Segmentation**: Add customer data and segmentation views
4. **Inventory Integration**: Track inventory levels based on order data
5. **Revenue Forecasting**: Build predictive models for sales forecasting

## Related Pipelines

- **Oil Rigs Pipeline**: Industrial IoT sensor monitoring example ([../oil_rigs/](../oil_rigs/))
- **Music Analytics Pipeline**: Lakeflow Declarative Pipelines example ([../../ldp/music_analytics/](../../ldp/music_analytics/))
