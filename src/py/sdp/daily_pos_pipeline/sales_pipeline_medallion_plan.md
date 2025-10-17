# Daily POS Pipeline Implementation Plan - Medallion Architecture

## Overview
MVP daily POS pipeline implementation using Spark Declarative Pipelines (SDP) following the Medallion Architecture pattern: Bronze (raw data), Silver (transformed data), and Gold (analytics-ready data).

## Architecture Design

### Medallion Architecture Layers

**Bronze Layer (Raw Data)**
- Ingest raw sales data from multiple sources
- Preserve original data formats and structure
- Add ingestion metadata (timestamp, source)

**Silver Layer (Transformed Data)**
- Clean and standardize data formats
- Handle missing values and data quality issues
- Normalize schemas across sources

**Gold Layer (Analytics-Ready Data)**
- Create business-ready aggregated views
- Daily sales metrics and KPIs
- Optimized for reporting and analytics

## Pipeline Structure

```
daily_pos_pipeline/
├── pipeline.yml                    # Pipeline configuration
├── run_pipeline.sh                 # Pipeline execution script
├── transformations/
│   ├── bronze/                     # Raw data layer
│   │   ├── pos_raw_sales_orders_mv.py       # POS raw data ingestion
│   │   ├── online_raw_sales_orders_mv.py    # Online store raw data
│   │   └── mobile_raw_sales_orders_mv.py    # Mobile app raw data
│   ├── silver/                     # Cleaned data layer
│   │   ├── pos_processed_sales_orders_mv.sql      # POS data cleaning
│   │   ├── online_processed_sales_orders_mv.sql   # Online data cleaning
│   │   └── mobile_processed_sales_orders_mv.sql   # Mobile data cleaning
│   └── gold/                       # Analytics layer
│       ├── daily_pos_sales_orders_gold_mv.sql    # Daily sales aggregation
│       └── sales_summary_gold_mv.sql  # Sales summary metrics
├── utils/
│   ├── sales_data_gen.py          # Sales data generation utilities
│   ├── sales_schema.py            # Unified sales schema definitions
│   └── data_quality.py           # Data validation and quality utilities
├── query_sales_data.py            # Sales data query module
└── analyze_daily_sales.py         # Sales analytics and reporting
```

## Utility-Centered Architecture

### Utils Module Organization
All shared functionality is centralized in the `utils/` directory:

**`utils/sales_schema.py`** - Schema definitions
```python
from pyspark.sql.types import *

SALES_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("source_channel", StringType(), True),  # 'POS', 'Online', 'Mobile'
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("sale_date", DateType(), True),
    StructField("customer_id", StringType(), True),
    StructField("location_id", StringType(), True)
])
```

**`utils/data_quality.py`** - Data validation utilities
```python
def validate_sales_data(df: DataFrame) -> DataFrame:
    """Apply data quality rules and validations"""
    return df.filter(col("total_amount") > 0)

def standardize_nulls(df: DataFrame) -> DataFrame:
    """Apply consistent null handling across all sources"""
    return df.fillna({
        "product_id": "UNKNOWN",
        "quantity": 1,
        "unit_price": 0.0,
        "customer_id": "GUEST"
    })
```

## Implementation Steps

### 1. Bronze Layer - Raw Data Ingestion

**`utils/sales_data_gen.py`** - Data generation utilities
```python
import importlib.util
from faker import Faker
from pyspark.sql import SparkSession
from .sales_schema import SALES_SCHEMA

def create_sales_data(source_channel: str, num_records: int = 1000) -> DataFrame:
    """Generate synthetic sales data with realistic quality issues"""
    # Implementation with Faker for realistic data generation
    # Include intentional data quality issues for testing
    pass

def load_sample_data(source_channel: str) -> DataFrame:
    """Load sample data for specific source channel"""
    pass
```

**Bronze Materialized Views** (Import from utils)
```python
# transformations/bronze/pos_raw_sales_orders_mv.py
import importlib.util
from pyspark.sql.functions import current_timestamp

# Load utility functions
utils_spec = importlib.util.spec_from_file_location("utils.sales_data_gen", "utils/sales_data_gen.py")
utils_module = importlib.util.module_from_spec(utils_spec)
utils_spec.loader.exec_module(utils_module)

@sdp.materialized_view
def pos_raw_sales_orders_mv() -> DataFrame:
    """Raw POS data with ingestion metadata"""
    return utils_module.create_sales_data('POS') \
        .withColumn("ingestion_timestamp", current_timestamp())
```

### 2. Silver Layer - Data Cleaning and Standardization

**Silver Transformations** (SQL with utils integration)
```sql
-- pos_processed_sales_orders_mv.sql
-- Note: In practice, data quality functions from utils/data_quality.py
-- would be applied via Python transformations for complex logic
CREATE MATERIALIZED VIEW pos_processed_sales_orders_mv AS
SELECT
    transaction_id,
    source_channel,
    COALESCE(product_id, 'UNKNOWN') as product_id,
    COALESCE(quantity, 1) as quantity,
    COALESCE(unit_price, 0.0) as unit_price,
    COALESCE(total_amount, quantity * unit_price) as total_amount,
    DATE(sale_date) as sale_date,
    COALESCE(customer_id, 'GUEST') as customer_id,
    location_id,
    current_timestamp() as processed_timestamp
FROM pos_raw_sales_orders_mv
WHERE total_amount > 0;
```

**Alternative Python-based Silver Layer** (for complex transformations)
```python
# transformations/silver/pos_processed_sales_orders_mv.py
import importlib.util

# Load utility functions
utils_spec = importlib.util.spec_from_file_location("utils.data_quality", "utils/data_quality.py")
utils_module = importlib.util.module_from_spec(utils_spec)
utils_spec.loader.exec_module(utils_module)

@sdp.materialized_view
def pos_processed_sales_orders_mv() -> DataFrame:
    \"\"\"Cleaned and standardized POS sales data\"\"\"
    raw_data = spark.table("pos_raw_sales_orders_mv")

    # Apply utility functions for data cleaning
    cleaned_data = utils_module.standardize_nulls(raw_data)
    validated_data = utils_module.validate_sales_data(cleaned_data)

    return validated_data.withColumn("processed_timestamp", current_timestamp())
```

### 3. Gold Layer - Analytics-Ready Data

**Daily Sales Aggregation**
```sql
-- daily_pos_sales_orders_gold_mv.sql
CREATE MATERIALIZED VIEW daily_pos_sales_orders_gold_mv AS
SELECT
    sale_date,
    source_channel,
    COUNT(*) as transaction_count,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_transaction_value,
    SUM(quantity) as total_items_sold
FROM (
    SELECT * FROM pos_processed_sales_orders_mv
    UNION ALL
    SELECT * FROM online_processed_sales_orders_mv
    UNION ALL
    SELECT * FROM mobile_processed_sales_orders_mv
) unified_sales
GROUP BY sale_date, source_channel;
```

### 4. Pipeline Configuration

**pipeline.yml**
```yaml
definitions:
  - glob:
      include: transformations/bronze/**/*.py
  - glob:
      include: transformations/silver/**/*.sql
  - glob:
      include: transformations/gold/**/*.sql
```

### 5. Execution and Analytics

**run_pipeline.sh**
```bash
#!/bin/bash
if [ -d "spark-warehouse" ]; then
    rm -rf spark-warehouse
fi
spark-pipelines run --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=spark-warehouse
```

**Analytics Module** (`analyze_daily_sales.py`)
- Query gold layer tables for business insights
- Generate daily sales reports
- Create visualizations using Plotly

## Data Flow Summary

1. **Bronze**: Ingest raw sales data from POS, Online, Mobile sources
2. **Silver**: Clean, standardize, and validate data quality
3. **Gold**: Aggregate into business-ready metrics and KPIs
4. **Analytics**: Query and visualize consolidated sales insights

## Benefits of This Utility-Centered Approach

- **Centralized Logic**: All utility functions consolidated in `utils/` directory
- **Reusable Components**: Data generation, schema, and quality functions shared across layers
- **Clear Separation**: Pure transformation logic separated from utility functions
- **Maintainable Code**: Single location for schema definitions and data quality rules
- **Clear Data Lineage**: Bronze → Silver → Gold progression with utils support
- **Consistent Quality**: Standardized data validation across all sources
- **Flexibility**: Easy to add new sources using existing utility functions
- **SDP Integration**: Follows established patterns with `importlib.util` loading

## Key SDP Patterns Used

- `@sdp.materialized_view` decorators for Python transformations
- SQL materialized views for simple data processing
- **Dynamic utility loading with `importlib.util`** - Core pattern for sharing code
- Configuration-driven discovery via `pipeline.yml`
- **Centralized utilities in `utils/`** - Schema, data generation, and quality functions
- Hybrid approach: SQL for simple transforms, Python for complex logic using utils
- Hive-compatible storage in `spark-warehouse/`