# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains modern ETL pipeline implementations demonstrating different data processing paradigms and frameworks. The project showcases both **Spark Declarative Pipelines (SDP)** for analytics workloads and **Spark Declarative Pipelines (SDP2DBX)** for streaming data processing:

### SDP Examples (src/py/sdp/)
1. **BrickFood** (`src/py/sdp/brickfood/`) - E-commerce order processing and analytics system
2. **Oil Rigs** (`src/py/sdp/oil_rigs/`) - Industrial IoT sensor monitoring and analysis system

### SDP2DBX Examples (src/py/sdp2dbx/)
1. **Music Analytics** (`src/py/sdp2dbx/music_analytics/`) - Million Song Dataset processing with medallion architecture

The project is structured as a uv-managed Python package with virtual environment isolation and modern dependency management.

## Development Commands

### Environment Setup
```bash
# Navigate to the SDP directory
cd src/py/sdp

# Install dependencies and create virtual environment
uv sync

# Activate virtual environment (optional, uv run handles this)
source .venv/bin/activate
```

### Running Pipelines

#### SDP Pipelines
```bash
# Run BrickFood pipeline
cd src/py/sdp/brickfood && ./run_pipeline.sh
# OR
cd src/py/sdp && python main.py brickfood

# Run Oil Rigs pipeline
cd src/py/sdp/oil_rigs && ./run_pipeline.sh
# OR
cd src/py/sdp && python main.py oil-rigs

# Run with spark-pipelines CLI directly
spark-pipelines run --conf spark.sql.catalogImplementation=hive --conf spark.sql.warehouse.dir=spark-warehouse
```

#### SDP2DBX Pipelines
```bash
# Music Analytics pipeline (Databricks Spark Declarative Pipelines)
cd src/py/sdp2dbx/music_analytics

# View pipeline documentation and architecture
cat README.md

# Deploy to Databricks workspace (requires Databricks environment)
# See transformations/ldp_musical_pipeline.py for implementation
```

### Development and Testing Commands

#### Running Tests
```bash
# Install dev dependencies (includes pytest)
cd src/py/sdp && uv sync --extra dev

# Run BrickFood tests (9 comprehensive tests for materialized views)
cd src/py/sdp/brickfood && uv run pytest tests/ -v

# Run tests with detailed output (shows query results)
cd src/py/sdp/brickfood && uv run pytest -v -s

# Run specific test function
cd src/py/sdp/brickfood && uv run pytest tests/test_materialized_views.py::test_query_orders_mv -v

# Run tests matching a pattern
cd src/py/sdp/brickfood && uv run pytest -k "orders" -v
```

#### Code Quality Tools
```bash
# Code formatting and linting
cd src/py/sdp && uv run black .
cd src/py/sdp && uv run flake8 .
cd src/py/sdp && uv run mypy .

# Install package in development mode
cd src/py/sdp && uv pip install -e .
```

#### Query and Analysis Scripts
```bash
# Use script commands defined in pyproject.toml
sdp-brickfood    # Run BrickFood queries
sdp-oil-rigs     # Run Oil Rigs queries

# Or run directly from brickfood directory
cd src/py/sdp/brickfood
uv run python query_tables.py           # Query approved orders
uv run python calculate_sales_tax.py     # Calculate sales tax and analytics
```

## Architecture Overview

### Core Framework Components

#### SDP (Spark Declarative Pipelines)
- **Framework**: Python decorators and SQL for declarative data transformations
- **Materialized Views**: Data transformations defined with `@sdp.materialized_view` decorator
- **Pipeline Configuration**: YAML files (`pipeline.yml`) define transformation discovery patterns using glob patterns
- **Storage**: Local Spark warehouse with Hive-compatible storage

#### SDP2DBX (Spark Declarative Pipelines for Databricks)
- **Framework**: Databricks native declarative pipeline framework (formerly Delta Live Tables)
- **Medallion Architecture**: Bronze/Silver/Gold data layers with automatic lineage
- **Data Quality**: Built-in expectations and validation with `@dp.expect`
- **Storage**: Delta tables with Unity Catalog integration

### Project Structure Patterns

#### SDP Pipeline Structure
```
sdp_pipeline_name/
├── pipeline.yml              # Pipeline configuration with glob patterns
├── transformations/           # Data transformation definitions
│   ├── *.py                  # Python-based transformations with @sdp.materialized_view
│   └── *.sql                 # SQL-based transformations
├── tests/                    # Test suite for materialized views
│   └── test_materialized_views.py  # Comprehensive tests for querying views
├── artifacts/utils/          # Pipeline-specific utilities
├── run_pipeline.sh           # Pipeline execution script
├── query_tables.py           # Query and display data
├── calculate_sales_tax.py    # Sales tax calculations (BrickFood)
└── *.py                      # Other query and analysis modules
```

#### SDP2DBX Pipeline Structure
```
sdp2dbx_pipeline_name/
├── README.md                 # Comprehensive pipeline documentation
├── images/                   # Pipeline visualization assets
└── transformations/          # SDP2DBX transformation definitions
    └── *.py                  # Python files with @dp.table decorators
```

### Data Flow Architecture

#### SDP Data Flow
1. **Data Generation**: Utility modules generate synthetic data using Faker library
2. **Transformations**: Materialized views process data using both Python and SQL transformations
3. **Storage**: Data persists to Hive-compatible spark-warehouse directory
4. **Analytics**: Query modules provide data access and visualization capabilities

#### SDP2DBX Data Flow (Music Analytics)
1. **Bronze Layer**: Raw data ingestion from Million Song Dataset with Auto Loader (`songs_raw`)
2. **Silver Layer**: Specialized data preparation with comprehensive validation
   - `songs_metadata_silver`: Release and temporal information with year/duration validation
   - `songs_audio_features_silver`: Musical characteristics with tempo/time signature validation
3. **Gold Layer**: Advanced analytics views across three categories:
   - **Temporal Analytics**: `top_artists_by_year`, `yearly_song_stats`, `release_trends_gold`, `artist_location_summary`
   - **Artist Analytics**: `top_artists_overall`, `artist_discography_gold`, `comprehensive_artist_profile_gold`
   - **Musical Analysis**: `musical_characteristics_gold`, `tempo_time_signature_analysis_gold`
4. **Visualization**: Comprehensive README with updated medallion architecture diagrams

### Key Framework Patterns

#### SDP Patterns
- **Decorator-based Transformations**: `@sdp.materialized_view` decorator converts functions to Spark transformations
- **Dynamic Module Loading**: Utility modules loaded via `importlib.util` for cross-pipeline code sharing
- **Hybrid SQL/Python**: SQL files and Python functions seamlessly integrated in transformation pipeline
- **Configuration-driven Discovery**: `pipeline.yml` uses glob patterns to auto-discover transformation files

#### SDP on Databricks  Patterns
- **Medallion Architecture**: Bronze/Silver/Gold progression with clear data lineage and specialized silver tables
- **Data Quality Framework**: Comprehensive `@dp.expect` decorators for validation rules (tempo ranges, year validation, duration checks)
- **Streaming Ingestion**: Auto Loader for incremental data processing with schema enforcement
- **Declarative Definitions**: `@dp.table` decorators for transformation specification with automatic dependency resolution
- **Specialized Silver Tables**: Domain-focused tables (`metadata_silver`, `audio_features_silver`) for targeted analytics
- **Advanced Gold Analytics**: Multi-dimensional analysis tables combining temporal, artist, and musical perspectives

## Important Dependencies

### SDP Dependencies
- **PySpark 4.1.0rc2**: Core Spark functionality with latest features (installed from local packages)
- **pyspark-connect 4.1.0rc2**: Spark Connect support for remote Spark clusters (installed from local packages)
- **Python 3.12+**: Required for the project
- **faker**: Synthetic data generation for realistic test datasets
- **plotly**: Data visualization capabilities for analytics
- **pytest, black, flake8, mypy**: Development and code quality tools (install with `uv sync --extra dev`)
- **databricks-sdk**: Databricks SDK for platform integration

### SDP2DBX Dependencies
- **Databricks Runtime**: Required for Spark Declarative Pipelines
- **Spark Declarative Pipelines**: Declarative pipeline framework (formerly Delta Live Tables)
- **Auto Loader**: Streaming file ingestion capability
- **Unity Catalog**: Data governance and lineage tracking

## Working with Transformations

### SDP Transformations
- Transformations in `transformations/` directories are auto-discovered via pipeline.yml glob patterns
- Python transformations must use `@sdp.materialized_view` decorator and return DataFrame
- SQL transformations are standard .sql files processed by the SDP framework
- Shared utilities are in `utils/` and loaded dynamically across pipelines

### SDP Testing
- Tests are organized under each pipeline's `tests/` directory (e.g., `brickfood/tests/`)
- Test files use pytest framework with simple function-based tests (no classes)
- BrickFood test suite includes 9 comprehensive tests:
  - **Core Tests**: Query all materialized views (orders_mv, approved_orders_mv, fulfilled_orders_mv, pending_orders_mv)
  - **Validation Tests**: Verify status distribution and column consistency
  - **Analytics Tests**: Price range analysis, item-level statistics, date range validation
- Run tests from pipeline directory: `cd brickfood && uv run pytest tests/ -v`
- All tests validate data integrity, schema consistency, and analytical queries

### SDP on Databricks Transformations
- Use `@dp.table` decorator to define materialized views
- Apply `@dp.expect` decorators for data quality validation
- Leverage `dp.read()` for referencing upstream tables
- Auto Loader handles streaming data ingestion with schema evolution

## Project Structure Overview
```
etl-pipelines/
├── CLAUDE.md                 # Claude Code guidance (this file)
├── README.md                 # Project overview and setup guide
└── src/py/
    ├── sdp/                  # Spark Declarative Pipelines
    │   ├── pyproject.toml    # UV project configuration
    │   ├── README.md         # Comprehensive SDP documentation
    │   ├── brickfood/        # E-commerce analytics pipeline
    │   │   ├── tests/        # Test suite with 9 tests for materialized views
    │   │   ├── transformations/  # Orders materialized views (Python + SQL)
    │   │   ├── query_tables.py   # Query approved orders
    │   │   └── calculate_sales_tax.py  # Sales tax analytics
    │   ├── oil_rigs/         # IoT sensor monitoring pipeline
    │   └── utils/            # Shared utilities (order_gen_util, oil_gen_util)
    ├── sdp2dbx/              # Spark Declarative Pipelines (Databricks)
    │   └── music_analytics/  # Million Song Dataset processing
    └── generators/           # Cross-framework data generators
```

## BrickFood Pipeline Details

The BrickFood pipeline demonstrates e-commerce order processing with comprehensive testing:

### Materialized Views
1. **orders_mv** - Base orders table (100 orders with random data)
2. **approved_orders_mv** - Filtered view for approved orders
3. **fulfilled_orders_mv** - Filtered view for fulfilled orders
4. **pending_orders_mv** - Filtered view for pending orders

### Analytics Scripts
- **query_tables.py** - Queries and displays approved orders
- **calculate_sales_tax.py** - Calculates 15% sales tax, provides:
  - Individual order totals with tax
  - Summary statistics (total sales, tax collected, grand total)
  - Breakdown by product item

### Test Suite (tests/test_materialized_views.py)
All 9 tests pass successfully and cover:
- Querying all 4 materialized views
- Validating status distribution (sum of status views = total orders)
- Verifying column consistency across views
- Price range analytics (< $100, $100-$500, >= $500)
- Product-level statistics and aggregations
- Date range validation