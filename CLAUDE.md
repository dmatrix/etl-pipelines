# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains modern ETL pipeline implementations demonstrating different data processing paradigms and frameworks. The project showcases both **Spark Declarative Pipelines (SDP)** for analytics workloads and **Lakeflow Declarative Pipelines (LDP)** for streaming data processing:

### SDP Examples (src/py/sdp/)
1. **BrickFood** (`src/py/sdp/brickfood/`) - E-commerce order processing and analytics system
2. **Oil Rigs** (`src/py/sdp/oil_rigs/`) - Industrial IoT sensor monitoring and analysis system

### LDP Examples (src/py/ldp/)
1. **Music Analytics** (`src/py/ldp/music_analytics/`) - Million Song Dataset processing with medallion architecture

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

#### LDP Pipelines
```bash
# Music Analytics pipeline (Databricks Lakeflow Declarative Pipelines)
cd src/py/ldp/music_analytics

# View pipeline documentation and architecture
cat README.md

# Deploy to Databricks workspace (requires Databricks environment)
# See transformations/ldp_musical_pipeline.py for implementation
```

### Development and Testing Commands
```bash
# Run tests
cd src/py/sdp && uv run pytest

# Code formatting and linting
cd src/py/sdp && uv run black .
cd src/py/sdp && uv run flake8 .
cd src/py/sdp && uv run mypy .

# Install package in development mode
cd src/py/sdp && uv pip install -e .

# Use script commands defined in pyproject.toml
sdp-brickfood    # Run BrickFood queries
sdp-oil-rigs     # Run Oil Rigs queries
```

## Architecture Overview

### Core Framework Components

#### SDP (Spark Declarative Pipelines)
- **Framework**: Python decorators and SQL for declarative data transformations
- **Materialized Views**: Data transformations defined with `@sdp.materialized_view` decorator
- **Pipeline Configuration**: YAML files (`pipeline.yml`) define transformation discovery patterns using glob patterns
- **Storage**: Local Spark warehouse with Hive-compatible storage

#### LDP (Lakeflow Declarative Pipelines)
- **Framework**: Databricks native declarative pipeline framework (formerly Delta Live Tables)
- **Medallion Architecture**: Bronze/Silver/Gold data layers with automatic lineage
- **Data Quality**: Built-in expectations and validation with `@dlt.expect`
- **Storage**: Delta tables with Unity Catalog integration

### Project Structure Patterns

#### SDP Pipeline Structure
```
sdp_pipeline_name/
├── pipeline.yml              # Pipeline configuration with glob patterns
├── transformations/           # Data transformation definitions
│   ├── *.py                  # Python-based transformations with @sdp.materialized_view
│   └── *.sql                 # SQL-based transformations
├── artifacts/utils/          # Pipeline-specific utilities
├── run_pipeline.sh           # Pipeline execution script
└── *.py                      # Query and analysis modules
```

#### LDP Pipeline Structure
```
ldp_pipeline_name/
├── README.md                 # Comprehensive pipeline documentation
├── images/                   # Pipeline visualization assets
└── transformations/          # LDP transformation definitions
    └── *.py                  # Python files with @dlt.table decorators
```

### Data Flow Architecture

#### SDP Data Flow
1. **Data Generation**: Utility modules generate synthetic data using Faker library
2. **Transformations**: Materialized views process data using both Python and SQL transformations
3. **Storage**: Data persists to Hive-compatible spark-warehouse directory
4. **Analytics**: Query modules provide data access and visualization capabilities

#### LDP Data Flow (Music Analytics)
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

#### LDP Patterns
- **Medallion Architecture**: Bronze/Silver/Gold progression with clear data lineage and specialized silver tables
- **Data Quality Framework**: Comprehensive `@dlt.expect` decorators for validation rules (tempo ranges, year validation, duration checks)
- **Streaming Ingestion**: Auto Loader for incremental data processing with schema enforcement
- **Declarative Definitions**: `@dlt.table` decorators for transformation specification with automatic dependency resolution
- **Specialized Silver Tables**: Domain-focused tables (`metadata_silver`, `audio_features_silver`) for targeted analytics
- **Advanced Gold Analytics**: Multi-dimensional analysis tables combining temporal, artist, and musical perspectives

## Important Dependencies

### SDP Dependencies
- **PySpark 4.1.0.dev1**: Core Spark functionality with latest features
- **pyspark-connect**: Spark Connect support for remote Spark clusters
- **faker**: Synthetic data generation for realistic test datasets
- **plotly**: Data visualization capabilities for analytics
- **pytest, black, flake8, mypy**: Development and code quality tools

### LDP Dependencies
- **Databricks Runtime**: Required for Lakeflow Declarative Pipelines
- **Lakeflow Declarative Pipelines**: Declarative pipeline framework (now called LDP)
- **Auto Loader**: Streaming file ingestion capability
- **Unity Catalog**: Data governance and lineage tracking

## Working with Transformations

### SDP Transformations
- Transformations in `transformations/` directories are auto-discovered via pipeline.yml glob patterns
- Python transformations must use `@sdp.materialized_view` decorator and return DataFrame
- SQL transformations are standard .sql files processed by the SDP framework
- Shared utilities are in `utils/` and loaded dynamically across pipelines

### LDP Transformations
- Use `@dlt.table` decorator to define materialized views
- Apply `@dlt.expect` decorators for data quality validation
- Leverage `dlt.read()` for referencing upstream tables
- Auto Loader handles streaming data ingestion with schema evolution

## Project Structure Overview
```
etl-pipelines/
├── src/py/
│   ├── sdp/                  # Spark Declarative Pipelines
│   │   ├── brickfood/        # E-commerce analytics
│   │   ├── oil_rigs/         # IoT sensor monitoring
│   │   └── utils/            # Shared utilities
│   ├── ldp/                  # Lakeflow Declarative Pipelines
│   │   └── music_analytics/  # Million Song Dataset processing
│   └── generators/           # Cross-framework data generators
└── README.md                 # Project overview and setup guide
```