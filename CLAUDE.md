# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains **Spark Declarative Pipelines (SDP)** examples demonstrating data pipeline development using Apache Spark with PySpark 4.1.0.dev1. The project showcases two complete ETL pipeline implementations:

1. **BrickFood** (`src/py/sdp/brickfood/`) - E-commerce order processing and analytics system
2. **Oil Rigs** (`src/py/sdp/oil_rigs/`) - Industrial IoT sensor monitoring and analysis system

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
- **Spark Declarative Pipelines (SDP)**: Framework using Python decorators and SQL for declarative data transformations
- **Materialized Views**: Data transformations defined with `@sdp.materialized_view` decorator
- **Pipeline Configuration**: YAML files (`pipeline.yml`) define transformation discovery patterns using glob patterns

### Project Structure Pattern
Each pipeline follows this structure:
```
pipeline_name/
├── pipeline.yml              # Pipeline configuration with glob patterns
├── transformations/           # Data transformation definitions
│   ├── *.py                  # Python-based transformations with @sdp.materialized_view
│   └── *.sql                 # SQL-based transformations
├── artifacts/utils/          # Pipeline-specific utilities
├── run_pipeline.sh           # Pipeline execution script
└── *.py                      # Query and analysis modules
```

### Data Flow Architecture
1. **Data Generation**: Utility modules generate synthetic data using Faker library
2. **Transformations**: Materialized views process data using both Python and SQL transformations
3. **Storage**: Data persists to Hive-compatible spark-warehouse directory
4. **Analytics**: Query modules provide data access and visualization capabilities

### Key Framework Patterns
- **Decorator-based Transformations**: `@sdp.materialized_view` decorator converts functions to Spark transformations
- **Dynamic Module Loading**: Utility modules loaded via `importlib.util` for cross-pipeline code sharing
- **Hybrid SQL/Python**: SQL files and Python functions seamlessly integrated in transformation pipeline
- **Configuration-driven Discovery**: `pipeline.yml` uses glob patterns to auto-discover transformation files

## Important Dependencies
- **PySpark 4.1.0.dev1**: Core Spark functionality with latest features
- **pyspark-connect**: Spark Connect support for remote Spark clusters  
- **faker**: Synthetic data generation for realistic test datasets
- **plotly**: Data visualization capabilities for analytics
- **pytest, black, flake8, mypy**: Development and code quality tools

## Working with Transformations
- Transformations in `transformations/` directories are auto-discovered via pipeline.yml glob patterns
- Python transformations must use `@sdp.materialized_view` decorator and return DataFrame
- SQL transformations are standard .sql files processed by the SDP framework
- Shared utilities are in `utils/` and loaded dynamically across pipelines