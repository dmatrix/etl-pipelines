# ETL Pipelines - Comprehensive Data Engineering Examples

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.1.0.dev1-orange.svg?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-LDP-red.svg?style=flat&logo=databricks&logoColor=white)](https://databricks.com/)
[![UV](https://img.shields.io/badge/UV-Package%20Manager-purple.svg?style=flat)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg?style=flat)](LICENSE)
[![Code Style](https://img.shields.io/badge/Code%20Style-black-black.svg?style=flat)](https://github.com/psf/black)

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg?style=flat)](https://github.com/username/etl-pipelines/graphs/commit-activity)
[![Made with Love](https://img.shields.io/badge/Made%20with-❤️-red.svg?style=flat)](https://github.com/username/etl-pipelines)
[![Open Source](https://badges.frapsoft.com/os/v1/open-source.svg?v=103&style=flat)](https://opensource.org/)

---

A collection of modern ETL pipeline implementations demonstrating different data processing paradigms and frameworks. This repository showcases both Spark Declarative Pipelines (SDP) for analytics workloads and [Databricks Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/) (LDP), formely called DLT, for real-time data processing.

## 📁 Project Structure

```
etl-pipelines/
├── README.md                          # This overview document
├── CLAUDE.md                          # Claude Code configuration
└── src/py/
    ├── sdp/                          # Spark Declarative Pipelines examples
    │   ├── README.md                 # Comprehensive SDP documentation
    │   ├── brickfood/                # E-commerce analytics pipeline
    │   ├── oil_rigs/                 # Industrial IoT monitoring pipeline
    │   └── utils/                    # Shared data generation utilities
    ├── ldp/                          # Lakeflow Declarative Pipelines (Databricks)
    │   └── music_analytics/          # Million Song Dataset analytics pipeline
    │       ├── README.md             # Music analytics documentation
    │       ├── images/               # Pipeline visualization assets
    │       └── transformations/      # LDP transformation definitions
    └── generators/                   # Cross-framework data generators
```

## 🚀 Getting Started

### SDP - Spark Declarative Pipelines
Perfect for batch analytics and data science workloads using PySpark.

```bash
# Navigate to SDP examples
cd src/py/sdp

# Install dependencies with UV
uv sync

# Run BrickFood e-commerce pipeline
python main.py brickfood

# Run Oil Rigs sensor monitoring pipeline
python main.py oil-rigs
```

### LDP - Lakeflow Declarative Pipelines (Databricks)
Ideal for streaming data processing with medallion architecture and data quality validation.

```bash
# Navigate to Music Analytics LDP example
cd src/py/ldp/music_analytics

# Deploy pipeline to Databricks workspace
# Pipeline processes Million Song Dataset with medallion architecture
# See README.md for detailed implementation overview
```

## 📊 Use Cases Demonstrated

### 1. **BrickFood E-commerce Analytics** (SDP)
- **Framework**: Spark Declarative Pipelines
- **Data**: Synthetic e-commerce orders with 20+ product categories
- **Features**: Order lifecycle management, sales tax calculations, business analytics
- **Storage**: Local Spark warehouse with Parquet files
- **Scale**: Development/testing workloads

### 2. **Oil Rigs Industrial Monitoring** (SDP) 
- **Framework**: Spark Declarative Pipelines
- **Data**: IoT sensor data from Texas oil fields (temperature, pressure, water level)
- **Features**: Multi-location monitoring, statistical analysis, interactive visualizations
- **Storage**: Local Spark warehouse with time-series data
- **Scale**: Sensor analytics and operational monitoring

### 3. **Music Analytics - Million Song Dataset** (LDP)
- **Framework**: Databricks Lakeflow Declarative Pipelines
- **Data**: Million Song Dataset with 20 fields of artist, song, and audio features
- **Features**: Medallion architecture (Bronze/Silver/Gold), streaming ingestion, data quality validation
- **Analytics**: Top artists, yearly trends, location-based music analytics, temporal statistics
- **Storage**: Delta tables with comprehensive data lineage
- **Scale**: Production-ready streaming data processing with Auto Loader

## 🛠️ Technologies & Frameworks

### Core Technologies
- **PySpark 4.1.0.dev1**: Latest Spark features with Python API
- **Databricks Lakeflow Declarative Pipelines**: Real-time data processing platform
- **Unity Catalog**: Data governance and lineage tracking
- **UV Package Manager**: Modern Python dependency management
- **Faker**: Realistic synthetic data generation
- **Plotly**: Interactive data visualizations

### Architecture Patterns
- **Declarative Pipelines**: SDP framework with Python decorators
- **Medallion Architecture**: Bronze/Silver/Gold data layers (DLT)
- **Materialized Views**: Efficient data transformation caching
- **Data Quality Framework**: Validation rules and monitoring
- **Shared Utilities**: Reusable data generation components

## 📋 Quick Reference

### SDP Commands
```bash
# Environment setup
cd src/py/sdp && uv sync

# Run pipelines
python main.py brickfood    # E-commerce analytics
python main.py oil-rigs     # IoT sensor monitoring

# Test utilities
uv run sdp-test-orders      # Test order generation
uv run sdp-test-oil-sensors # Test sensor data generation

# Development commands
uv run pytest              # Run tests
uv run black .             # Format code
uv run flake8 .            # Lint code
```

### LDP Commands
```bash
# Navigate to Music Analytics pipeline
cd src/py/ldp/music_analytics

# View pipeline documentation and architecture
cat README.md

# Deploy to Databricks workspace (requires Databricks environment)
# See transformations/dlt_songs_pipeline.py for implementation
```

## 🎯 Learning Objectives

This repository demonstrates:

1. **Framework Comparison**: SDP vs DLT for different use cases
2. **Data Generation**: Realistic synthetic data creation patterns
3. **Pipeline Architecture**: Declarative vs streaming approaches  
4. **Quality Engineering**: Data validation and monitoring strategies
5. **Modern Tooling**: UV, Unity Catalog, and latest Spark features
6. **Production Patterns**: Environment management and deployment workflows

## 📚 Documentation

- **[SDP README.md](src/py/sdp/README.md)**: Comprehensive Spark Declarative Pipelines guide
- **[Music Analytics LDP README](src/py/ldp/music_analytics/README.md)**: Million Song Dataset Lakeflow Declarative Pipelines implementation
- **[CLAUDE.md](CLAUDE.md)**: Claude Code configuration for repository navigation

## 🔧 Development Setup

### Prerequisites
- **Python 3.11+**: Required for all frameworks
- **UV Package Manager**: Modern dependency management
- **Java 11+**: Required by PySpark (handled automatically)
- **Databricks Workspace**: Required for DLT pipelines

### Installation
```bash
# Install UV package manager
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone repository
git clone <repository-url>
cd etl-pipelines

# Setup SDP environment
cd src/py/sdp && uv sync

# Verify installation
uv run python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
```

## 💡 Best Practices Demonstrated

### Code Organization
- **Centralized Utilities**: Shared data generation functions
- **Clear Separation**: Framework-specific implementations
- **Configuration Management**: Environment-specific settings
- **Comprehensive Testing**: Unit tests and validation scripts

### Data Engineering
- **Quality First**: Built-in data validation and monitoring
- **Scalable Patterns**: From development to production
- **Modern Tooling**: Latest framework features and best practices
- **Documentation**: Comprehensive guides and examples

### Pipeline Design
- **Modularity**: Reusable components and transformations  
- **Observability**: Metrics, logging, and monitoring
- **Flexibility**: Support for both batch and streaming workloads
- **Maintainability**: Clear structure and comprehensive documentation

---

*This repository provides practical examples of modern data engineering patterns, suitable for learning and development. Each framework demonstrates different strengths and use cases in the data processing ecosystem.*
