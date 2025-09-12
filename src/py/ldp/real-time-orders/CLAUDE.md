# Claude Code Configuration - E-commerce Order Analytics DLT Pipeline

This document provides configuration and context information for Claude Code to work effectively with the e-commerce order analytics DLT pipeline project.

## Project Overview

This project implements a comprehensive Databricks Delta Live Tables (DLT) pipeline for batch e-commerce order processing, featuring medallion architecture (Bronze/Silver/Gold) with Unity Catalog integration and PostgreSQL metrics export.

## Key Components

### Project Structure
- `pipelines/ecommerce_orders_dlt/` - Core DLT pipeline implementation
  - `transformations/` - Bronze, Silver, Gold layer definitions using `@dlt.table`
  - `configurations/` - Environment-specific JSON configuration files
  - `metrics/` - Operational metrics and PostgreSQL export logic
- `notebooks/` - Development and exploration notebooks
- `dashboards/` - Analytics and monitoring dashboard definitions
- `utils/` - Shared utility functions and helpers

### Data Generators
Order data is generated using the unified generators in `../../generators/`:
- `OrderGenerator` - Creates realistic e-commerce order JSON batches
- Default batch size: 100 orders per batch
- Fields: order_id, order_item, price, items_ordered, status, date_ordered, customer_id, order_timestamp

## Architecture Patterns

### DLT Implementation
- **Bronze Layer**: Raw JSON ingestion with Auto Loader batch mode
- **Silver Layer**: Data quality validation and enrichment using `@dlt.expect_all_or_drop`
- **Gold Layer**: Business metrics and customer analytics
- **Materialized Views**: All tables use `@dlt.table` for batch processing (not streaming)

### Unity Catalog Integration
```
Volume Path: /Volumes/{catalog}/{schema}/{volume}/orders/landing/
Checkpoints: /Volumes/{catalog}/{schema}/{volume}/orders/checkpoints/
Schema: /Volumes/{catalog}/{schema}/{volume}/orders/schema/
```

### Data Quality Framework
- Quality rules defined in `utils/data_quality.py`
- Expectation-based validation with quarantine tables
- Metrics tracking for data quality rates

## Configuration Management

### Environment Files
- `configurations/dev_config.json` - Development environment
- `configurations/staging_config.json` - Staging environment  
- `configurations/prod_config.json` - Production environment

### Configuration Variables
- `CATALOG_NAME` - Unity Catalog catalog
- `SCHEMA_NAME` - Unity Catalog schema
- `VOLUME_NAME` - Unity Catalog volume
- `POSTGRES_HOST` - PostgreSQL host for metrics export
- `POSTGRES_DATABASE` - PostgreSQL database name

## Common Tasks

### Generate Test Data
```bash
cd ../../generators
python general_order_generator.py --interval 1 --end 1 --batch-size 50
```

### Deploy Pipeline
1. Configure environment settings in `configurations/`
2. Create DLT pipeline using Databricks UI or API
3. Set pipeline to triggered mode for batch processing
4. Configure target Unity Catalog location

### Monitor Pipeline
- View pipeline health in `dashboards/pipeline_monitoring.json`
- Check operational metrics exported to PostgreSQL
- Review data quality metrics and expectations

## Reference Architecture

Based on proven patterns from [databricksfree](https://github.com/dmatrix/databricksfree) order processing system, specifically:
- Modular pipeline organization
- Clear separation of concerns
- Comprehensive monitoring and alerting
- Environment-specific configurations

## Development Notes

### DLT Best Practices
- Use `@dlt.table` for all materialized views (not streaming tables)
- Implement triggered execution for batch processing
- Leverage Auto Loader for efficient file processing
- Include comprehensive data quality expectations

### Testing Strategy
- Use `notebooks/pipeline_testing.ipynb` for validation
- Test with small batch sizes during development
- Validate data quality rules and expectations
- Verify PostgreSQL metrics export functionality

### Deployment Workflow
1. **Development**: Test with dev configuration and small datasets
2. **Staging**: Validate full pipeline with realistic data volumes
3. **Production**: Deploy with production configuration and monitoring

## Build & Test Commands

```bash
# Generate test order data
cd ../../generators && python general_order_generator.py --interval 1 --end 1

# Validate pipeline configuration
python utils/config_manager.py --validate --env dev

# Test data quality rules
python utils/data_quality.py --test

# Run pipeline health check
python metrics/pipeline_metrics.py --check
```

## Context for Claude Code

When working with this project:
1. **DLT Focus**: All table definitions use `@dlt.table` materialized views for batch processing
2. **Configuration-Driven**: Environment settings managed through JSON files
3. **Quality-First**: Comprehensive data validation and monitoring
4. **Modular Design**: Clear separation between layers and concerns
5. **Unity Catalog**: Full governance and lineage tracking
6. **Operational Excellence**: Metrics export to existing PostgreSQL systems

The project demonstrates enterprise-grade data engineering practices for e-commerce order analytics with modern lakehouse architecture.