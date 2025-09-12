# E-commerce Order Analytics DLT Pipeline

A comprehensive Databricks Delta Live Tables implementation for batch e-commerce order processing, inspired by modern data lakehouse architectures.

## 📁 Project Structure

```
real-time-orders/
├── README.md                                    # Project overview
├── ecommerce-retail-ordering-system.md         # Architecture documentation
├── pipelines/                                  # DLT pipeline definitions
│   └── ecommerce_orders_dlt/
│       ├── transformations/                    # DLT table definitions
│       ├── configurations/                     # Pipeline configs
│       └── metrics/                           # Operational metrics
├── notebooks/                                  # Development notebooks
├── dashboards/                                 # Analytics dashboards
└── utils/                                     # Utility functions
```

## 🚀 Quick Start

1. **Generate Order Data**: Use the order generators in `../../generators/`
2. **Configure Pipeline**: Set Unity Catalog paths in `pipelines/ecommerce_orders_dlt/configurations/`
3. **Deploy Pipeline**: Create DLT pipeline using configurations
4. **Monitor**: View metrics and dashboards

## 📊 Data Flow

```
Order JSON Batches → Bronze → Silver → Gold → PostgreSQL Metrics
```

- **Bronze**: Raw order ingestion with Auto Loader
- **Silver**: Cleaned data with quality rules  
- **Gold**: Business metrics and analytics
- **Metrics**: Operational monitoring export

## 🔧 Key Features

- ✅ Unity Catalog volume integration
- ✅ Batch processing with `@dlt.table` materialized views
- ✅ Data quality validation and monitoring
- ✅ PostgreSQL metrics export
- ✅ Environment-specific configurations
- ✅ Comprehensive business analytics

---

*Based on proven patterns from Databricks reference architectures for order processing systems.*