# Music Analytics - Databricks Lakeflow Declarative Pipeline

## Overview

This directory contains a **Lakeflow Declarative Pipeline (LDP)** implementation for processing and analyzing the Million Song Dataset. The pipeline demonstrates modern data engineering patterns using declarative transformations with streaming data ingestion and comprehensive data quality validation.

## Pipeline Architecture - Medallion Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MEDALLION ARCHITECTURE                            │
│                      Lakeflow Declarative Pipelines                         │
└─────────────────────────────────────────────────────────────────────────────┘

🥉 BRONZE LAYER (Raw Data)
┌─────────────────────────────────────────────────────────────────────────┐
│  📁 songs_raw                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ • Raw CSV ingestion via Auto Loader                                │ │
│  │ • Tab-separated Million Song Dataset                               │ │
│  │ • 20 fields: artist info, song metadata, audio features            │ │
│  │ • Streaming ingestion with schema enforcement                      │ │
│  │ • Source: /databricks-datasets/songs/data-001                      │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
🥈 SILVER LAYER (Cleaned & Validated)
┌─────────────────────────────────────────────────────────────────────────┐
│  📁 songs_prepared                                                      │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ • Data quality validation with @dlt.expect                         │ │
│  │ • Column renaming (title → song_title)                             │ │
│  │ • Schema standardization and field selection                       │ │
│  │ • Quality checks:                                                  │ │
│  │   ✓ Valid artist names, titles, locations                          │ │
│  │   ✓ Positive durations, tempo, time signatures                     │ │
│  │   ✓ Valid years                                                    │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
🥇 GOLD LAYER (Analytics-Ready)
┌──────────────────────────────────────────────────────────────────────────┐
│  📊 Business Intelligence Views                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │ 🎯 top_artists_by_year                                              │ │
│  │    • Artist song counts per year                                    │ │
│  │    • Ranked by productivity                                         │ │
│  │                                                                     │ │
│  │ 🏆 top_artists_overall                                              │ │
│  │    • All-time artist song counts                                    │ │
│  │    • Career productivity rankings                                   │ │
│  │                                                                     │ │
│  │ 📈 yearly_song_stats                                                │ │
│  │    • Year-over-year summary statistics                              │ │
│  │    • Duration, tempo trends and percentiles                         │ │
│  │                                                                     │ │
│  │ 🌍 artist_location_summary                                          │ │
│  │    • Geographic distribution of music                               │ │
│  │    • Location-based audio characteristics                           │ │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

## Pipeline Visualization

![Million Song Dataset Pipeline](images/dlt_songs_pipeline.png)

*Figure 1: Lakeflow Declarative Pipeline flow for the Million Song Dataset showing the medallion pattern data flow from raw ingestion through analytics-ready gold layer views in the Datarbicks Platform*

## Data Schema

The pipeline processes 20 fields from the Million Song Dataset:

### Core Fields
- **Artist Information**: `artist_id`, `artist_name`, `artist_location`, `artist_lat`, `artist_long`
- **Song Metadata**: `song_id`, `title`, `release`, `year`
- **Audio Features**: `duration`, `tempo`, `loudness`, `key`, `time_signature`
- **Audio Analysis**: `key_confidence`, `time_signature_confidence`, `song_hotnes`
- **Audio Timing**: `end_of_fade_in`, `start_of_fade_out`

## Pipeline Components

### 1. Bronze Layer: Raw Data Ingestion
```python
@dlt.table
def songs_raw():
    """Streaming ingestion with Auto Loader"""
```
- **Technology**: Databricks Auto Loader with cloudFiles
- **Format**: Tab-separated CSV files
- **Schema**: Explicit StructType with 20 fields
- **Processing**: Streaming incremental ingestion

### 2. Silver Layer: Data Preparation & Quality
```python
@dlt.table
@dlt.expect("valid_artist_name", "artist_name IS NOT NULL")
@dlt.expect("valid_duration", "duration > 0")
# ... additional quality checks
def songs_prepared():
```
- **Data Quality**: Multiple `@dlt.expect` validations
- **Transformations**: Column renaming and field selection
- **Validation Rules**: Null checks, positive value constraints

### 3. Gold Layer: Analytics Views

#### `top_artists_by_year`
- Artists ranked by song count per year
- Sorted by productivity and chronologically

#### `top_artists_overall`
- All-time artist song counts
- Career-spanning productivity analysis

#### `yearly_song_stats`
- Temporal trends in music characteristics
- Statistical aggregations: count, average, min/max, median

#### `artist_location_summary`
- Geographic distribution of musical output
- Location-based audio characteristic analysis

## Key Features

### Lakeflow Declarative Pipelines Pattern
- **Declarative**: `@dlt.table` decorators for transformation definition
- **Quality-First**: Built-in data expectations and validation
- **Streaming**: Real-time processing with Auto Loader
- **Lineage**: Automatic dependency tracking between tables

### Data Quality & Validation
- Comprehensive `@dlt.expect` rules for data integrity
- Automatic handling of data quality violations
- Schema enforcement at ingestion

### Scalable Architecture
- Medallion pattern for clear data progression
- Separation of concerns: raw → cleaned → analytics
- Optimized for both streaming and batch workloads

## File Structure

```
src/py/ldp/music_analytics/
├── transformations/
│   └── dlt_songs_pipeline.py       # Main pipeline definition
└── README.md                        # This documentation
```

## Usage

This pipeline is designed to run in a Databricks environment with Delta Live Tables support. The transformations automatically create a dependency graph ensuring proper execution order from bronze through gold layers.

## Technology Stack

- **Databricks**: Unified analytics platform
- **Delta Live Tables**: Declarative pipeline framework
- **PySpark**: Distributed data processing
- **Auto Loader**: Streaming file ingestion
- **Million Song Dataset**: Rich music metadata and audio features

The pipeline showcases modern data engineering best practices with emphasis on data quality, streaming capabilities, and analytics-ready output for music industry insights.
