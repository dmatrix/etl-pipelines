# Databricks notebook source
# ldp_songs_dataset.py
# -----------------------------------------------------------
# Lakeflow Declarative Pipelines implementation for the Million Song Dataset
# Demonstrates LDP patterns with streaming data ingestion
# -----------------------------------------------------------

import dlt
from pyspark.sql.functions import desc, expr
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructType,
    StructField,
)

# Location of the raw subset of the Million Song Dataset
file_path = "/databricks-datasets/songs/data-001"

# Explicit schema for the tab-separated CSV files
schema = StructType(
    [
        StructField("artist_id", StringType(), True),
        StructField("artist_lat", DoubleType(), True),
        StructField("artist_long", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("end_of_fade_in", DoubleType(), True),
        StructField("key", IntegerType(), True),
        StructField("key_confidence", DoubleType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("release", StringType(), True),
        StructField("song_hotnes", DoubleType(), True),
        StructField("song_id", StringType(), True),
        StructField("start_of_fade_out", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("time_signature", DoubleType(), True),
        StructField("time_signature_confidence", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("partial_sequence", IntegerType(), True),
    ]
)

# -----------------------------------------------------------
# Raw ingestion table
# -----------------------------------------------------------
@dlt.table(
    comment=(
        "Raw data from a subset of the Million Song Dataset; a collection "
        "of features and metadata for contemporary music tracks. Processed via Lakeflow Declarative Pipelines."
    )
)
def songs_raw():
    """Ingest new CSV files as they arrive with Auto Loader."""
    return (
        spark.readStream.format("cloudFiles")
        .schema(schema)
        .option("cloudFiles.format", "csv")
        .option("sep", "\t")
        .load(file_path)
    )

# -----------------------------------------------------------
# Cleaned & validated view
# -----------------------------------------------------------
@dlt.table(comment="Million Song Dataset with data cleaned and prepared for analysis using Lakeflow Declarative Pipelines.")
@dlt.expect("valid_artist_name", "artist_name IS NOT NULL")
@dlt.expect("valid_title", "song_title IS NOT NULL")
@dlt.expect("artist_location", "artist_location IS NOT NULL")
@dlt.expect("valid_duration", "duration > 0")
@dlt.expect("valid_tempo", "tempo > 0")
@dlt.expect("valid_time_signature", "time_signature > 0")
@dlt.expect("valid_year", "year > 0")
@dlt.expect("valid_release","release IS NOT NULL" )

def songs_prepared():
    return (
        spark.read.table("songs_raw")
        .withColumnRenamed("title", "song_title")
        .select(
            "artist_id",
            "artist_name",
            "artist_location",
            "duration",
            "release",
            "tempo",
            "time_signature",
            "song_title",
            "year",
        )
    )

# -----------------------------------------------------------
# Aggregated view: Top artists per year
# -----------------------------------------------------------
@dlt.table(
    comment=(
        "A table summarizing counts of songs released by the artists "
        "who released the most songs each year."
    )
)
def top_artists_by_year():
    return (
        spark.read.table("songs_prepared")
        .filter(expr("year > 0"))
        .groupBy("artist_name", "year")
        .count()
        .withColumnRenamed("count", "total_number_of_songs")
        .sort(desc("total_number_of_songs"), desc("year"))
    )


from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Create additional materialized views using Lakeflow Declarative Pipelines
# 
# 1.  Top artists across the entire catalogue
# ---------------------------------------------------------------------------
@dlt.table(
    name="top_artists_overall",
    comment="All-time count of songs released by each artist via Lakeflow Declarative Pipelines."
)
def top_artists_overall():
    df = dlt.read("songs_prepared")

    result = (df.groupBy("artist_name")
                .agg(F.count("*").alias("total_number_of_songs"))
                .orderBy(F.desc("total_number_of_songs")))

    return result


# ---------------------------------------------------------------------------
# 2.  Year-over-year song-level summary statistics
# ---------------------------------------------------------------------------
@dlt.table(
    name="yearly_song_stats",
    comment="Year-over-year summary statistics for released songs processed by Lakeflow Declarative Pipelines."
)
def yearly_song_stats():
    df = dlt.read("songs_prepared")

    result = (df
        .filter(F.col("year") > 0)                               # drop unknown years
        .groupBy("year")
        .agg(
            F.count("*").alias("song_count"),
            F.avg("duration").alias("avg_duration_seconds"),
            F.max("duration").alias("max_duration_seconds"),
            F.min("duration").alias("min_duration_seconds"),
            F.avg("tempo").alias("avg_tempo_bpm"),
            F.expr("percentile_approx(tempo, 0.5)").alias("median_tempo_bpm")
        )
        .orderBy("year"))

    return result


# ---------------------------------------------------------------------------
# 3.  Location-level song statistics
# ---------------------------------------------------------------------------
@dlt.table(
    name="artist_location_summary",
    comment="Song counts and average attributes by artist location using Lakeflow Declarative Pipelines."
)
def artist_location_summary():
    df = dlt.read("songs_prepared")

    result = (df
        .withColumn("location",
                    F.coalesce(F.col("artist_location"), F.lit("Unknown")))
        .groupBy("location")
        .agg(
            F.count("*").alias("songs_from_location"),
            F.avg("duration").alias("avg_duration_seconds"),
            F.avg("tempo").alias("avg_tempo_bpm")
        )
        .orderBy(F.desc("songs_from_location")))

    return result
