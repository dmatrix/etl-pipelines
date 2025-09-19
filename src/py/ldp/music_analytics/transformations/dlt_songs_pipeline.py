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
# Helper function for data preparation (not a materialized table)
# -----------------------------------------------------------
def get_prepared_songs_data():
    """Helper function to get cleaned and prepared songs data for silver tables."""
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
# Silver Layer - Specialized Tables
# -----------------------------------------------------------

@dlt.table(
    comment="Song metadata and release information - focused on temporal and release data."
)
@dlt.expect("valid_release_year", "year > 1900 AND year <= 2030")
@dlt.expect("valid_song_title_metadata", "song_title IS NOT NULL AND trim(song_title) != ''")
@dlt.expect("valid_artist_name_metadata", "artist_name IS NOT NULL AND trim(artist_name) != ''")
@dlt.expect("valid_release_info", "release IS NOT NULL AND trim(release) != ''")
@dlt.expect("reasonable_duration_metadata", "duration > 10 AND duration < 3600")
def songs_metadata_silver():
    """Silver table focused on song metadata, releases, and temporal information."""
    prepared_data = get_prepared_songs_data()
    return (
        prepared_data
        .select(
            "song_title",
            "artist_name",
            "artist_id",
            "release",
            "year",
            "duration"
        )
        .filter(F.col("year").isNotNull() & (F.col("year") > 0))
    )

@dlt.table(
    comment="Song audio features and musical characteristics - focused on tempo, rhythm and musical analysis."
)
@dlt.expect("valid_tempo_range", "tempo > 40 AND tempo < 250")
@dlt.expect("valid_time_signature_range", "time_signature >= 1 AND time_signature <= 12")
@dlt.expect("valid_song_title_audio", "song_title IS NOT NULL AND trim(song_title) != ''")
@dlt.expect("valid_artist_name_audio", "artist_name IS NOT NULL AND trim(artist_name) != ''")
@dlt.expect("reasonable_duration_audio", "duration > 10 AND duration < 3600")
def songs_audio_features_silver():
    """Silver table focused on audio characteristics and musical features."""
    prepared_data = get_prepared_songs_data()
    return (
        prepared_data
        .select(
            "song_title",
            "artist_name",
            "tempo",
            "time_signature",
            "duration"
        )
        .filter(
            F.col("tempo").isNotNull() &
            F.col("time_signature").isNotNull() &
            (F.col("tempo") > 0) &
            (F.col("time_signature") > 0)
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
        dlt.read("songs_metadata_silver")
        .filter(F.col("year") > 0)
        .groupBy("artist_name", "year")
        .count()
        .withColumnRenamed("count", "total_number_of_songs")
        .sort(F.desc("total_number_of_songs"), F.desc("year"))
    )


from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
    df = dlt.read("songs_metadata_silver")

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
    # Join both silver tables to get complete yearly statistics
    metadata_df = dlt.read("songs_metadata_silver")
    audio_df = dlt.read("songs_audio_features_silver")

    # Join on song and artist to get complete data
    df = metadata_df.join(
        audio_df.select("song_title", "artist_name", "tempo", "time_signature"),
        on=["song_title", "artist_name"],
        how="inner"
    )

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
    # Use the helper function to get location data (since it's not in our silver tables)
    df = get_prepared_songs_data()

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


# ---------------------------------------------------------------------------
# Gold Layer - Advanced Analytics from Silver Tables
# ---------------------------------------------------------------------------

@dlt.table(
    name="release_trends_gold",
    comment="Release trends and temporal analysis from metadata silver layer."
)
def release_trends_gold():
    """Analyze release patterns, temporal trends, and album productivity over time."""
    df = dlt.read("songs_metadata_silver")

    result = (df
        .groupBy("year")
        .agg(
            F.count("*").alias("total_songs_released"),
            F.countDistinct("release").alias("unique_releases"),
            F.countDistinct("artist_name").alias("active_artists"),
            F.avg("duration").alias("avg_song_duration"),
            F.expr("percentile_approx(duration, 0.5)").alias("median_song_duration"),
            F.max("duration").alias("longest_song_duration"),
            F.min("duration").alias("shortest_song_duration")
        )
        .withColumn("songs_per_release", F.col("total_songs_released") / F.col("unique_releases"))
        .orderBy("year"))

    return result

@dlt.table(
    name="artist_discography_gold",
    comment="Artist catalog analysis including career span and productivity metrics from metadata silver."
)
def artist_discography_gold():
    """Comprehensive artist discography analysis with career metrics."""
    df = dlt.read("songs_metadata_silver")

    result = (df
        .groupBy("artist_name")
        .agg(
            F.count("*").alias("total_songs"),
            F.countDistinct("release").alias("album_count"),
            F.min("year").alias("career_start_year"),
            F.max("year").alias("career_end_year"),
            F.avg("duration").alias("avg_song_duration"),
            F.sum("duration").alias("total_catalog_duration_seconds")
        )
        .withColumn("career_span_years", F.col("career_end_year") - F.col("career_start_year") + 1)
        .withColumn("songs_per_album", F.col("total_songs") / F.col("album_count"))
        .withColumn("total_catalog_duration_hours", F.col("total_catalog_duration_seconds") / 3600)
        .filter(F.col("total_songs") >= 3)  # Filter for artists with meaningful catalogs
        .orderBy(F.desc("total_songs")))

    return result

@dlt.table(
    name="musical_characteristics_gold",
    comment="Audio feature distributions and musical analysis from audio features silver layer."
)
def musical_characteristics_gold():
    """Analyze tempo distributions, time signatures, and musical characteristics."""
    df = dlt.read("songs_audio_features_silver")

    # Create tempo categories for analysis
    result = (df
        .withColumn("tempo_category",
                   F.when(F.col("tempo") < 80, "Slow")
                   .when(F.col("tempo") < 120, "Medium")
                   .when(F.col("tempo") < 160, "Fast")
                   .otherwise("Very Fast"))
        .withColumn("duration_category",
                   F.when(F.col("duration") < 180, "Short")
                   .when(F.col("duration") < 300, "Medium")
                   .otherwise("Long"))
        .groupBy("tempo_category", "time_signature", "duration_category")
        .agg(
            F.count("*").alias("song_count"),
            F.avg("tempo").alias("avg_tempo"),
            F.avg("duration").alias("avg_duration"),
            F.expr("percentile_approx(tempo, 0.5)").alias("median_tempo")
        )
        .orderBy("tempo_category", "time_signature", "duration_category"))

    return result

@dlt.table(
    name="tempo_time_signature_analysis_gold",
    comment="Detailed tempo and time signature relationship analysis from audio features silver."
)
def tempo_time_signature_analysis_gold():
    """Deep analysis of tempo and time signature relationships and patterns."""
    df = dlt.read("songs_audio_features_silver")

    result = (df
        .groupBy("time_signature")
        .agg(
            F.count("*").alias("song_count"),
            F.avg("tempo").alias("avg_tempo"),
            F.min("tempo").alias("min_tempo"),
            F.max("tempo").alias("max_tempo"),
            F.expr("percentile_approx(tempo, 0.25)").alias("tempo_q1"),
            F.expr("percentile_approx(tempo, 0.5)").alias("tempo_median"),
            F.expr("percentile_approx(tempo, 0.75)").alias("tempo_q3"),
            F.stddev("tempo").alias("tempo_stddev"),
            F.avg("duration").alias("avg_duration")
        )
        .withColumn("tempo_range", F.col("max_tempo") - F.col("min_tempo"))
        .withColumn("popularity_rank", F.row_number().over(Window.orderBy(F.desc("song_count"))))
        .orderBy("popularity_rank"))

    return result

@dlt.table(
    name="comprehensive_artist_profile_gold",
    comment="Combined artist analysis merging metadata and audio characteristics from both silver tables."
)
def comprehensive_artist_profile_gold():
    """Comprehensive artist profiles combining discography and musical style analysis."""
    metadata_df = dlt.read("songs_metadata_silver").select(
        "artist_name",
        "song_title",
        "year",
        "duration",
        "release"
    )

    audio_df = dlt.read("songs_audio_features_silver").select(
        "artist_name",
        "song_title",
        "tempo",
        "time_signature"
    )

    # Join the silver tables on artist and song
    combined_df = metadata_df.join(
        audio_df,
        on=["artist_name", "song_title"],
        how="inner"
    )

    result = (combined_df
        .groupBy("artist_name")
        .agg(
            F.count("*").alias("total_songs"),
            F.countDistinct("release").alias("album_count"),
            F.min("year").alias("career_start"),
            F.max("year").alias("career_end"),
            F.avg("duration").alias("avg_song_length"),
            F.avg("tempo").alias("avg_tempo"),
            F.expr("percentile_approx(tempo, 0.5)").alias("median_tempo"),
            F.stddev("tempo").alias("tempo_variation"),
            F.countDistinct("time_signature").alias("time_signature_diversity"),
            F.expr("mode(time_signature)").alias("most_common_time_signature"),
            F.sum("duration").alias("total_catalog_duration")
        )
        .withColumn("career_span", F.col("career_end") - F.col("career_start") + 1)
        .withColumn("songs_per_year", F.col("total_songs") / F.col("career_span"))
        .withColumn("musical_consistency",
                   F.when(F.col("tempo_variation") < 15, "Very Consistent")
                   .when(F.col("tempo_variation") < 25, "Consistent")
                   .when(F.col("tempo_variation") < 35, "Moderate")
                   .otherwise("Diverse"))
        .filter(F.col("total_songs") >= 5)  # Focus on artists with substantial catalogs
        .orderBy(F.desc("total_songs")))

    return result
