# Databricks Python file for Spark Declarative Pipelines on Databricks
# sdp_musical_pipeline.py
# -----------------------------------------------------------
# Spark Declarative Pipelines implementation for the Million Song Dataset
# Demonstrates SDP patterns with streaming data ingestion on Databricks
#
# Key Benefits of Spark Declarative Pipelines:
# 1. Simplified Development & Maintenance: SDP uses declarative @dp.table decorators that
#    automatically handle complex orchestration, dependency management, and error recovery,
#    reducing code complexity compared to traditional ETL frameworks.
#
# 2. Built-in Data Quality & Governance: Native @dp.expect decorators provide comprehensive
#    data validation with automatic quarantine of bad records, detailed quality metrics,
#    and lineage tracking without additional infrastructure setup.
#
# 3. Auto-scaling & Cost Optimization: SDP automatically optimizes cluster sizing, manages
#    incremental processing, orchestration, and execution order, helping reduce costs by
#    only sizing resources it needs for successful and efficient execution.
# -----------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructType,
    StructField,
)
from pyspark.sql.window import Window


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
# Raw ingestion table - Bronze layer
# -----------------------------------------------------------
@dp.table(
        name="songs_raw_bronze",
        comment="Raw data from a subset of the Million Song Dataset; a collection "
        "of features and metadata for contemporary music tracks. Processed via Spark Declarative Pipelines."
    )
def songs_raw_bronze():
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
        spark.read.table("songs_raw_bronze")
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
# 1. Silver Layer - Specialized Tables
# -----------------------------------------------------------

@dp.table(
    name="songs_metadata_silver",
    comment="Song metadata and release information - focused on temporal and release data."
)
@dp.expect("valid_release_year", "year > 1900 AND year <= 2030")
@dp.expect("valid_song_title_metadata", "song_title IS NOT NULL AND trim(song_title) != ''")
@dp.expect("valid_artist_name_metadata", "artist_name IS NOT NULL AND trim(artist_name) != ''")
@dp.expect("valid_release_info", "release IS NOT NULL AND trim(release) != ''")
@dp.expect("reasonable_duration_metadata", "duration > 10 AND duration < 3600")
@dp.expect("valid_artist_location", "artist_location IS NULL OR trim(artist_location) != ''")
def songs_metadata_silver():
    """Silver table focused on song metadata, releases, and temporal information."""
    prepared_data = get_prepared_songs_data()
    return (
        prepared_data
        .select(
            "song_title",
            "artist_name",
            "artist_id",
            "artist_location",
            "release",
            "year",
            "duration"
        )
        .filter(F.col("year").isNotNull() & (F.col("year") > 0))
    )

# -----------------------------------------------------------
# 2. Silver Layer - Specialized Tables
# -----------------------------------------------------------
@dp.table(
    name="songs_audio_features_silver",
    comment="Song audio features and musical characteristics - focused on tempo, rhythm and musical analysis."
)
@dp.expect("valid_tempo_range", "tempo > 40 AND tempo < 250")
@dp.expect("valid_time_signature_range", "time_signature >= 1 AND time_signature <= 12")
@dp.expect("valid_song_title_audio", "song_title IS NOT NULL AND trim(song_title) != ''")
@dp.expect("valid_artist_name_audio", "artist_name IS NOT NULL AND trim(artist_name) != ''")
@dp.expect("reasonable_duration_audio", "duration > 10 AND duration < 3600")
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
# 1. Aggregated Gold layer view: Top artists per year
# -----------------------------------------------------------
@dp.table(  
        name="top_artists_by_year_gold",
        comment="A table summarizing counts of songs released by the artists "
        "who released the most songs each year."
    )

def top_artists_by_year_gold():
    return (
        dp.read("songs_metadata_silver")
        .filter(F.col("year") > 0)
        .groupBy("artist_name", "year")
        .count()
        .withColumnRenamed("count", "total_number_of_songs")
        .sort(F.desc("total_number_of_songs"), F.desc("year")

    )
    

# ---------------------------------------------------------------------------
# Create additional materialized views using Spark Declarative Pipelines
#
# 2. Aggregated Gold layer view: Top artists across the entire catalogue
# ---------------------------------------------------------------------------
@dp.table(
    name="top_artists_overall_gold",
    comment="All-time count of songs released by each artist via Spark Declarative Pipelines."
)
def top_artists_overall_gold():
    df = dp.read("songs_metadata_silver")

    result = (df.groupBy("artist_name")
                .agg(F.count("*").alias("total_number_of_songs"))
                .orderBy(F.desc("total_number_of_songs")))

    return result


# ---------------------------------------------------------------------------
# 3. Aggregated Gold layer view: Year-over-year song-level summary statistics
# ---------------------------------------------------------------------------
@dp.table(
    name="yearly_song_stats_gold",
    comment="Year-over-year summary statistics for released songs processed by Spark Declarative Pipelines."
)
def yearly_song_stats_gold():
    # Join both silver tables to get complete yearly statistics
    metadata_df = dp.read("songs_metadata_silver")
    audio_df = dp.read("songs_audio_features_silver")

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
# 4. Aggregated Gold layer view: Location-level song statistics
# ---------------------------------------------------------------------------
@dp.table(
    name="artist_location_summary_gold",
    comment="Song counts and average attributes by artist location using Spark Declarative Pipelines."
)
@dp.expect("valid_location_summary", "location IS NOT NULL AND trim(location) != ''")
@dp.expect("positive_song_count", "songs_from_location > 0")
@dp.expect("positive_artist_count", "unique_artists_from_location > 0")
@dp.expect("reasonable_duration_location", "avg_duration_seconds > 0 AND avg_duration_seconds < 3600")
def artist_location_summary_gold():
    """Analyze geographic distribution of musical output using properly validated silver layer data."""
    df = dp.read("songs_metadata_silver")

    result = (df
        .withColumn("location",
                    F.coalesce(F.col("artist_location"), F.lit("Unknown")))
        .groupBy("location")
        .agg(
            F.count("*").alias("songs_from_location"),
            F.avg("duration").alias("avg_duration_seconds"),
            F.countDistinct("artist_name").alias("unique_artists_from_location")
        )
        .orderBy(F.desc("songs_from_location")))

    return result


# ---------------------------------------------------------------------------
# 5. Aggregated Gold layer view: Release trends and temporal analysis
# ---------------------------------------------------------------------------

@dp.table(
    name="release_trends_gold",
    comment="Release trends and temporal analysis from metadata silver layer."
)
def release_trends_gold():
    """Analyze release patterns, temporal trends, and album productivity over time."""
    df = dp.read("songs_metadata_silver")

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

# ---------------------------------------------------------------------------
# 6. Aggregated Gold layer view: Artist discography and career metrics
# ---------------------------------------------------------------------------
@dp.table(
    name="artist_discography_gold",
    comment="Artist catalog analysis including career span and productivity metrics from metadata silver."
)
def artist_discography_gold():
    """Comprehensive artist discography analysis with career metrics."""
    df = dp.read("songs_metadata_silver")

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

# ---------------------------------------------------------------------------
# 7. Aggregated Gold layer view: Musical characteristics and analysis
# ---------------------------------------------------------------------------
@dp.table(
    name="musical_characteristics_gold",
    comment="Audio feature distributions and musical analysis from audio features silver layer."
)
def musical_characteristics_gold():
    """Analyze tempo distributions, time signatures, and musical characteristics."""
    df = dp.read("songs_audio_features_silver")

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

# ---------------------------------------------------------------------------
# 8. Aggregated Gold layer view: Tempo and time signature relationship analysis
# ---------------------------------------------------------------------------
@dp.table(
    name="tempo_time_signature_analysis_gold",
    comment="Detailed tempo and time signature relationship analysis from audio features silver."
)
def tempo_time_signature_analysis_gold():
    """Deep analysis of tempo and time signature relationships and patterns."""
    df = dp.read("songs_audio_features_silver")

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

# ---------------------------------------------------------------------------
# 9. Aggregated Gold layer view: Comprehensive artist profile
# ---------------------------------------------------------------------------
@dp.table(
    name="comprehensive_artist_profile_gold",
    comment="Combined artist analysis merging metadata and audio characteristics from both silver tables."
)
def comprehensive_artist_profile_gold():
    """Comprehensive artist profiles combining discography and musical style analysis."""
    metadata_df = dp.read("songs_metadata_silver").select(
        "artist_name",
        "song_title",
        "year",
        "duration",
        "release"
    )

    audio_df = dp.read("songs_audio_features_silver").select(
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
