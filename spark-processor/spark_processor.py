"""
Spark Processor for Dota 2 Game Log Analytics.

Reads match events from Kafka, performs batch analytics with Spark,
computes metrics (hero stats, player performance, match trends),
and writes results to HDFS (Parquet) and Elasticsearch.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, avg, count, sum as spark_sum,
    when, round as spark_round, lit, max as spark_max,
    min as spark_min, desc, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, FloatType, BooleanType, TimestampType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("spark-processor")


# Schema for Kafka JSON messages
MATCH_EVENT_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("match_id", LongType(), True),
    StructField("duration", IntegerType(), True),
    StructField("game_mode", IntegerType(), True),
    StructField("radiant_score", IntegerType(), True),
    StructField("dire_score", IntegerType(), True),
    StructField("radiant_win", BooleanType(), True),
    StructField("account_id", LongType(), True),
    StructField("player_name", StringType(), True),
    StructField("player_slot", IntegerType(), True),
    StructField("team", StringType(), True),
    StructField("result", StringType(), True),
    StructField("hero_id", IntegerType(), True),
    StructField("hero_name", StringType(), True),
    StructField("kills", IntegerType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("kda", FloatType(), True),
    StructField("gpm", IntegerType(), True),
    StructField("xpm", IntegerType(), True),
    StructField("hero_damage", IntegerType(), True),
    StructField("hero_healing", IntegerType(), True),
    StructField("tower_damage", IntegerType(), True),
    StructField("last_hits", IntegerType(), True),
    StructField("denies", IntegerType(), True),
    StructField("level", IntegerType(), True),
])


def create_spark_session():
    """Create and configure Spark session."""
    spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    es_host = os.getenv("ES_HOST", "elasticsearch")
    es_port = os.getenv("ES_PORT", "9200")
    
    spark = SparkSession.builder \
        .appName("Dota2 Game Log Analytics") \
        .master(spark_master) \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
        .config("spark.es.nodes", es_host) \
        .config("spark.es.port", es_port) \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.es.index.auto.create", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created. Master: {spark_master}")
    return spark


def read_from_kafka(spark):
    """Read match events from Kafka topic (batch mode)."""
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "dota2-match-logs")
    
    logger.info(f"Reading from Kafka: {kafka_broker}, topic: {kafka_topic}")
    
    # Wait for data to be available
    max_wait = 120  # seconds
    waited = 0
    while waited < max_wait:
        try:
            df = spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_broker) \
                .option("subscribe", kafka_topic) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            count = df.count()
            if count > 0:
                logger.info(f"Found {count} messages in Kafka topic")
                break
            else:
                logger.info(f"No data yet, waiting... ({waited}s/{max_wait}s)")
                time.sleep(10)
                waited += 10
        except Exception as e:
            logger.warning(f"Kafka not ready: {e}. Retrying in 10s...")
            time.sleep(10)
            waited += 10
    
    # Parse JSON from Kafka values
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), MATCH_EVENT_SCHEMA).alias("data")) \
        .select("data.*")
    
    logger.info(f"Parsed {parsed_df.count()} match events from Kafka")
    return parsed_df


def compute_hero_stats(df):
    """Compute per-hero aggregated statistics."""
    logger.info("Computing hero statistics...")
    
    hero_stats = df.groupBy("hero_id", "hero_name").agg(
        count("*").alias("total_picks"),
        spark_sum(when(col("result") == "WIN", 1).otherwise(0)).alias("total_wins"),
        spark_round(avg("kills"), 2).alias("avg_kills"),
        spark_round(avg("deaths"), 2).alias("avg_deaths"),
        spark_round(avg("assists"), 2).alias("avg_assists"),
        spark_round(avg("kda"), 2).alias("avg_kda"),
        spark_round(avg("gpm"), 2).alias("avg_gpm"),
        spark_round(avg("xpm"), 2).alias("avg_xpm"),
        spark_round(avg("hero_damage"), 2).alias("avg_hero_damage"),
        spark_round(avg("tower_damage"), 2).alias("avg_tower_damage"),
        spark_round(avg("last_hits"), 2).alias("avg_last_hits"),
        spark_round(avg("duration"), 2).alias("avg_duration"),
    ).withColumn(
        "win_rate", spark_round(col("total_wins") / col("total_picks") * 100, 2)
    ).withColumn(
        "timestamp", lit(datetime.now(timezone.utc).isoformat())
    ).orderBy(desc("total_picks"))
    
    logger.info(f"Computed stats for {hero_stats.count()} heroes")
    hero_stats.show(20, truncate=False)
    return hero_stats


def compute_player_performance(df):
    """Compute top player performances."""
    logger.info("Computing player performance metrics...")
    
    player_stats = df.groupBy("account_id", "player_name").agg(
        count("*").alias("games_played"),
        spark_sum(when(col("result") == "WIN", 1).otherwise(0)).alias("wins"),
        spark_round(avg("kills"), 2).alias("avg_kills"),
        spark_round(avg("deaths"), 2).alias("avg_deaths"),
        spark_round(avg("assists"), 2).alias("avg_assists"),
        spark_round(avg("kda"), 2).alias("avg_kda"),
        spark_round(avg("gpm"), 2).alias("avg_gpm"),
        spark_round(avg("xpm"), 2).alias("avg_xpm"),
        spark_sum("kills").alias("total_kills"),
        spark_sum("deaths").alias("total_deaths"),
        spark_sum("assists").alias("total_assists"),
    ).withColumn(
        "win_rate", spark_round(col("wins") / col("games_played") * 100, 2)
    ).orderBy(desc("avg_kda"))
    
    logger.info(f"Computed stats for {player_stats.count()} players")
    player_stats.show(20, truncate=False)
    return player_stats


def compute_match_trends(df):
    """Compute match-level trends."""
    logger.info("Computing match trends...")
    
    # Overall match stats
    match_stats = df.select("match_id", "duration", "radiant_win", "radiant_score", "dire_score") \
        .dropDuplicates(["match_id"]) \
        .agg(
            count("*").alias("total_matches"),
            spark_round(avg("duration"), 0).alias("avg_duration_seconds"),
            spark_sum(when(col("radiant_win") == True, 1).otherwise(0)).alias("radiant_wins"),
            spark_sum(when(col("radiant_win") == False, 1).otherwise(0)).alias("dire_wins"),
            spark_round(avg("radiant_score"), 2).alias("avg_radiant_score"),
            spark_round(avg("dire_score"), 2).alias("avg_dire_score"),
            spark_max("duration").alias("longest_match"),
            spark_min("duration").alias("shortest_match"),
        )
    
    logger.info("Match trends:")
    match_stats.show(truncate=False)
    return match_stats


def write_to_hdfs(df, path, name="data"):
    """Write DataFrame to HDFS in Parquet format."""
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
    full_path = f"{hdfs_namenode}/dota2/processed/{path}"
    
    try:
        df.write \
            .mode("overwrite") \
            .parquet(full_path)
        logger.info(f"Written {name} to HDFS: {full_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write to HDFS: {e}")
        return False


def write_to_elasticsearch(df, index_name):
    """Write DataFrame to Elasticsearch."""
    try:
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", index_name) \
            .option("es.write.operation", "upsert") \
            .option("es.mapping.id", "hero_id" if "hero_id" in df.columns else "account_id") \
            .mode("overwrite") \
            .save()
        logger.info(f"Written to Elasticsearch index: {index_name}")
        return True
    except Exception as e:
        logger.warning(f"ES Spark write failed (data already indexed by collector): {e}")
        return False


def main():
    """Main Spark processing pipeline."""
    logger.info("=" * 60)
    logger.info("  DOTA 2 GAME LOG ANALYTICS - SPARK PROCESSOR")
    logger.info("=" * 60)
    
    # Wait for data collector to produce some data
    initial_wait = int(os.getenv("SPARK_INITIAL_WAIT", "60"))
    logger.info(f"Waiting {initial_wait}s for data collector to produce data...")
    time.sleep(initial_wait)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read data from Kafka
        events_df = read_from_kafka(spark)
        
        if events_df.count() == 0:
            logger.warning("No data found in Kafka. Exiting.")
            return
        
        # Cache the dataframe for multiple computations
        events_df.cache()
        
        logger.info(f"\nTotal events to process: {events_df.count()}")
        logger.info("\nSample data:")
        events_df.show(5, truncate=False)
        
        # Compute analytics
        logger.info("\n--- HERO STATISTICS ---")
        hero_stats = compute_hero_stats(events_df)
        
        logger.info("\n--- PLAYER PERFORMANCE ---")
        player_perf = compute_player_performance(events_df)
        
        logger.info("\n--- MATCH TRENDS ---")
        match_trends = compute_match_trends(events_df)
        
        # Write to HDFS
        logger.info("\n--- Writing to HDFS ---")
        write_to_hdfs(events_df, "match_events", "match events")
        write_to_hdfs(hero_stats, "hero_stats", "hero stats")
        write_to_hdfs(player_perf, "player_performance", "player performance")
        
        # Write to Elasticsearch (via Spark connector)
        logger.info("\n--- Writing to Elasticsearch ---")
        write_to_elasticsearch(hero_stats, "dota2_hero_stats_spark")
        
        # Print final summary
        logger.info("\n" + "=" * 60)
        logger.info("  SPARK PROCESSING COMPLETE")
        logger.info(f"  Total events processed: {events_df.count()}")
        logger.info(f"  Unique heroes: {hero_stats.count()}")
        logger.info(f"  Unique players: {player_perf.count()}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Spark processing failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
