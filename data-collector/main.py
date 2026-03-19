"""
Main entry point for the Dota 2 Data Collector.
Orchestrates: API fetch → Log transform → Kafka produce → ES index
"""

import os
import sys
import json
import logging
import time
from datetime import datetime, timezone
from collections import defaultdict

from collector import DotaCollector
from log_transformer import transform_match_to_logs, log_event_to_string
from kafka_producer import DotaKafkaProducer
from es_indexer import DotaElasticsearchIndexer
from hero_constants import fetch_hero_map, get_hero_name

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("dota2-collector")


def compute_hero_aggregates(all_log_events):
    """Compute aggregated hero statistics from all log events."""
    hero_data = defaultdict(lambda: {
        "picks": 0, "wins": 0,
        "kills": [], "deaths": [], "assists": [], "kda": [],
        "gpm": [], "xpm": [], "hero_damage": [], "duration": []
    })
    
    for event in all_log_events:
        hero_id = event["hero_id"]
        hero_name = event["hero_name"]
        data = hero_data[(hero_id, hero_name)]
        
        data["picks"] += 1
        if event["result"] == "WIN":
            data["wins"] += 1
        
        data["kills"].append(event["kills"])
        data["deaths"].append(event["deaths"])
        data["assists"].append(event["assists"])
        data["kda"].append(event["kda"])
        data["gpm"].append(event["gpm"])
        data["xpm"].append(event["xpm"])
        data["hero_damage"].append(event["hero_damage"])
        data["duration"].append(event["duration"])
    
    hero_stats = []
    for (hero_id, hero_name), data in hero_data.items():
        picks = data["picks"]
        stats = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "hero_id": hero_id,
            "hero_name": hero_name,
            "total_picks": picks,
            "total_wins": data["wins"],
            "win_rate": round(data["wins"] / max(picks, 1) * 100, 2),
            "avg_kills": round(sum(data["kills"]) / max(picks, 1), 2),
            "avg_deaths": round(sum(data["deaths"]) / max(picks, 1), 2),
            "avg_assists": round(sum(data["assists"]) / max(picks, 1), 2),
            "avg_kda": round(sum(data["kda"]) / max(picks, 1), 2),
            "avg_gpm": round(sum(data["gpm"]) / max(picks, 1), 2),
            "avg_xpm": round(sum(data["xpm"]) / max(picks, 1), 2),
            "avg_hero_damage": round(sum(data["hero_damage"]) / max(picks, 1), 2),
            "avg_duration": round(sum(data["duration"]) / max(picks, 1), 2)
        }
        hero_stats.append(stats)
    
    # Sort by pick count
    hero_stats.sort(key=lambda x: x["total_picks"], reverse=True)
    return hero_stats


def main():
    """Main pipeline execution."""
    logger.info("=" * 60)
    logger.info("  DOTA 2 GAME LOG ANALYTICS - DATA COLLECTOR")
    logger.info("=" * 60)
    
    # Configuration from environment
    api_key = os.getenv("OPENDOTA_API_KEY", "")
    batch_size = int(os.getenv("MATCH_BATCH_SIZE", "50"))
    api_delay = float(os.getenv("API_DELAY", "1.2"))
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "dota2-match-logs")
    es_host = os.getenv("ES_HOST", "elasticsearch")
    es_port = int(os.getenv("ES_PORT", "9200"))
    
    logger.info(f"Config: batch_size={batch_size}, api_delay={api_delay}s")
    logger.info(f"Kafka: {kafka_broker} / topic: {kafka_topic}")
    logger.info(f"Elasticsearch: {es_host}:{es_port}")
    
    # Step 1: Fetch hero mapping
    logger.info("\n--- Step 1: Fetching Hero Mapping ---")
    fetch_hero_map(api_key if api_key else None)
    
    # Step 2: Initialize Kafka producer
    logger.info("\n--- Step 2: Connecting to Kafka ---")
    kafka = DotaKafkaProducer(broker=kafka_broker, topic=kafka_topic)
    
    # Step 3: Initialize Elasticsearch
    logger.info("\n--- Step 3: Setting up Elasticsearch ---")
    es = DotaElasticsearchIndexer(host=es_host, port=es_port)
    es.setup_indices()
    
    # Step 4: Collect match data from OpenDota
    logger.info("\n--- Step 4: Collecting Match Data from OpenDota ---")
    collector = DotaCollector(
        api_key=api_key if api_key else None,
        batch_size=batch_size,
        api_delay=api_delay
    )
    
    all_log_events = []
    total_matches = 0
    total_events = 0
    
    for match_data in collector.collect_matches():
        total_matches += 1
        
        # Save raw data
        collector.save_raw_data(match_data)
        
        # Transform to log events
        log_events = transform_match_to_logs(match_data)
        
        if not log_events:
            continue
        
        # Print log lines
        for event in log_events:
            logger.info(f"  LOG: {log_event_to_string(event)}")
        
        # Send to Kafka
        sent, failed = kafka.send_batch(log_events)
        total_events += sent
        
        # Index to Elasticsearch (player stats)
        es.bulk_index("dota2_player_stats", log_events)
        
        # Track all events for aggregation
        all_log_events.extend(log_events)
        
        logger.info(
            f"Progress: {total_matches} matches, {total_events} events sent to Kafka"
        )
    
    # Step 5: Compute and index hero aggregates
    logger.info("\n--- Step 5: Computing Hero Aggregates ---")
    hero_stats = compute_hero_aggregates(all_log_events)
    
    if hero_stats:
        es.bulk_index("dota2_hero_stats", hero_stats)
        
        logger.info("\n--- TOP 10 HEROES BY PICK RATE ---")
        logger.info(f"{'Hero':<25} {'Picks':>6} {'Wins':>6} {'Win%':>7} {'Avg K/D/A':>15} {'Avg KDA':>8}")
        logger.info("-" * 75)
        for hs in hero_stats[:10]:
            logger.info(
                f"{hs['hero_name']:<25} {hs['total_picks']:>6} {hs['total_wins']:>6} "
                f"{hs['win_rate']:>6.1f}% "
                f"{hs['avg_kills']:>4.1f}/{hs['avg_deaths']:>4.1f}/{hs['avg_assists']:>4.1f} "
                f"{hs['avg_kda']:>7.2f}"
            )
    
    # Save hero stats to file
    os.makedirs("/app/data/processed", exist_ok=True)
    with open("/app/data/processed/hero_stats.json", "w") as f:
        json.dump(hero_stats, f, indent=2)
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("  COLLECTION COMPLETE")
    logger.info(f"  Total matches collected: {total_matches}")
    logger.info(f"  Total events sent to Kafka: {total_events}")
    logger.info(f"  Unique heroes seen: {len(hero_stats)}")
    logger.info(f"  ES player_stats docs: {es.get_doc_count('dota2_player_stats')}")
    logger.info(f"  ES hero_stats docs: {es.get_doc_count('dota2_hero_stats')}")
    logger.info("=" * 60)
    
    # Cleanup
    kafka.close()
    
    logger.info("Data collector finished. Keeping container alive for inspection...")
    # Keep container alive briefly for Spark processor to pick up Kafka data
    time.sleep(30)


if __name__ == "__main__":
    main()
