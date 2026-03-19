#!/bin/bash
# ============================================
# Dota 2 Game Log Analytics - Startup Script
# ============================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "============================================"
echo "  DOTA 2 GAME LOG ANALYTICS PIPELINE"
echo "============================================"
echo ""

# Step 1: Start infrastructure services
echo "[1/7] Starting infrastructure services (Kafka, HDFS, Hive, ES, Spark)..."
docker compose up -d zookeeper kafka namenode datanode \
    hive-metastore-postgresql hive-metastore hive-server \
    spark-master spark-worker elasticsearch kibana

echo ""
echo "[2/7] Waiting for services to be healthy..."
echo "  Waiting for Kafka..."
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    sleep 5
    echo "    Kafka not ready yet..."
done
echo "  ✓ Kafka is ready"

echo "  Waiting for Elasticsearch..."
until curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; do
    sleep 5
    echo "    Elasticsearch not ready yet..."
done
echo "  ✓ Elasticsearch is ready"

echo "  Waiting for HDFS NameNode..."
until curl -s http://localhost:9870 > /dev/null 2>&1; do
    sleep 5
    echo "    HDFS not ready yet..."
done
echo "  ✓ HDFS is ready"

# Step 3: Create Kafka topic
echo ""
echo "[3/7] Creating Kafka topic..."
docker exec kafka kafka-topics --create \
    --topic dota2-match-logs \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || true
echo "  ✓ Kafka topic ready"

# Step 4: Create ES index mapping
echo ""
echo "[4/7] Setting up Elasticsearch index..."
curl -s -X PUT "http://localhost:9200/dota2_player_stats" \
    -H "Content-Type: application/json" \
    -d @elasticsearch/index_mapping.json > /dev/null 2>&1 || true
echo "  ✓ Elasticsearch index ready"

# Step 5: Create HDFS directories
echo ""
echo "[5/7] Creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /dota2/raw 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /dota2/processed 2>/dev/null || true
docker exec namenode hdfs dfs -chmod -R 777 /dota2 2>/dev/null || true
echo "  ✓ HDFS directories ready"

# Step 6: Start data collector
echo ""
echo "[6/7] Starting data collector (fetching from OpenDota API)..."
docker compose up -d --build data-collector
echo "  ✓ Data collector started"
echo "  → Follow logs: docker compose logs -f data-collector"

# Step 7: Start Spark processor (after a delay)
echo ""
echo "[7/7] Starting Spark processor (will wait for data)..."
docker compose up -d --build spark-processor
echo "  ✓ Spark processor started"
echo "  → Follow logs: docker compose logs -f spark-processor"

# Step 8: Import Kibana dashboards
echo ""
echo "Waiting for Kibana to be ready..."
until curl -s http://localhost:5601/api/status | grep -q '"level":"available"' 2>/dev/null; do
    sleep 10
    echo "  Kibana not ready yet..."
done
echo "  ✓ Kibana is ready"

echo "Importing Kibana dashboards..."
curl -s -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
    -H "kbn-xsrf: true" \
    --form file=@kibana/dashboards.ndjson > /dev/null 2>&1 || true
echo "  ✓ Kibana dashboards imported"

# Done!
echo ""
echo "============================================"
echo "  ALL SERVICES STARTED SUCCESSFULLY!"
echo "============================================"
echo ""
echo "  Service URLs:"
echo "  ─────────────────────────────────────"
echo "  Kibana Dashboard:   http://localhost:5601"
echo "  Elasticsearch:      http://localhost:9200"
echo "  Spark Master UI:    http://localhost:8080"
echo "  HDFS NameNode UI:   http://localhost:9870"
echo "  Kafka:              localhost:29092"
echo "  Hive Server:        localhost:10000"
echo ""
echo "  Useful commands:"
echo "  ─────────────────────────────────────"
echo "  View collector logs:  docker compose logs -f data-collector"
echo "  View Spark logs:      docker compose logs -f spark-processor"
echo "  View all logs:        docker compose logs -f"
echo "  Stop all services:    docker compose down"
echo "  Reset everything:     docker compose down -v"
echo ""
