# 🎮 Dota 2 Game Log Analytics Pipeline

A complete big data pipeline that collects Dota 2 match data from the [OpenDota API](https://docs.opendota.com/) and processes it through a full stack of big data technologies.

## 🏗️ Architecture & Mechanism

This system uses an industry-standard modern Data Engineering architecture that ingests, processes, stores, and visualizes massive amounts of parsed game log streaming data.

```text
               ┌────────────────────────────┐
               │        OpenDota API        │
               │  (Player + Match + Hero)   │
               └────────────┬───────────────┘
                            │
                            ▼
               ┌────────────────────────────┐
               │   Python Data Collector    │
               │  - Fetch API data          │
               │  - Convert → Game Logs     │
               │  - Simulate streaming      │
               └──────┬───────────────────┬─┘
       Speed Layer    │                   │   Batch Layer
     (Real-time)      │                   │   (Analytics)
                      ▼                   ▼
    ┌────────────────────────────┐  ┌────────────────────────────┐
    │      Elasticsearch         │  │        Kafka Topic         │
    │  Indexed analytics data    │  │     "dota2-match-logs"     │
    │  (dota2_player_stats)      │  │  (Real-time ingestion)     │
    └────────────┬───────────────┘  └────────────┬───────────────┘
                 │                               │
                 ▼                               ▼
    ┌────────────────────────────┐  ┌────────────────────────────┐
    │          Kibana            │  │      Spark Processing      │
    │   Dashboard Visualization  │  │  - Read from Kafka         │
    │  - Charts / Graphs         │  │  - Transform & Aggregate   │
    └────────────────────────────┘  └───────┬─────────┬──────────┘
                                            │         │
                              ┌─────────────▼───┐   ┌─▼──────────────────┐
                              │      HDFS       │   │        Hive        │
                              │  Parquet Store  │   │  Structured Tables │
                              │  (Deep Storage) │   │  (SQL queries)     │
                              └─────────────────┘   └────────────────────┘
```

### The 7 Big Data Stacks
This project strictly integrates **7 major big data stack components**:

1. **Apache Kafka + Zookeeper (Streaming/Ingestion)**: Handles the high-throughput, real-time message stream of Dota 2 player logs. It ensures the system can buffer millions of events without dropping data during peak ingestion loads. 
2. **Apache Spark / PySpark (Processing/Analytics)**: Consumes the raw events from the Kafka topic and performs parallel Big Data ETL (Extract, Transform, Load) aggregations over the cluster computing framework to calculate win rates and map metrics.
3. **Hadoop HDFS (Distributed Storage)**: Stores the massive amounts of processed Spark output safely across NameNodes and DataNodes in highly-compressed Parquet column-file format.
4. **Apache Hive (Data Warehouse/Querying)**: Sits on top of the Hadoop file system to provide a SQL interface, allowing data scientists to run structured MapReduce SQL queries across terabytes of flat Parquet files.
5. **Elasticsearch (NoSQL Search Engine)**: Extremely fast document-oriented database that ingests a replica of the player events to allow sub-millisecond metric aggregations (used natively over Hadoop for real-time speed).
6. **Kibana (Data Visualization)**: Connects natively to the Elasticsearch clusters to provide a real-time, zero-latency dynamic dashboard layout for the end-user.
7. **Docker (Cluster Orchestration)**: Containerizes all 9 Big Data microservices into portable, isolated containers operating on an internal custom virtual sub-network.

## 🚀 Quick Start

### Prerequisites
- **Docker** & **Docker Compose** installed
- At least **8 GB RAM** allocated to Docker
- Internet connection (for OpenDota API)

### Start the Pipeline

**Linux/WSL:**
```bash
cd dota2-analytics
chmod +x scripts/start.sh
./scripts/start.sh
```

**Windows PowerShell:**
```powershell
cd dota2-analytics
.\scripts\start.ps1
```

**Manual start:**
```bash
# Start all services
docker compose up -d

# Watch the data collector
docker compose logs -f data-collector

# Watch the Spark processor
docker compose logs -f spark-processor
```

### Access the Dashboards

| Service | URL |
|---|---|
| 📊 **Kibana Dashboard** | [http://localhost:5601](http://localhost:5601) |
| ⚡ **Spark Master UI** | [http://localhost:8080](http://localhost:8080) |
| 📁 **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) |
| 🔍 **Elasticsearch** | [http://localhost:9200](http://localhost:9200) |

## 📊 What Gets Analyzed

### Hero Usage Statistics
- Pick rate across all matches
- Win rate per hero
- Average KDA, GPM, XPM per hero

### Match Trends
- Average match duration
- Radiant vs Dire win rates
- Score distributions

## 🔧 Configuration

Edit `.env` to customize:

```env
# Increase batch size for more data (slower due to API rate limits)
MATCH_BATCH_SIZE=50

# Add an OpenDota API key for higher rate limits
OPENDOTA_API_KEY=your_key_here

# Adjust API delay (seconds between calls)
API_DELAY=1.2
```

## 📂 Project Structure

```
dota2-analytics/
├── docker-compose.yml          # Service orchestration
├── .env                        # Configuration
├── hadoop.env                  # Hadoop settings
├── data-collector/             # Python data collection
│   ├── main.py                 # Pipeline orchestrator
│   ├── collector.py            # OpenDota API client
│   ├── log_transformer.py      # JSON → log event converter
│   ├── kafka_producer.py       # Kafka message producer
│   ├── es_indexer.py           # Elasticsearch indexer
│   └── hero_constants.py       # Hero ID → name mapping
├── spark-processor/            # PySpark analytics
│   └── spark_processor.py      # Spark batch processing job
├── hive/
│   └── init.sql                # Hive table definitions
├── elasticsearch/
│   └── index_mapping.json      # ES index template
├── kibana/
│   └── dashboards.ndjson       # Pre-built Kibana dashboards
└── scripts/
    ├── start.sh                # Linux startup script
    └── start.ps1               # Windows startup script
```

## 🛠️ Useful Commands

```bash
# View logs
docker compose logs -f data-collector
docker compose logs -f spark-processor
docker compose logs -f

# Check Kafka messages
docker exec kafka kafka-console-consumer \
    --topic dota2-match-logs \
    --bootstrap-server localhost:9092 \
    --from-beginning --max-messages 5

# Check Elasticsearch data
curl http://localhost:9200/dota2_player_stats/_count
curl http://localhost:9200/dota2_hero_stats/_search?pretty

# Check HDFS files
docker exec namenode hdfs dfs -ls -R /dota2/

# Query Hive
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 \
    -e "SELECT hero_name, total_picks, win_rate FROM dota2_hero_usage ORDER BY total_picks DESC LIMIT 10;"

# Stop everything
docker compose down

# Stop and remove all data
docker compose down -v
```

## 🐛 Troubleshooting

| Issue | Solution |
|---|---|
| Services won't start | Ensure Docker has at least 8GB RAM |
| API rate limiting | Add an API key in `.env` or increase `API_DELAY` |
| Kafka connection errors | Wait 30-60s after starting for Kafka to initialize |
| No data in Kibana | Check data collector logs, ensure time range is set correctly |
| Spark job fails | Check `docker compose logs spark-processor` for errors |
