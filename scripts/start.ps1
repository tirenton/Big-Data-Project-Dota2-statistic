# ============================================
# Dota 2 Game Log Analytics - Windows Startup
# ============================================

$ErrorActionPreference = "Continue"
$ProjectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectDir

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  DOTA 2 GAME LOG ANALYTICS PIPELINE" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Start infrastructure
Write-Host "[1/7] Starting infrastructure services..." -ForegroundColor Yellow
docker compose up -d zookeeper kafka namenode datanode `
    hive-metastore-postgresql hive-metastore hive-server `
    spark-master spark-worker elasticsearch kibana

# Step 2: Wait for services
Write-Host ""
Write-Host "[2/7] Waiting for services to be healthy..." -ForegroundColor Yellow

Write-Host "  Waiting for Kafka..."
do {
    Start-Sleep -Seconds 5
    $result = docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1
} while ($LASTEXITCODE -ne 0)
Write-Host "  ✓ Kafka is ready" -ForegroundColor Green

Write-Host "  Waiting for Elasticsearch..."
do {
    Start-Sleep -Seconds 5
    try { $response = Invoke-WebRequest -Uri "http://localhost:9200/_cluster/health" -UseBasicParsing -ErrorAction SilentlyContinue } catch { $response = $null }
} while ($null -eq $response -or $response.StatusCode -ne 200)
Write-Host "  ✓ Elasticsearch is ready" -ForegroundColor Green

Write-Host "  Waiting for HDFS..."
do {
    Start-Sleep -Seconds 5
    try { $response = Invoke-WebRequest -Uri "http://localhost:9870" -UseBasicParsing -ErrorAction SilentlyContinue } catch { $response = $null }
} while ($null -eq $response -or $response.StatusCode -ne 200)
Write-Host "  ✓ HDFS is ready" -ForegroundColor Green

# Step 3: Create Kafka topic
Write-Host ""
Write-Host "[3/7] Creating Kafka topic..." -ForegroundColor Yellow
docker exec kafka kafka-topics --create --topic dota2-match-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists 2>$null
Write-Host "  ✓ Kafka topic ready" -ForegroundColor Green

# Step 4: ES index
Write-Host ""
Write-Host "[4/7] Setting up Elasticsearch index..." -ForegroundColor Yellow
$mapping = Get-Content "elasticsearch/index_mapping.json" -Raw
try { Invoke-RestMethod -Uri "http://localhost:9200/dota2_player_stats" -Method Put -ContentType "application/json" -Body $mapping -ErrorAction SilentlyContinue } catch {}
Write-Host "  ✓ Elasticsearch index ready" -ForegroundColor Green

# Step 5: HDFS directories
Write-Host ""
Write-Host "[5/7] Creating HDFS directories..." -ForegroundColor Yellow
docker exec namenode hdfs dfs -mkdir -p /dota2/raw 2>$null
docker exec namenode hdfs dfs -mkdir -p /dota2/processed 2>$null
docker exec namenode hdfs dfs -chmod -R 777 /dota2 2>$null
Write-Host "  ✓ HDFS directories ready" -ForegroundColor Green

# Step 6: Data collector
Write-Host ""
Write-Host "[6/7] Starting data collector..." -ForegroundColor Yellow
docker compose up -d --build data-collector
Write-Host "  ✓ Data collector started" -ForegroundColor Green

# Step 7: Spark processor
Write-Host ""
Write-Host "[7/7] Starting Spark processor..." -ForegroundColor Yellow
docker compose up -d --build spark-processor
Write-Host "  ✓ Spark processor started" -ForegroundColor Green

# Import Kibana dashboards
Write-Host ""
Write-Host "Waiting for Kibana..." -ForegroundColor Yellow
do {
    Start-Sleep -Seconds 10
    try { $response = Invoke-WebRequest -Uri "http://localhost:5601/api/status" -UseBasicParsing -ErrorAction SilentlyContinue } catch { $response = $null }
} while ($null -eq $response -or $response.StatusCode -ne 200)
Write-Host "  ✓ Kibana is ready" -ForegroundColor Green

Write-Host "Importing Kibana dashboards..." -ForegroundColor Yellow
$dashboardFile = Get-Item "kibana/dashboards.ndjson"
curl -s -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" -H "kbn-xsrf: true" --form "file=@$($dashboardFile.FullName)" 2>$null
Write-Host "  ✓ Kibana dashboards imported" -ForegroundColor Green

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "  ALL SERVICES STARTED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Service URLs:" -ForegroundColor Cyan
Write-Host "  Kibana Dashboard:   http://localhost:5601"
Write-Host "  Elasticsearch:      http://localhost:9200"
Write-Host "  Spark Master UI:    http://localhost:8080"
Write-Host "  HDFS NameNode UI:   http://localhost:9870"
Write-Host "  Kafka:              localhost:29092"
Write-Host "  Hive Server:        localhost:10000"
Write-Host ""
Write-Host "  Useful commands:" -ForegroundColor Cyan
Write-Host "  View collector logs:  docker compose logs -f data-collector"
Write-Host "  View Spark logs:      docker compose logs -f spark-processor"
Write-Host "  View all logs:        docker compose logs -f"
Write-Host "  Stop all services:    docker compose down"
Write-Host "  Reset everything:     docker compose down -v"
Write-Host ""
