"""Fix unknown hero and update Kibana dashboard."""
import requests
import json

ES_URL = "http://elasticsearch:9200"
KIBANA_URL = "http://kibana:5601"
HEADERS = {"kbn-xsrf": "true"}

# 1. Delete Unknown Hero entries from ES
print("=== Removing Unknown Hero (hero_id=0) ===")
r = requests.post(
    f"{ES_URL}/dota2_player_stats/_delete_by_query",
    json={"query": {"term": {"hero_id": 0}}}
)
result = r.json()
print(f"  player_stats: deleted {result.get('deleted', 0)} docs")

r = requests.post(
    f"{ES_URL}/dota2_hero_stats/_delete_by_query",
    json={"query": {"term": {"hero_id": 0}}}
)
result = r.json()
print(f"  hero_stats: deleted {result.get('deleted', 0)} docs")

# Refresh indices
requests.post(f"{ES_URL}/dota2_player_stats/_refresh")
requests.post(f"{ES_URL}/dota2_hero_stats/_refresh")

# Print counts
r1 = requests.get(f"{ES_URL}/dota2_player_stats/_count").json()
r2 = requests.get(f"{ES_URL}/dota2_hero_stats/_count").json()
print(f"  Remaining: player_stats={r1['count']}, hero_stats={r2['count']}")

# 2. Delete all existing dashboard objects
print("\n=== Cleaning Kibana saved objects ===")
for obj_type, obj_id in [
    ("dashboard", "dota2-analytics-dashboard"),
    ("visualization", "hero-pick-rate-viz"),
    ("visualization", "hero-win-rate-viz"),
    ("visualization", "kda-distribution-viz"),
    ("visualization", "team-winloss-viz"),
    ("visualization", "top-players-viz"),
    ("visualization", "gpm-xpm-viz"),
    ("visualization", "match-duration-viz"),
    ("visualization", "hero-pick-pie-viz"),
    ("visualization", "total-matches-viz"),
    ("visualization", "hero-pick-table-viz"),
]:
    r = requests.delete(
        f"{KIBANA_URL}/api/saved_objects/{obj_type}/{obj_id}",
        headers=HEADERS
    )
    print(f"  Delete {obj_id}: {r.status_code}")

# 3. Import new dashboard
print("\n=== Importing updated dashboard ===")
with open("/app/dashboards.ndjson", "rb") as f:
    r = requests.post(
        f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true",
        headers=HEADERS,
        files={"file": ("dashboards.ndjson", f, "application/x-ndjson")}
    )
result = r.json()
print(f"  Imported {result.get('successCount', 0)} objects, success={result.get('success')}")
if result.get('errors'):
    for e in result['errors']:
        print(f"  Error: {e}")

print("\n=== Done! ===")
