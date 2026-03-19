"""Quick script to index hero stats into Elasticsearch."""
from es_indexer import DotaElasticsearchIndexer
import json

es = DotaElasticsearchIndexer(host='elasticsearch', port=9200)
es.setup_indices()

with open('/app/data/processed/hero_stats.json', 'r') as f:
    hero_stats = json.load(f)

count = es.bulk_index('dota2_hero_stats', hero_stats)
print(f'Indexed {count} hero stats')
print(f'hero_stats count: {es.get_doc_count("dota2_hero_stats")}')
print(f'player_stats count: {es.get_doc_count("dota2_player_stats")}')
