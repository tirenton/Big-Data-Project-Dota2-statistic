"""
Elasticsearch indexer: Sends processed log events directly to Elasticsearch.
"""

import json
import logging
import time
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError

logger = logging.getLogger(__name__)


# Index mapping for match player events
MATCH_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "event_type": {"type": "keyword"},
            "match_id": {"type": "long"},
            "duration": {"type": "integer"},
            "game_mode": {"type": "integer"},
            "radiant_score": {"type": "integer"},
            "dire_score": {"type": "integer"},
            "radiant_win": {"type": "boolean"},
            "account_id": {"type": "long"},
            "player_name": {"type": "keyword"},
            "player_slot": {"type": "integer"},
            "team": {"type": "keyword"},
            "result": {"type": "keyword"},
            "hero_id": {"type": "integer"},
            "hero_name": {"type": "keyword"},
            "kills": {"type": "integer"},
            "deaths": {"type": "integer"},
            "assists": {"type": "integer"},
            "kda": {"type": "float"},
            "gpm": {"type": "integer"},
            "xpm": {"type": "integer"},
            "hero_damage": {"type": "integer"},
            "hero_healing": {"type": "integer"},
            "tower_damage": {"type": "integer"},
            "last_hits": {"type": "integer"},
            "denies": {"type": "integer"},
            "level": {"type": "integer"}
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}

# Aggregated hero stats index mapping
HERO_STATS_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "hero_id": {"type": "integer"},
            "hero_name": {"type": "keyword"},
            "total_picks": {"type": "integer"},
            "total_wins": {"type": "integer"},
            "win_rate": {"type": "float"},
            "avg_kills": {"type": "float"},
            "avg_deaths": {"type": "float"},
            "avg_assists": {"type": "float"},
            "avg_kda": {"type": "float"},
            "avg_gpm": {"type": "float"},
            "avg_xpm": {"type": "float"},
            "avg_hero_damage": {"type": "float"},
            "avg_duration": {"type": "float"}
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}


class DotaElasticsearchIndexer:
    """Indexes Dota 2 analytics data into Elasticsearch."""
    
    def __init__(self, host="elasticsearch", port=9200, max_retries=30):
        self.es = None
        self.host = host
        self.port = port
        self._connect(max_retries)
    
    def _connect(self, max_retries=30):
        """Connect to Elasticsearch with retries."""
        for attempt in range(max_retries):
            try:
                self.es = Elasticsearch(
                    [f"http://{self.host}:{self.port}"],
                    request_timeout=30
                )
                if self.es.ping():
                    logger.info(f"Connected to Elasticsearch: {self.host}:{self.port}")
                    return
            except Exception:
                pass
            
            wait_time = min(5 * (attempt + 1), 30)
            logger.warning(
                f"Elasticsearch not available (attempt {attempt + 1}/{max_retries}). "
                f"Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)
        
        raise ConnectionError(f"Could not connect to Elasticsearch after {max_retries} attempts")
    
    def setup_indices(self):
        """Create Elasticsearch indices with mappings."""
        indices = {
            "dota2_player_stats": MATCH_INDEX_MAPPING,
            "dota2_hero_stats": HERO_STATS_INDEX_MAPPING
        }
        
        for index_name, mapping in indices.items():
            try:
                if not self.es.indices.exists(index=index_name):
                    self.es.indices.create(index=index_name, body=mapping)
                    logger.info(f"Created index: {index_name}")
                else:
                    logger.info(f"Index already exists: {index_name}")
            except Exception as e:
                logger.error(f"Failed to create index {index_name}: {e}")
    
    def index_player_event(self, log_event):
        """Index a single player match event."""
        try:
            doc_id = f"{log_event['match_id']}_{log_event['player_slot']}"
            self.es.index(
                index="dota2_player_stats",
                id=doc_id,
                document=log_event
            )
            return True
        except Exception as e:
            logger.error(f"Failed to index player event: {e}")
            return False
    
    def index_hero_stats(self, hero_stats):
        """Index aggregated hero statistics."""
        try:
            doc_id = f"hero_{hero_stats['hero_id']}"
            self.es.index(
                index="dota2_hero_stats",
                id=doc_id,
                document=hero_stats
            )
            return True
        except Exception as e:
            logger.error(f"Failed to index hero stats: {e}")
            return False
    
    def bulk_index(self, index_name, documents):
        """Bulk index documents."""
        from elasticsearch.helpers import bulk
        
        actions = []
        for doc in documents:
            action = {
                "_index": index_name,
                "_source": doc
            }
            if "match_id" in doc and "player_slot" in doc:
                action["_id"] = f"{doc['match_id']}_{doc['player_slot']}"
            elif "hero_id" in doc:
                action["_id"] = f"hero_{doc['hero_id']}"
            actions.append(action)
        
        try:
            success, errors = bulk(self.es, actions, raise_on_error=False)
            logger.info(f"Bulk indexed {success} documents to {index_name}")
            if errors:
                logger.warning(f"Bulk indexing errors: {len(errors)}")
            return success
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}")
            return 0
    
    def get_doc_count(self, index_name):
        """Get document count for an index."""
        try:
            result = self.es.count(index=index_name)
            return result["count"]
        except Exception:
            return 0
