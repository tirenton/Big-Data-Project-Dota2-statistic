"""
OpenDota API Data Collector.
Fetches public match data and detailed match information.
"""

import requests
import time
import json
import os
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://api.opendota.com/api"


class DotaCollector:
    """Collects Dota 2 match data from OpenDota API."""
    
    def __init__(self, api_key=None, batch_size=50, api_delay=1.2):
        self.api_key = api_key
        self.batch_size = batch_size
        self.api_delay = api_delay
        self.session = requests.Session()
        
        if api_key:
            self.session.params = {"api_key": api_key}
    
    def _request(self, endpoint, params=None):
        """Make a rate-limited API request."""
        url = f"{BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            time.sleep(self.api_delay)  # Rate limiting
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.warning("Rate limited! Waiting 60 seconds...")
                time.sleep(60)
                return self._request(endpoint, params)
            logger.error(f"HTTP error for {endpoint}: {e}")
            return None
        except Exception as e:
            logger.error(f"Request error for {endpoint}: {e}")
            return None
    
    def fetch_public_matches(self):
        """Fetch a batch of recent public match IDs."""
        logger.info(f"Fetching {self.batch_size} public matches...")
        matches = self._request("/publicMatches")
        
        if not matches:
            logger.error("Failed to fetch public matches")
            return []
        
        # Take only the requested batch size
        match_ids = [m["match_id"] for m in matches[:self.batch_size]]
        logger.info(f"Got {len(match_ids)} match IDs")
        return match_ids
    
    def fetch_match_details(self, match_id):
        """Fetch detailed data for a single match."""
        logger.info(f"Fetching details for match {match_id}...")
        data = self._request(f"/matches/{match_id}")
        
        if not data:
            logger.warning(f"Failed to fetch match {match_id}")
            return None
        
        return data
    
    def collect_matches(self):
        """
        Main collection workflow:
        1. Get public match IDs
        2. Fetch detailed data for each match
        3. Yield match data as it's collected
        """
        match_ids = self.fetch_public_matches()
        
        if not match_ids:
            logger.error("No match IDs collected. Exiting.")
            return
        
        collected = 0
        failed = 0
        
        for match_id in match_ids:
            match_data = self.fetch_match_details(match_id)
            
            if match_data:
                collected += 1
                logger.info(
                    f"[{collected}/{len(match_ids)}] Match {match_id}: "
                    f"duration={match_data.get('duration', 'N/A')}s, "
                    f"radiant_win={match_data.get('radiant_win', 'N/A')}"
                )
                yield match_data
            else:
                failed += 1
                logger.warning(f"Skipping match {match_id} (failed to fetch)")
        
        logger.info(f"Collection complete: {collected} collected, {failed} failed")
    
    def save_raw_data(self, match_data, output_dir="/app/data/raw"):
        """Save raw match JSON to disk."""
        os.makedirs(output_dir, exist_ok=True)
        match_id = match_data.get("match_id", "unknown")
        filepath = os.path.join(output_dir, f"match_{match_id}.json")
        
        with open(filepath, "w") as f:
            json.dump(match_data, f, indent=2)
        
        return filepath
