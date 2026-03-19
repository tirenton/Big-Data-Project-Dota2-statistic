"""
Log Transformer: Converts structured match JSON into game event log lines.
Each match generates one log entry per player (10 players = 10 log lines).
"""

import json
import logging
from datetime import datetime, timezone

from hero_constants import get_hero_name

logger = logging.getLogger(__name__)


def transform_match_to_logs(match_data):
    """
    Transform a match JSON object into a list of structured log events.
    
    Each log event represents one player's performance in the match.
    
    Returns:
        list[dict]: List of log event dictionaries
    """
    if not match_data:
        return []
    
    match_id = match_data.get("match_id")
    duration = match_data.get("duration", 0)
    radiant_win = match_data.get("radiant_win")
    start_time = match_data.get("start_time", 0)
    game_mode = match_data.get("game_mode", 0)
    radiant_score = match_data.get("radiant_score", 0)
    dire_score = match_data.get("dire_score", 0)
    
    # Convert Unix timestamp to ISO format (HACKED FOR REAL-TIME DEMONSTRATION)
    # Instead of using the 1-hour-old Valve timestamp, we stamp the 
    # exact second we downloaded it so it shows up in "Last 15 minutes"!
    match_timestamp = datetime.now(timezone.utc).isoformat()
    
    players = match_data.get("players", [])
    if not players:
        logger.warning(f"Match {match_id} has no player data")
        return []
    
    log_events = []
    
    for player in players:
        hero_id = player.get("hero_id", 0)
        
        # Skip players that abandoned before picking a hero (or incomplete API data)
        if hero_id == 0:
            continue
            
        hero_name = get_hero_name(hero_id)
        player_slot = player.get("player_slot", 0)
        is_radiant = player_slot < 128
        
        # Determine win/loss
        if radiant_win is not None:
            player_won = (is_radiant and radiant_win) or (not is_radiant and not radiant_win)
        else:
            player_won = None
        
        # Extract player stats
        kills = player.get("kills", 0)
        deaths = player.get("deaths", 0)
        assists = player.get("assists", 0)
        gpm = player.get("gold_per_min", 0)
        xpm = player.get("xp_per_min", 0)
        hero_damage = player.get("hero_damage", 0)
        hero_healing = player.get("hero_healing", 0)
        tower_damage = player.get("tower_damage", 0)
        last_hits = player.get("last_hits", 0)
        denies = player.get("denies", 0)
        level = player.get("level", 0)
        account_id = player.get("account_id")
        persona_name = player.get("personaname", "Anonymous")
        
        # Compute KDA ratio
        kda = (kills + assists) / max(deaths, 1)
        
        # Build log event
        log_event = {
            # Match info
            "timestamp": match_timestamp,
            "event_type": "MATCH_END",
            "match_id": match_id,
            "duration": duration,
            "game_mode": game_mode,
            "radiant_score": radiant_score,
            "dire_score": dire_score,
            "radiant_win": radiant_win,
            
            # Player info
            "account_id": account_id,
            "player_name": persona_name if persona_name else "Anonymous",
            "player_slot": player_slot,
            "team": "Radiant" if is_radiant else "Dire",
            "result": "WIN" if player_won else ("LOSS" if player_won is not None else "UNKNOWN"),
            
            # Hero info
            "hero_id": hero_id,
            "hero_name": hero_name,
            
            # Performance stats
            "kills": kills,
            "deaths": deaths,
            "assists": assists,
            "kda": round(kda, 2),
            "gpm": gpm,
            "xpm": xpm,
            "hero_damage": hero_damage,
            "hero_healing": hero_healing,
            "tower_damage": tower_damage,
            "last_hits": last_hits,
            "denies": denies,
            "level": level
        }
        
        log_events.append(log_event)
    
    logger.info(f"Match {match_id}: generated {len(log_events)} log events")
    return log_events


def log_event_to_string(log_event):
    """
    Convert a log event dict to a human-readable log line string.
    
    Format:
        TIMESTAMP | EVENT_TYPE | match_id=X | hero=Y | player=Z | kills=K | deaths=D | assists=A | result=R | duration=Ds
    """
    return (
        f"{log_event['timestamp']} | {log_event['event_type']} | "
        f"match_id={log_event['match_id']} | hero={log_event['hero_name']} | "
        f"player={log_event['player_name']} | team={log_event['team']} | "
        f"kills={log_event['kills']} | deaths={log_event['deaths']} | "
        f"assists={log_event['assists']} | kda={log_event['kda']} | "
        f"gpm={log_event['gpm']} | xpm={log_event['xpm']} | "
        f"result={log_event['result']} | duration={log_event['duration']}s"
    )


def log_event_to_json(log_event):
    """Convert log event to JSON string."""
    return json.dumps(log_event)
