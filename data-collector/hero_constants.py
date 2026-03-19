"""
Hero ID → Name mapping constants.
Fetched from OpenDota /heroes endpoint at startup, with a fallback static map.
"""

import requests
import logging
import time

logger = logging.getLogger(__name__)

# Static fallback mapping (top ~30 popular heroes)
HERO_MAP_FALLBACK = {
    1: "Anti-Mage", 2: "Axe", 3: "Bane", 4: "Bloodseeker", 5: "Crystal Maiden",
    6: "Drow Ranger", 7: "Earthshaker", 8: "Juggernaut", 9: "Mirana", 10: "Morphling",
    11: "Shadow Fiend", 12: "Phantom Lancer", 13: "Puck", 14: "Pudge", 15: "Razor",
    16: "Sand King", 17: "Storm Spirit", 18: "Sven", 19: "Tiny", 20: "Vengeful Spirit",
    21: "Windranger", 22: "Zeus", 23: "Kunkka", 25: "Lina", 26: "Lion",
    27: "Shadow Shaman", 28: "Slardar", 29: "Tidehunter", 30: "Witch Doctor",
    31: "Lich", 32: "Riki", 33: "Enigma", 34: "Tinker", 35: "Sniper",
    36: "Necrophos", 37: "Warlock", 38: "Beastmaster", 39: "Queen of Pain",
    40: "Venomancer", 41: "Faceless Void", 42: "Wraith King", 43: "Death Prophet",
    44: "Phantom Assassin", 45: "Pugna", 46: "Templar Assassin", 47: "Viper",
    48: "Luna", 49: "Dragon Knight", 50: "Dazzle", 51: "Clockwerk",
    52: "Leshrac", 53: "Nature's Prophet", 54: "Lifestealer", 55: "Dark Seer",
    56: "Clinkz", 57: "Omniknight", 58: "Enchantress", 59: "Huskar",
    60: "Night Stalker", 61: "Broodmother", 62: "Bounty Hunter", 63: "Weaver",
    64: "Jakiro", 65: "Batrider", 66: "Chen", 67: "Spectre", 68: "Ancient Apparition",
    69: "Doom", 70: "Ursa", 71: "Spirit Breaker", 72: "Gyrocopter",
    73: "Alchemist", 74: "Invoker", 75: "Silencer", 76: "Outworld Destroyer",
    77: "Lycan", 78: "Brewmaster", 79: "Shadow Demon", 80: "Lone Druid",
    81: "Chaos Knight", 82: "Meepo", 83: "Treant Protector", 84: "Ogre Magi",
    85: "Undying", 86: "Rubick", 87: "Disruptor", 88: "Nyx Assassin",
    89: "Naga Siren", 90: "Keeper of the Light", 91: "Io", 92: "Visage",
    93: "Slark", 94: "Medusa", 95: "Troll Warlord", 96: "Centaur Warrunner",
    97: "Magnus", 98: "Timbersaw", 99: "Bristleback", 100: "Tusk",
    101: "Skywrath Mage", 102: "Abaddon", 103: "Elder Titan", 104: "Legion Commander",
    105: "Techies", 106: "Ember Spirit", 107: "Earth Spirit", 108: "Underlord",
    109: "Terrorblade", 110: "Phoenix", 111: "Oracle", 112: "Winter Wyvern",
    113: "Arc Warden", 114: "Monkey King", 119: "Dark Willow", 120: "Pangolier",
    121: "Grimstroke", 123: "Hoodwink", 126: "Void Spirit", 128: "Snapfire",
    129: "Mars", 131: "Ringmaster", 135: "Dawnbreaker", 136: "Marci",
    137: "Primal Beast", 138: "Muerta"
}

# Global hero map (populated at runtime)
HERO_MAP = {}


def fetch_hero_map(api_key=None):
    """Fetch hero mapping from OpenDota API."""
    global HERO_MAP
    
    url = "https://api.opendota.com/api/heroes"
    params = {}
    if api_key:
        params["api_key"] = api_key
    
    try:
        logger.info("Fetching hero mapping from OpenDota API...")
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        heroes = response.json()
        
        HERO_MAP = {hero["id"]: hero["localized_name"] for hero in heroes}
        logger.info(f"Loaded {len(HERO_MAP)} heroes from API")
        return HERO_MAP
        
    except Exception as e:
        logger.warning(f"Failed to fetch heroes from API: {e}. Using fallback map.")
        HERO_MAP = HERO_MAP_FALLBACK.copy()
        return HERO_MAP


def get_hero_name(hero_id):
    """Get hero name by ID."""
    if not HERO_MAP:
        return HERO_MAP_FALLBACK.get(hero_id, f"Unknown Hero ({hero_id})")
    return HERO_MAP.get(hero_id, f"Unknown Hero ({hero_id})")
