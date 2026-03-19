-- ============================================
-- Dota 2 Analytics - Hive Table Definitions
-- ============================================
-- These tables are external tables reading from HDFS Parquet files
-- written by the Spark processor.

-- Raw match event logs (per-player per-match)
CREATE EXTERNAL TABLE IF NOT EXISTS dota2_matches (
    `timestamp`     STRING,
    event_type      STRING,
    match_id        BIGINT,
    duration        INT,
    game_mode       INT,
    radiant_score   INT,
    dire_score      INT,
    radiant_win     BOOLEAN,
    account_id      BIGINT,
    player_name     STRING,
    player_slot     INT,
    team            STRING,
    result          STRING,
    hero_id         INT,
    hero_name       STRING,
    kills           INT,
    deaths          INT,
    assists         INT,
    kda             FLOAT,
    gpm             INT,
    xpm             INT,
    hero_damage     INT,
    hero_healing    INT,
    tower_damage    INT,
    last_hits       INT,
    denies          INT,
    level           INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/dota2/processed/match_events';


-- Aggregated hero usage statistics
CREATE EXTERNAL TABLE IF NOT EXISTS dota2_hero_usage (
    `timestamp`     STRING,
    hero_id         INT,
    hero_name       STRING,
    total_picks     INT,
    total_wins      INT,
    win_rate        FLOAT,
    avg_kills       FLOAT,
    avg_deaths      FLOAT,
    avg_assists     FLOAT,
    avg_kda         FLOAT,
    avg_gpm         FLOAT,
    avg_xpm         FLOAT,
    avg_hero_damage FLOAT,
    avg_tower_damage FLOAT,
    avg_last_hits   FLOAT,
    avg_duration    FLOAT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/dota2/processed/hero_stats';


-- Player performance aggregates
CREATE EXTERNAL TABLE IF NOT EXISTS dota2_player_performance (
    account_id      BIGINT,
    player_name     STRING,
    games_played    INT,
    wins            INT,
    avg_kills       FLOAT,
    avg_deaths      FLOAT,
    avg_assists     FLOAT,
    avg_kda         FLOAT,
    avg_gpm         FLOAT,
    avg_xpm         FLOAT,
    total_kills     BIGINT,
    total_deaths    BIGINT,
    total_assists   BIGINT,
    win_rate        FLOAT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/dota2/processed/player_performance';


-- ============================================
-- Sample Queries
-- ============================================

-- Top 10 most picked heroes
-- SELECT hero_name, total_picks, win_rate 
-- FROM dota2_hero_usage 
-- ORDER BY total_picks DESC 
-- LIMIT 10;

-- Top players by KDA
-- SELECT player_name, games_played, avg_kda, win_rate 
-- FROM dota2_player_performance 
-- ORDER BY avg_kda DESC 
-- LIMIT 10;

-- Average match duration
-- SELECT AVG(duration) as avg_duration 
-- FROM dota2_matches 
-- WHERE match_id IS NOT NULL;
