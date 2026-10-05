-- ==============================================================================
-- FORMULA 1 LAKEHOUSE - GOLD LAYER ANALYTICAL QUERIES
-- Target: Databricks SQL / DuckDB / Delta Lake
-- ==============================================================================

-- 1. Official Driver Championship Leaderboard (Latest Season)
SELECT 
    race_year,
    rank,
    full_name AS driver_name,
    driver_nationality AS nationality,
    team_name,
    total_points,
    wins,
    podiums,
    races_entered
FROM delta.`./data/gold/driver_standings`
WHERE race_year = 2021
ORDER BY rank ASC;

-- 2. Constructors Championship Standings
SELECT 
    race_year,
    rank,
    team_name,
    team_nationality AS nationality,
    total_points,
    wins,
    podiums
FROM delta.`./data/gold/constructor_standings`
WHERE race_year = 2021
ORDER BY rank ASC;

-- 3. Race Podium Finishes and Overtaking Performance
SELECT 
    race_year,
    round,
    race_name,
    driver_name,
    team_name,
    grid,
    position,
    positions_gained,
    points,
    race_time,
    fastest_lap_speed
FROM delta.`./data/gold/race_results_gold`
WHERE position <= 3
ORDER BY race_year DESC, round ASC, position ASC;

-- 4. Pit Stop Efficiency by Constructor
SELECT 
    race_year,
    team_name,
    COUNT(total_pit_stops) AS total_races_participated,
    SUM(total_pit_stops) AS cumulative_pit_stops,
    ROUND(AVG(avg_pit_stop_sec), 3) AS avg_stop_duration_sec,
    ROUND(MIN(fastest_pit_stop_sec), 3) AS season_fastest_stop_sec
FROM delta.`./data/gold/pit_stop_analysis`
GROUP BY race_year, team_name
ORDER BY avg_stop_duration_sec ASC;

-- 5. Qualifying Pole Position Conversion Rate
SELECT 
    race_year,
    driver_name,
    COUNT(*) AS pole_positions,
    SUM(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS wins_from_pole,
    ROUND(SUM(CASE WHEN finish_position = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 1) AS pole_to_win_percentage
FROM delta.`./data/gold/qualifying_analysis`
WHERE qualifying_position = 1
GROUP BY race_year, driver_name
ORDER BY pole_positions DESC;
