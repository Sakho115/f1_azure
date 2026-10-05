-- ==============================================================================
-- FORMULA 1 LAKEHOUSE - ADVANCED ANALYTICAL KPIS & WINDOW FUNCTIONS
-- ==============================================================================

-- 1. Rolling Cumulative Points Trajectory by Driver Across the Season
WITH race_points AS (
    SELECT 
        race_year,
        round,
        race_name,
        driver_name,
        team_name,
        points
    FROM delta.`./data/gold/race_results_gold`
)
SELECT 
    race_year,
    round,
    race_name,
    driver_name,
    team_name,
    points AS race_points_earned,
    SUM(points) OVER (
        PARTITION BY race_year, driver_name 
        ORDER BY round 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_season_points
FROM race_points
ORDER BY race_year, round, cumulative_season_points DESC;

-- 2. Teammate Qualifying Battle (Head-to-Head Comparison)
WITH quali_data AS (
    SELECT 
        q.race_year,
        q.race_name,
        q.driver_name,
        r.team_name,
        q.qualifying_position
    FROM delta.`./data/gold/qualifying_analysis` q
    JOIN delta.`./data/gold/race_results_gold` r
      ON q.race_year = r.race_year AND q.race_name = r.race_name AND q.driver_name = r.driver_name
)
SELECT 
    team_name,
    driver_name,
    COUNT(*) AS sessions_participated,
    AVG(qualifying_position) AS avg_qualifying_pos,
    MIN(qualifying_position) AS best_qualifying_pos
FROM quali_data
GROUP BY team_name, driver_name
ORDER BY team_name, avg_qualifying_pos ASC;

-- 3. Top Overtakers: Most Net Positions Gained in Race Conditions
SELECT 
    driver_name,
    team_name,
    SUM(positions_gained) AS total_positions_gained,
    AVG(positions_gained) AS avg_positions_gained_per_race,
    MAX(positions_gained) AS max_positions_gained_in_single_race
FROM delta.`./data/gold/race_results_gold`
WHERE positions_gained > 0
GROUP BY driver_name, team_name
ORDER BY total_positions_gained DESC;
