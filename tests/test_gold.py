"""
Unit and integration tests for Gold Layer Analytical Marts.
Tests point accumulation, victory counts, and window ranking algorithms.
"""

import os
import pytest
from src.gold.standings import StandingsManager
from src.gold.telemetry import TelemetryManager

def test_gold_standings_and_telemetry(spark_session):
    standings = StandingsManager(spark_session)
    telemetry = TelemetryManager(spark_session)

    # 1. Driver Standings
    driver_df = standings.calculate_driver_standings()
    assert "total_points" in driver_df.columns
    assert "wins" in driver_df.columns
    assert "rank" in driver_df.columns
    assert driver_df.count() > 0

    # Ensure rank 1 has the most or equal points to rank 2
    top_two = driver_df.filter("rank <= 2").orderBy("rank").collect()
    if len(top_two) >= 2:
        assert top_two[0]["total_points"] >= top_two[1]["total_points"]

    # 2. Constructor Standings
    constructor_df = standings.calculate_constructor_standings()
    assert "total_points" in constructor_df.columns
    assert "rank" in constructor_df.columns
    assert constructor_df.count() > 0

    # 3. Race Results Gold
    race_results_gold = telemetry.build_race_results_gold()
    assert "positions_gained" in race_results_gold.columns
    assert "circuit_name" in race_results_gold.columns
    assert race_results_gold.count() > 0
