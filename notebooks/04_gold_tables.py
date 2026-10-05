"""
# Databricks / Local Lakehouse Notebook: 04_gold_tables
# Description: Generates curated Gold dimensional and analytical marts:
# driver_standings, constructor_standings, race_results_gold, pit_stop_analysis, qualifying_analysis.
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.spark import get_spark_session
from src.utils.logger import get_logger
from src.gold.standings import StandingsManager
from src.gold.telemetry import TelemetryManager

logger = get_logger("Notebook_04_Gold")

def run_gold_tables():
    spark = get_spark_session(app_name="04_Gold_Tables")
    try:
        standings = StandingsManager(spark)
        telemetry = TelemetryManager(spark)

        counts = {
            "driver_standings": standings.calculate_driver_standings().count(),
            "constructor_standings": standings.calculate_constructor_standings().count(),
            "race_results_gold": telemetry.build_race_results_gold().count(),
            "pit_stop_analysis": telemetry.build_pit_stop_analysis().count(),
            "qualifying_analysis": telemetry.build_qualifying_analysis().count(),
        }
        logger.info(f"Successfully generated all Gold analytical marts: {counts}")
        return counts
    finally:
        pass

if __name__ == "__main__":
    run_gold_tables()
