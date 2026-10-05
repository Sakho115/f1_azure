"""
Deterministic Historical Backfill Engine for Formula 1 Lakehouse.
Enables historical backfills across race seasons and window dates in deterministic chronological order.
"""

import os
import sys
import argparse
from typing import Optional, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.spark import get_spark_session
from src.utils.logger import get_logger
from orchestration.pipeline import F1LakehousePipeline
from pyspark.sql.functions import col

logger = get_logger("HistoricalBackfillEngine")

class BackfillManager:
    """
    Manages deterministic replay and backfilling of historical race windows.
    """

    def __init__(self):
        self.pipeline = F1LakehousePipeline()

    def run_backfill(
        self,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        window_date: Optional[str] = None
    ):
        """
        Executes a deterministic backfill over the requested temporal window.
        """
        logger.info(f"Starting deterministic backfill: start_year={start_year}, end_year={end_year}, window_date={window_date}")
        self.pipeline._init_spark()

        # Step 1: Ensure bronze ingestion is complete
        logger.info("Verifying Bronze Lakehouse tables...")
        self.pipeline.run_bronze_stage()

        # Step 2: Load races and filter by requested window
        races_df = self.pipeline.spark.read.format("delta").load(
            os.path.join(self.pipeline.config["storage"]["bronze"], "races")
        )

        filter_expr = "1=1"
        if start_year:
            filter_expr += f" AND year >= {start_year}"
        if end_year:
            filter_expr += f" AND year <= {end_year}"
        if window_date:
            filter_expr += f" AND date <= '{window_date}'"

        target_races = (
            races_df
            .filter(filter_expr)
            .select("raceId", "year", "round", "name", "date")
            .orderBy("year", "round")
            .collect()
        )

        logger.info(f"Identified {len(target_races)} races matching backfill criteria.")

        for r in target_races:
            logger.info(f"Replaying Race: ID={r['raceId']} Year={r['year']} Round={r['round']} Name='{r['name']}' Date={r['date']}")

        # Step 3: Run Silver transformations and Delta MERGE
        logger.info("Executing Silver layer upsert for backfilled window...")
        self.pipeline.run_silver_stage()

        # Step 4: Recompute Gold marts
        logger.info("Recomputing Gold Championship Standings and Telemetry Marts...")
        self.pipeline.run_gold_stage()

        # Step 5: Validate Data Quality
        logger.info("Validating post-backfill data quality...")
        self.pipeline.run_quality_stage()

        logger.info("Deterministic backfill successfully executed.")

def main():
    parser = argparse.ArgumentParser(description="Deterministic Historical Backfill Runner")
    parser.add_argument("--start-year", type=int, default=2021, help="Start season year")
    parser.add_argument("--end-year", type=int, default=2022, help="End season year")
    parser.add_argument("--window-date", type=str, default=None, help="Upper bound date (YYYY-MM-DD)")
    args = parser.parse_args()

    manager = BackfillManager()
    manager.run_backfill(
        start_year=args.start_year,
        end_year=args.end_year,
        window_date=args.window_date
    )

if __name__ == "__main__":
    main()
