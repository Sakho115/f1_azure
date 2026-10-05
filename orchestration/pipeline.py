"""
Master Pipeline Orchestrator for Formula 1 Lakehouse.
Provides conditional stage execution, dependency management, execution metrics,
and CLI options mirroring Azure Data Factory / Databricks Workflows.
"""

import os
import sys
import time
import argparse
import yaml
from typing import Dict, Any, Optional

# Ensure project root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.spark import get_spark_session
from src.utils.logger import get_logger
from src.ingestion.file_ingestion import BronzeIngestionManager
from src.silver.merge_manager import SilverManager
from src.gold.standings import StandingsManager
from src.gold.telemetry import TelemetryManager
from src.quality.expectations import DataQualityValidator

logger = get_logger("PipelineOrchestrator")

class F1LakehousePipeline:
    """
    Coordinates end-to-end execution of Bronze Ingestion, Silver Transformations & Upserts,
    Gold Analytical Marts, and Quality Assertions.
    """

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), "config.yaml")
        self.config = self._load_config()
        self.spark = None

    def _load_config(self) -> Dict[str, Any]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _init_spark(self):
        if self.spark is None:
            self.spark = get_spark_session(
                app_name=self.config.get("pipeline", {}).get("name", "F1LakehousePipeline")
            )

    def run_bronze_stage(self) -> Dict[str, int]:
        logger.info("================== STAGE: BRONZE INGESTION ==================")
        self._init_spark()
        manager = BronzeIngestionManager(self.spark)
        counts = manager.ingest_all()
        logger.info("Bronze Stage Successfully Completed.")
        return counts

    def run_silver_stage(self) -> Dict[str, int]:
        logger.info("================== STAGE: SILVER TRANSFORM & MERGE ==================")
        self._init_spark()
        manager = SilverManager(self.spark)
        counts = manager.process_all_silver()
        logger.info("Silver Stage Successfully Completed.")
        return counts

    def run_gold_stage(self) -> Dict[str, int]:
        logger.info("================== STAGE: GOLD ANALYTICAL MARTS ==================")
        self._init_spark()
        standings = StandingsManager(self.spark)
        telemetry = TelemetryManager(self.spark)

        counts = {
            "driver_standings": standings.calculate_driver_standings().count(),
            "constructor_standings": standings.calculate_constructor_standings().count(),
            "race_results_gold": telemetry.build_race_results_gold().count(),
            "pit_stop_analysis": telemetry.build_pit_stop_analysis().count(),
            "qualifying_analysis": telemetry.build_qualifying_analysis().count(),
        }
        logger.info(f"Gold Stage Successfully Completed: {counts}")
        return counts

    def run_quality_stage(self) -> Dict[str, Any]:
        logger.info("================== STAGE: DATA QUALITY ASSERTIONS ==================")
        self._init_spark()
        validator = DataQualityValidator(self.spark)
        report = validator.run_full_suite()
        logger.info("Data Quality Stage Successfully Passed.")
        return report

    def run_all(self) -> Dict[str, Any]:
        """Runs the entire lakehouse pipeline end-to-end."""
        start_time = time.time()
        logger.info(">>> Starting End-to-End Formula 1 Lakehouse Pipeline Execution <<<")

        metrics = {
            "bronze": self.run_bronze_stage(),
            "silver": self.run_silver_stage(),
            "gold": self.run_gold_stage(),
            "quality": self.run_quality_stage(),
        }

        duration = round(time.time() - start_time, 2)
        logger.info(f">>> Lakehouse Pipeline Finished Successfully in {duration} seconds <<<")
        return {"status": "SUCCESS", "duration_seconds": duration, "metrics": metrics}

def main():
    parser = argparse.ArgumentParser(description="Formula 1 Lakehouse Pipeline Orchestrator")
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold", "quality", "all"],
        default="all",
        help="Specify the pipeline stage to execute (default: all)"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Execution mode (full refresh vs incremental merge)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate execution without modifying lakehouse Delta tables"
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info(f"[DRY RUN] Would execute stage '{args.stage}' in '{args.mode}' mode. No tables modified.")
        return

    orchestrator = F1LakehousePipeline()

    if args.stage == "bronze":
        orchestrator.run_bronze_stage()
    elif args.stage == "silver":
        orchestrator.run_silver_stage()
    elif args.stage == "gold":
        orchestrator.run_gold_stage()
    elif args.stage == "quality":
        orchestrator.run_quality_stage()
    elif args.stage == "all":
        orchestrator.run_all()

if __name__ == "__main__":
    main()
