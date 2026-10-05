"""
Gold Layer Analytical Marts: Race Results, Pit Stop Analysis, and Qualifying Analytics.
Provides denormalized star-schema performance views and race telemetry metrics.
"""

import os
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, min as _min, count, desc, current_timestamp
)
from src.utils.logger import get_logger

logger = get_logger("GoldTelemetry")

class TelemetryManager:
    """
    Computes curated analytical views for race results, pit stop strategies, and qualifying metrics.
    """

    def __init__(self, spark: SparkSession, base_data_dir: Optional[str] = None):
        self.spark = spark
        self.base_data_dir = base_data_dir or os.getenv("LAKEHOUSE_STORAGE_ROOT", "./data")
        self.silver_dir = os.path.join(self.base_data_dir, "silver")
        self.gold_dir = os.path.join(self.base_data_dir, "gold")
        os.makedirs(self.gold_dir, exist_ok=True)

    def build_race_results_gold(self) -> DataFrame:
        """
        Builds race_results_gold denormalized dimensional view:
        Joins results, races, circuits, drivers, and constructors.
        """
        logger.info("Computing Gold Race Results Mart...")
        results = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "results"))
        races = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "races"))
        circuits = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "circuits"))
        drivers = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "drivers"))
        constructors = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "constructors"))

        df = (
            results
            .join(races, "race_id")
            .join(circuits, "circuit_id")
            .join(drivers, "driver_id")
            .join(constructors, "constructor_id")
            .select(
                races["race_year"],
                races["round"],
                races["race_name"],
                races["race_date"],
                circuits["circuit_name"],
                circuits["location"].alias("circuit_location"),
                circuits["country"].alias("circuit_country"),
                drivers["full_name"].alias("driver_name"),
                drivers["code"].alias("driver_code"),
                drivers["number"].alias("driver_number"),
                drivers["nationality"].alias("driver_nationality"),
                constructors["name"].alias("team_name"),
                results["grid"],
                results["position"],
                results["position_text"],
                results["position_order"],
                results["points"],
                results["laps"],
                results["time"].alias("race_time"),
                results["fastest_lap"],
                results["fastest_lap_time"],
                results["fastest_lap_speed"]
            )
            .withColumn("positions_gained", col("grid") - col("position"))
            .withColumn("ingestion_date", current_timestamp())
        )

        target = os.path.join(self.gold_dir, "race_results_gold")
        df.write.format("delta").mode("overwrite").partitionBy("race_year").save(target)
        logger.info(f"Gold race_results_gold saved to {target} (count: {df.count()})")
        return df

    def build_pit_stop_analysis(self) -> DataFrame:
        """
        Builds pit_stop_analysis Gold table:
        Calculates average pit stop duration, total stops, and fastest stop per team/race.
        """
        logger.info("Computing Gold Pit Stop Analysis Mart...")
        pitstops = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "pitstops"))
        races = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "races"))
        results = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "results"))
        constructors = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "constructors"))

        clean_constructors = constructors.select("constructor_id", col("name").alias("team_name"))
        clean_races = races.select("race_id", "race_year", "race_name")

        joined = (
            pitstops
            .join(clean_races, "race_id")
            .join(results.select("race_id", "driver_id", "constructor_id"), ["race_id", "driver_id"])
            .join(clean_constructors, "constructor_id")
        )

        agg_df = (
            joined
            .groupBy("race_year", "race_name", "constructor_id", "team_name")
            .agg(
                count("stop").alias("total_pit_stops"),
                avg("milliseconds").alias("avg_pit_stop_ms"),
                _min("milliseconds").alias("fastest_pit_stop_ms")
            )
            .withColumn("avg_pit_stop_sec", col("avg_pit_stop_ms") / 1000.0)
            .withColumn("fastest_pit_stop_sec", col("fastest_pit_stop_ms") / 1000.0)
            .withColumn("ingestion_date", current_timestamp())
        )

        target = os.path.join(self.gold_dir, "pit_stop_analysis")
        agg_df.write.format("delta").mode("overwrite").partitionBy("race_year").save(target)
        logger.info(f"Gold pit_stop_analysis saved to {target} (count: {agg_df.count()})")
        return agg_df

    def build_qualifying_analysis(self) -> DataFrame:
        """
        Builds qualifying_analysis Gold table:
        Analyzes qualifying pace, pole position delta, and conversion into race victory.
        """
        logger.info("Computing Gold Qualifying Analysis Mart...")
        qualifying = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "qualifying"))
        races = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "races"))
        drivers = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "drivers"))
        results = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "results"))

        joined = (
            qualifying
            .join(races, "race_id")
            .join(drivers, "driver_id")
            .join(results.select("race_id", "driver_id", results["position"].alias("finish_position")), ["race_id", "driver_id"])
            .select(
                races["race_year"],
                races["race_name"],
                drivers["full_name"].alias("driver_name"),
                qualifying["position"].alias("qualifying_position"),
                col("finish_position"),
                qualifying["q1"],
                qualifying["q2"],
                qualifying["q3"]
            )
            .withColumn("grid_delta", col("qualifying_position") - col("finish_position"))
            .withColumn("ingestion_date", current_timestamp())
        )

        target = os.path.join(self.gold_dir, "qualifying_analysis")
        joined.write.format("delta").mode("overwrite").partitionBy("race_year").save(target)
        logger.info(f"Gold qualifying_analysis saved to {target} (count: {joined.count()})")
        return joined
