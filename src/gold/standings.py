"""
Gold Layer Analytical Marts: Driver and Constructor Standings.
Uses Spark window functions to aggregate championship points, count race victories,
and calculate official FIA-compliant season championship rankings.
"""

import os
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, count, when, desc, row_number, rank, dense_rank, current_timestamp
)
from pyspark.sql.window import Window
from src.utils.logger import get_logger

logger = get_logger("GoldStandings")

class StandingsManager:
    """
    Computes curated Gold Driver and Constructor championship standings from Silver tables.
    """

    def __init__(self, spark: SparkSession, base_data_dir: Optional[str] = None):
        self.spark = spark
        self.base_data_dir = base_data_dir or os.getenv("LAKEHOUSE_STORAGE_ROOT", "./data")
        self.silver_dir = os.path.join(self.base_data_dir, "silver")
        self.gold_dir = os.path.join(self.base_data_dir, "gold")
        os.makedirs(self.gold_dir, exist_ok=True)

    def calculate_driver_standings(self) -> DataFrame:
        """
        Builds driver_standings Gold table:
        Aggregates season points, total wins (position=1), podiums (position<=3),
        and applies championship rank ranking with tie-breaking rules.
        """
        logger.info("Computing Gold Driver Standings...")
        results_df = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "results"))
        races_df = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "races"))
        drivers_df = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "drivers"))
        constructors_df = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "constructors"))

        # Select explicit disambiguated columns before join
        clean_drivers = drivers_df.select(
            "driver_id", "driver_ref", "full_name", col("nationality").alias("driver_nationality")
        )
        clean_constructors = constructors_df.select(
            "constructor_id", col("name").alias("team_name")
        )

        # Join datasets
        joined_df = (
            results_df
            .join(races_df.select("race_id", "race_year"), "race_id")
            .join(clean_drivers, "driver_id")
            .join(clean_constructors, "constructor_id")
        )

        # Aggregate points and wins per driver per season
        agg_df = (
            joined_df
            .groupBy("race_year", "driver_id", "driver_ref", "full_name", "driver_nationality", "team_name")
            .agg(
                _sum("points").alias("total_points"),
                count(when(col("position") == 1, True)).alias("wins"),
                count(when((col("position") >= 1) & (col("position") <= 3), True)).alias("podiums"),
                count("result_id").alias("races_entered")
            )
        )

        # Window specification for ranking: by year, descending by points, then wins
        standing_window = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

        final_df = (
            agg_df
            .withColumn("rank", dense_rank().over(standing_window))
            .withColumn("ingestion_date", current_timestamp())
        )

        target_path = os.path.join(self.gold_dir, "driver_standings")
        final_df.write.format("delta").mode("overwrite").partitionBy("race_year").save(target_path)
        logger.info(f"Gold driver_standings saved to {target_path} (count: {final_df.count()})")
        return final_df

    def calculate_constructor_standings(self) -> DataFrame:
        """
        Builds constructor_standings Gold table:
        Aggregates team points, team victories, and calculates constructors championship rank.
        """
        logger.info("Computing Gold Constructor Standings...")
        results_df = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "results"))
        races_df = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "races"))
        constructors_df = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "constructors"))

        clean_constructors = constructors_df.select(
            "constructor_id", "constructor_ref", col("name").alias("team_name"), col("nationality").alias("team_nationality")
        )

        joined_df = (
            results_df
            .join(races_df.select("race_id", "race_year"), "race_id")
            .join(clean_constructors, "constructor_id")
        )

        agg_df = (
            joined_df
            .groupBy("race_year", "constructor_id", "constructor_ref", "team_name", "team_nationality")
            .agg(
                _sum("points").alias("total_points"),
                count(when(col("position") == 1, True)).alias("wins"),
                count(when((col("position") >= 1) & (col("position") <= 3), True)).alias("podiums")
            )
        )

        standing_window = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

        final_df = (
            agg_df
            .withColumn("rank", dense_rank().over(standing_window))
            .withColumn("ingestion_date", current_timestamp())
        )

        target_path = os.path.join(self.gold_dir, "constructor_standings")
        final_df.write.format("delta").mode("overwrite").partitionBy("race_year").save(target_path)
        logger.info(f"Gold constructor_standings saved to {target_path} (count: {final_df.count()})")
        return final_df
