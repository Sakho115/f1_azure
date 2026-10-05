"""
# Databricks / Local Lakehouse Notebook: 03_incremental_merge
# Description: Demonstrates incremental Delta MERGE upserts with audit columns (create_date / update_date).
"""

import sys
import os
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.spark import get_spark_session
from src.utils.logger import get_logger
from src.silver.merge_manager import SilverManager
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

logger = get_logger("Notebook_03_IncrementalMerge")

def run_incremental_merge_demo():
    spark = get_spark_session(app_name="03_Incremental_Merge")
    manager = SilverManager(spark)
    base_data_dir = os.getenv("LAKEHOUSE_STORAGE_ROOT", "./data")
    silver_results_path = os.path.join(base_data_dir, "silver", "results")

    logger.info("Verifying initial Silver results table state...")
    initial_df = spark.read.format("delta").load(silver_results_path)
    initial_count = initial_df.count()
    first_row = initial_df.filter(col("result_id") == 25001).select("result_id", "points", "create_date", "update_date").collect()[0]
    logger.info(f"Before Merge: result_id=25001 points={first_row['points']} create_date={first_row['create_date']} update_date={first_row['update_date']}")

    # Simulate updated race results batch (e.g. post-race penalty or bonus point update)
    time.sleep(1) # Ensure timestamp increment
    updated_batch = initial_df.filter(col("result_id") == 25001).withColumn("points", lit(26.0)) # awarded fastest lap point
    # Recalculate hash
    from src.transformations.results import transform_results
    # Apply merge
    logger.info("Executing Delta MERGE with updated point value for result_id=25001...")
    manager.merge_fact_table("results", updated_batch, ["result_id"], partition_by="race_id")

    after_df = spark.read.format("delta").load(silver_results_path)
    updated_row = after_df.filter(col("result_id") == 25001).select("result_id", "points", "create_date", "update_date").collect()[0]
    logger.info(f"After Merge: result_id=25001 points={updated_row['points']} create_date={updated_row['create_date']} update_date={updated_row['update_date']}")

    assert updated_row["points"] == 26.0, "Points should be updated to 26.0"
    assert updated_row["create_date"] == first_row["create_date"], "create_date must be preserved!"
    assert updated_row["update_date"] >= first_row["update_date"], "update_date must be updated!"
    logger.info("Incremental Delta MERGE verification PASSED with audit column preservation!")

if __name__ == "__main__":
    run_incremental_merge_demo()
