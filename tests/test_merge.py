"""
Unit and integration tests for Delta Lake MERGE and Upsert operations.
Verifies SCD Type 1 dimension updates and incremental fact table merges with audit columns.
"""

import os
import time
import pytest
from pyspark.sql.functions import col, lit, current_timestamp
from src.silver.merge_manager import SilverManager

def test_incremental_fact_merge_and_audit_columns(spark_session, tmp_path):
    manager = SilverManager(spark_session, base_data_dir=str(tmp_path))

    # Initial batch: 2 results
    initial_data = [
        (101, 1, 1, 1, 44, 1, 1, "1", 1, 25.0, 50, "1:30:00", 5400000, 40, 1, "1:32.0", 210.0, 1, "hash_v1"),
        (102, 1, 2, 2, 33, 2, 2, "2", 2, 18.0, 50, "1:30:05", 5405000, 42, 2, "1:32.5", 209.5, 1, "hash_v1"),
    ]
    cols = ["result_id", "race_id", "driver_id", "constructor_id", "number", "grid", "position",
            "position_text", "position_order", "points", "laps", "time", "milliseconds",
            "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "status_id", "record_hash"]
    
    df1 = spark_session.createDataFrame(initial_data, cols)
    manager.merge_fact_table("results", df1, ["result_id"], partition_by="race_id")

    silver_path = os.path.join(str(tmp_path), "silver", "results")
    read_df1 = spark_session.read.format("delta").load(silver_path)
    assert read_df1.count() == 2

    row1_initial = read_df1.filter(col("result_id") == 101).collect()[0]
    initial_create_date = row1_initial["create_date"]
    initial_update_date = row1_initial["update_date"]

    time.sleep(1) # Ensure timestamp tick

    # Batch 2: Update row 101 (bonus point, points=26), leave 102 unchanged, insert new row 103
    updated_data = [
        (101, 1, 1, 1, 44, 1, 1, "1", 1, 26.0, 50, "1:30:00", 5400000, 40, 1, "1:32.0", 210.0, 1, "hash_v2"), # updated
        (102, 1, 2, 2, 33, 2, 2, "2", 2, 18.0, 50, "1:30:05", 5405000, 42, 2, "1:32.5", 209.5, 1, "hash_v1"), # unchanged
        (103, 1, 3, 3, 16, 3, 3, "3", 3, 15.0, 50, "1:30:10", 5410000, 45, 3, "1:33.0", 208.0, 1, "hash_v1"), # new insert
    ]
    df2 = spark_session.createDataFrame(updated_data, cols)
    manager.merge_fact_table("results", df2, ["result_id"], partition_by="race_id")

    read_df2 = spark_session.read.format("delta").load(silver_path)
    assert read_df2.count() == 3

    row101_after = read_df2.filter(col("result_id") == 101).collect()[0]
    assert row101_after["points"] == 26.0, "Points should be updated"
    assert row101_after["create_date"] == initial_create_date, "create_date must be preserved across updates!"
    assert row101_after["update_date"] > initial_update_date, "update_date must advance when row is updated!"

    row103 = read_df2.filter(col("result_id") == 103).collect()[0]
    assert row103["points"] == 15.0
    assert row103["create_date"] is not None
