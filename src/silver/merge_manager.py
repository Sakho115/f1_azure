"""
Delta Lake Silver Layer Merge and Upsert Manager.
Implements robust Delta MERGE logic for incremental fact tables and full/SCD-1 loading
for dimension tables, maintaining create_date and update_date audit columns.
"""

import os
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
from src.utils.logger import get_logger
from src.transformations.circuits import transform_circuits
from src.transformations.races import transform_races
from src.transformations.constructors import transform_constructors
from src.transformations.drivers import transform_drivers
from src.transformations.results import transform_results
from src.transformations.pitstops import transform_pitstops
from src.transformations.laptimes import transform_laptimes
from src.transformations.qualifying import transform_qualifying

logger = get_logger("SilverMergeManager")

class SilverManager:
    """
    Manages transformations from Bronze Delta to Silver Delta tables,
    applying Delta MERGE upserts with audit columns and partitioning.
    """

    def __init__(self, spark: SparkSession, base_data_dir: Optional[str] = None):
        self.spark = spark
        self.base_data_dir = base_data_dir or os.getenv("LAKEHOUSE_STORAGE_ROOT", "./data")
        self.bronze_dir = os.path.join(self.base_data_dir, "bronze")
        self.silver_dir = os.path.join(self.base_data_dir, "silver")
        os.makedirs(self.silver_dir, exist_ok=True)

    def load_dimension(
        self,
        table_name: str,
        staging_df: DataFrame,
        primary_keys: List[str]
    ) -> None:
        """
        Loads a dimension table using SCD Type 1 Delta MERGE or initial write.
        Preserves original create_date upon update and updates update_date.
        """
        target_path = os.path.join(self.silver_dir, table_name)
        now_ts = current_timestamp()

        # Add audit columns if not present
        if "create_date" not in staging_df.columns:
            staging_df = staging_df.withColumn("create_date", now_ts)
        if "update_date" not in staging_df.columns:
            staging_df = staging_df.withColumn("update_date", now_ts)

        if not DeltaTable.isDeltaTable(self.spark, target_path):
            logger.info(f"Target dimension {table_name} does not exist. Performing initial Delta write to {target_path}")
            staging_df.write.format("delta").mode("overwrite").save(target_path)
            return

        logger.info(f"Executing Delta MERGE for dimension: {table_name}")
        target_delta = DeltaTable.forPath(self.spark, target_path)
        
        # Build join condition
        match_cond = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])

        # Exclude create_date from updates to preserve original insertion audit
        update_cols = {c: f"source.{c}" for c in staging_df.columns if c not in ["create_date", "update_date"]}
        update_cols["update_date"] = "current_timestamp()"

        target_delta.alias("target").merge(
            source=staging_df.alias("source"),
            condition=match_cond
        ).whenMatchedUpdate(
            set=update_cols
        ).whenNotMatchedInsertAll().execute()

        logger.info(f"Dimension {table_name} MERGE complete.")

    def merge_fact_table(
        self,
        table_name: str,
        staging_df: DataFrame,
        primary_keys: List[str],
        partition_by: Optional[str] = "race_id"
    ) -> None:
        """
        Incrementally merges fact records into the Silver Delta table.
        Uses record_hash to conditionally update only modified rows.
        Preserves create_date on updates and updates update_date.
        """
        target_path = os.path.join(self.silver_dir, table_name)
        now_ts = current_timestamp()

        if "create_date" not in staging_df.columns:
            staging_df = staging_df.withColumn("create_date", now_ts)
        if "update_date" not in staging_df.columns:
            staging_df = staging_df.withColumn("update_date", now_ts)

        if not DeltaTable.isDeltaTable(self.spark, target_path):
            logger.info(f"Target fact {table_name} does not exist. Initializing partitioned Delta table at {target_path}")
            writer = staging_df.write.format("delta").mode("overwrite")
            if partition_by and partition_by in staging_df.columns:
                writer = writer.partitionBy(partition_by)
            writer.save(target_path)
            return

        logger.info(f"Performing incremental Delta MERGE on fact table: {table_name}")
        target_delta = DeltaTable.forPath(self.spark, target_path)

        match_cond = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        
        # Update set excluding create_date
        update_set = {c: f"source.{c}" for c in staging_df.columns if c not in ["create_date", "update_date"]}
        update_set["update_date"] = "current_timestamp()"

        # Conditional update only if hash differs
        target_delta.alias("target").merge(
            source=staging_df.alias("source"),
            condition=match_cond
        ).whenMatchedUpdate(
            condition="target.record_hash != source.record_hash",
            set=update_set
        ).whenNotMatchedInsertAll().execute()

        logger.info(f"Fact table {table_name} MERGE complete.")

    def process_all_silver(self) -> Dict[str, int]:
        """
        Runs full transformation and merge for all 8 Silver tables.
        """
        logger.info("Executing comprehensive Bronze -> Silver lakehouse processing...")
        results = {}

        # 1. Circuits Dimension
        bronze_circuits = self.spark.read.format("delta").load(os.path.join(self.bronze_dir, "circuits"))
        silver_circuits = transform_circuits(bronze_circuits)
        self.load_dimension("circuits", silver_circuits, ["circuit_id"])
        results["circuits"] = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "circuits")).count()

        # 2. Races Dimension
        bronze_races = self.spark.read.format("delta").load(os.path.join(self.bronze_dir, "races"))
        silver_races = transform_races(bronze_races)
        self.load_dimension("races", silver_races, ["race_id"])
        results["races"] = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "races")).count()

        # 3. Constructors Dimension
        bronze_constructors = self.spark.read.format("delta").load(os.path.join(self.bronze_dir, "constructors"))
        silver_constructors = transform_constructors(bronze_constructors)
        self.load_dimension("constructors", silver_constructors, ["constructor_id"])
        results["constructors"] = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "constructors")).count()

        # 4. Drivers Dimension
        bronze_drivers = self.spark.read.format("delta").load(os.path.join(self.bronze_dir, "drivers"))
        silver_drivers = transform_drivers(bronze_drivers)
        self.load_dimension("drivers", silver_drivers, ["driver_id"])
        results["drivers"] = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "drivers")).count()

        # 5. Results Fact
        bronze_results = self.spark.read.format("delta").load(os.path.join(self.bronze_dir, "results"))
        silver_results = transform_results(bronze_results)
        self.merge_fact_table("results", silver_results, ["result_id"], partition_by="race_id")
        results["results"] = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "results")).count()

        # 6. Pitstops Fact
        bronze_pitstops = self.spark.read.format("delta").load(os.path.join(self.bronze_dir, "pitstops"))
        silver_pitstops = transform_pitstops(bronze_pitstops)
        self.merge_fact_table("pitstops", silver_pitstops, ["race_id", "driver_id", "stop"], partition_by="race_id")
        results["pitstops"] = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "pitstops")).count()

        # 7. Laptimes Fact
        bronze_laptimes = self.spark.read.format("delta").load(os.path.join(self.bronze_dir, "laptimes"))
        silver_laptimes = transform_laptimes(bronze_laptimes)
        self.merge_fact_table("laptimes", silver_laptimes, ["race_id", "driver_id", "lap"], partition_by="race_id")
        results["laptimes"] = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "laptimes")).count()

        # 8. Qualifying Fact
        bronze_qualifying = self.spark.read.format("delta").load(os.path.join(self.bronze_dir, "qualifying"))
        silver_qualifying = transform_qualifying(bronze_qualifying)
        self.merge_fact_table("qualifying", silver_qualifying, ["qualify_id"], partition_by="race_id")
        results["qualifying"] = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "qualifying")).count()

        logger.info(f"Silver processing complete: {results}")
        return results
