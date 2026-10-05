"""
Raw to Bronze Ingestion Engine.
Reads source datasets across all eight required formats (CSV, single-line JSON, nested JSON,
multi-line JSON, split CSVs, split multi-line JSONs), injects bronze audit metadata
(ingestion_date, input_file_name, source_system), and saves as Delta Lake tables.
"""

import os
from typing import Dict, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from src.utils.logger import get_logger

logger = get_logger("BronzeIngestion")

class BronzeIngestionManager:
    """
    Manages extraction from raw staging/sample files and ingestion into Bronze Delta Lake tables.
    """

    def __init__(self, spark: SparkSession, base_data_dir: Optional[str] = None):
        self.spark = spark
        self.base_data_dir = base_data_dir or os.getenv("LAKEHOUSE_STORAGE_ROOT", "./data")
        self.sample_dir = os.path.join(self.base_data_dir, "sample")
        self.raw_dir = os.path.join(self.base_data_dir, "raw")
        self.bronze_dir = os.path.join(self.base_data_dir, "bronze")
        os.makedirs(self.bronze_dir, exist_ok=True)

    def _resolve_source_path(self, dataset: str, default_filename: str) -> str:
        """
        Determines the source path, prioritizing data/raw/<dataset> if populated,
        otherwise falling back seamlessly to data/sample/<dataset>.
        """
        raw_path = os.path.join(self.raw_dir, dataset)
        if os.path.exists(raw_path) and os.listdir(raw_path):
            return raw_path
        
        sample_path = os.path.join(self.sample_dir, default_filename)
        if os.path.exists(sample_path):
            return sample_path
        
        raise FileNotFoundError(f"Neither raw path '{raw_path}' nor sample path '{sample_path}' found.")

    def _add_bronze_audit_columns(self, df: DataFrame, source_name: str) -> DataFrame:
        """
        Appends mandatory bronze metadata columns:
        - ingestion_date: Timestamp of ingestion execution
        - input_file_name: Physical provenance of data record
        - source_system: System identifier
        """
        return (
            df.withColumn("ingestion_date", current_timestamp())
              .withColumn("input_file_name", input_file_name())
              .withColumn("source_system", lit(source_name))
        )

    def ingest_circuits(self) -> DataFrame:
        """1. Circuits: CSV format"""
        source = self._resolve_source_path("circuits", "circuits.csv")
        logger.info(f"Ingesting circuits CSV from {source}")
        df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(source)
        )
        bronze_df = self._add_bronze_audit_columns(df, "Ergast_Circuits_CSV")
        target = os.path.join(self.bronze_dir, "circuits")
        bronze_df.write.format("delta").mode("overwrite").save(target)
        logger.info(f"Saved Bronze circuits Delta table to {target} (count: {bronze_df.count()})")
        return bronze_df

    def ingest_races(self) -> DataFrame:
        """2. Races: CSV format"""
        source = self._resolve_source_path("races", "races.csv")
        logger.info(f"Ingesting races CSV from {source}")
        df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(source)
        )
        bronze_df = self._add_bronze_audit_columns(df, "Ergast_Races_CSV")
        target = os.path.join(self.bronze_dir, "races")
        bronze_df.write.format("delta").mode("overwrite").save(target)
        logger.info(f"Saved Bronze races Delta table to {target} (count: {bronze_df.count()})")
        return bronze_df

    def ingest_constructors(self) -> DataFrame:
        """3. Constructors: single-line JSON"""
        source = self._resolve_source_path("constructors", "constructors.json")
        logger.info(f"Ingesting constructors single-line JSON from {source}")
        df = (
            self.spark.read.format("json")
            .option("multiline", "false")
            .load(source)
        )
        bronze_df = self._add_bronze_audit_columns(df, "Ergast_Constructors_JSON")
        target = os.path.join(self.bronze_dir, "constructors")
        bronze_df.write.format("delta").mode("overwrite").save(target)
        logger.info(f"Saved Bronze constructors Delta table to {target} (count: {bronze_df.count()})")
        return bronze_df

    def ingest_drivers(self) -> DataFrame:
        """4. Drivers: nested JSON"""
        source = self._resolve_source_path("drivers", "drivers.json")
        logger.info(f"Ingesting drivers nested JSON from {source}")
        df = (
            self.spark.read.format("json")
            .option("multiline", "false")
            .load(source)
        )
        bronze_df = self._add_bronze_audit_columns(df, "Ergast_Drivers_Nested_JSON")
        target = os.path.join(self.bronze_dir, "drivers")
        bronze_df.write.format("delta").mode("overwrite").save(target)
        logger.info(f"Saved Bronze drivers Delta table to {target} (count: {bronze_df.count()})")
        return bronze_df

    def ingest_results(self) -> DataFrame:
        """5. Results: single-line JSON (FACT)"""
        source = self._resolve_source_path("results", "results.json")
        logger.info(f"Ingesting results single-line JSON from {source}")
        df = (
            self.spark.read.format("json")
            .option("multiline", "false")
            .load(source)
        )
        bronze_df = self._add_bronze_audit_columns(df, "Ergast_Results_JSON")
        target = os.path.join(self.bronze_dir, "results")
        bronze_df.write.format("delta").mode("overwrite").save(target)
        logger.info(f"Saved Bronze results Delta table to {target} (count: {bronze_df.count()})")
        return bronze_df

    def ingest_pitstops(self) -> DataFrame:
        """6. Pitstops: multi-line JSON (FACT)"""
        source = self._resolve_source_path("pitstops", "pit_stops.json")
        logger.info(f"Ingesting pitstops multi-line JSON from {source}")
        df = (
            self.spark.read.format("json")
            .option("multiline", "true")
            .load(source)
        )
        bronze_df = self._add_bronze_audit_columns(df, "Ergast_Pitstops_MultiLine_JSON")
        target = os.path.join(self.bronze_dir, "pitstops")
        bronze_df.write.format("delta").mode("overwrite").save(target)
        logger.info(f"Saved Bronze pitstops Delta table to {target} (count: {bronze_df.count()})")
        return bronze_df

    def ingest_laptimes(self) -> DataFrame:
        """7. Laptimes: split CSV files (FACT)"""
        source = self._resolve_source_path("laptimes", "lap_times")
        logger.info(f"Ingesting split CSV laptimes from {source}")
        df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(os.path.join(source, "*.csv"))
        )
        bronze_df = self._add_bronze_audit_columns(df, "Ergast_Laptimes_Split_CSV")
        target = os.path.join(self.bronze_dir, "laptimes")
        bronze_df.write.format("delta").mode("overwrite").save(target)
        logger.info(f"Saved Bronze laptimes Delta table to {target} (count: {bronze_df.count()})")
        return bronze_df

    def ingest_qualifying(self) -> DataFrame:
        """8. Qualifying: split multi-line JSON files (FACT)"""
        source = self._resolve_source_path("qualifying", "qualifying")
        logger.info(f"Ingesting split multi-line qualifying JSON from {source}")
        df = (
            self.spark.read.format("json")
            .option("multiline", "true")
            .load(os.path.join(source, "*.json"))
        )
        bronze_df = self._add_bronze_audit_columns(df, "Ergast_Qualifying_Split_JSON")
        target = os.path.join(self.bronze_dir, "qualifying")
        bronze_df.write.format("delta").mode("overwrite").save(target)
        logger.info(f"Saved Bronze qualifying Delta table to {target} (count: {bronze_df.count()})")
        return bronze_df

    def ingest_all(self) -> Dict[str, int]:
        """Runs full bronze ingestion for all eight datasets."""
        logger.info("Executing comprehensive Bronze Lakehouse ingestion...")
        counts = {
            "circuits": self.ingest_circuits().count(),
            "races": self.ingest_races().count(),
            "constructors": self.ingest_constructors().count(),
            "drivers": self.ingest_drivers().count(),
            "results": self.ingest_results().count(),
            "pitstops": self.ingest_pitstops().count(),
            "laptimes": self.ingest_laptimes().count(),
            "qualifying": self.ingest_qualifying().count(),
        }
        logger.info(f"Bronze ingestion complete: {counts}")
        return counts
