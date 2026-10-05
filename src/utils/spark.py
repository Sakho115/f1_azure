"""
Unified Apache Spark and Delta Lake Session Factory.
Provides production-tuned SparkSession configurations for both Local Mode
and Azure Databricks deployments.
"""

import os
import sys
from typing import Optional
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.utils.logger import get_logger
from src.utils.secrets import SecretsManager

logger = get_logger("SparkSessionBuilder")

def get_spark_session(
    app_name: str = "Formula1Lakehouse",
    master: Optional[str] = None,
    env: Optional[str] = None
) -> SparkSession:
    """
    Initializes and configures a SparkSession enabled with Delta Lake support.
    
    Ensures identical Python worker/driver alignment to prevent version mismatches.
    Configures Delta Lake catalog, extensions, and schema evolution.
    """
    # Enforce current Python binary for worker and driver execution
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    env = env or os.getenv("PIPELINE_ENV", "local").lower()
    master = master or os.getenv("SPARK_MASTER", "local[*]")

    logger.info(f"Initializing SparkSession: app_name='{app_name}', master='{master}', env='{env}'")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # Delta Lake Core Configurations
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Delta schema evolution and merge optimizations
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        # Local execution resource tuning
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )

    # Configure ADLS Gen2 Service Principal Auth if running in Azure mode
    if env == "azure":
        secrets = SecretsManager(env="azure")
        adls_configs = secrets.get_adls_spark_configs()
        for k, v in adls_configs.items():
            builder = builder.config(k, v)

    # Use configure_spark_with_delta_pip to ensure delta jars are resolved
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    logger.info(f"SparkSession successfully initialized (Spark version {spark.version})")
    return spark
