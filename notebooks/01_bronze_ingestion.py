"""
# Databricks / Local Lakehouse Notebook: 01_bronze_ingestion
# Description: Ingests raw F1 data across 8 distinct formats into Bronze Delta Lake tables.
"""

import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.spark import get_spark_session
from src.utils.logger import get_logger
from src.ingestion.file_ingestion import BronzeIngestionManager

logger = get_logger("Notebook_01_Bronze")

def run_bronze_ingestion():
    spark = get_spark_session(app_name="01_Bronze_Ingestion")
    try:
        manager = BronzeIngestionManager(spark)
        counts = manager.ingest_all()
        logger.info("Successfully completed Bronze Ingestion stage.")
        return counts
    finally:
        pass

if __name__ == "__main__":
    run_bronze_ingestion()
