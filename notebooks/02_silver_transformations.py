"""
# Databricks / Local Lakehouse Notebook: 02_silver_transformations
# Description: Cleans, structures, enriches, and validates schemas from Bronze into Silver Delta tables.
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.spark import get_spark_session
from src.utils.logger import get_logger
from src.silver.merge_manager import SilverManager

logger = get_logger("Notebook_02_Silver")

def run_silver_transformations():
    spark = get_spark_session(app_name="02_Silver_Transformations")
    try:
        manager = SilverManager(spark)
        counts = manager.process_all_silver()
        logger.info("Successfully completed Silver Transformations stage.")
        return counts
    finally:
        pass

if __name__ == "__main__":
    run_silver_transformations()
