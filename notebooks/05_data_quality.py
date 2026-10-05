"""
# Databricks / Local Lakehouse Notebook: 05_data_quality
# Description: Executes automated Great Expectations style data quality test suites.
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.spark import get_spark_session
from src.utils.logger import get_logger
from src.quality.expectations import DataQualityValidator

logger = get_logger("Notebook_05_DataQuality")

def run_data_quality_suite():
    spark = get_spark_session(app_name="05_Data_Quality")
    try:
        validator = DataQualityValidator(spark)
        report = validator.run_full_suite()
        logger.info(f"Data Quality Report Summary: {report['summary']}")
        return report
    finally:
        pass

if __name__ == "__main__":
    run_data_quality_suite()
