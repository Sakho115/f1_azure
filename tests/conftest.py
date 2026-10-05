"""
Pytest configuration and shared fixtures for Formula 1 Lakehouse Pipeline.
"""

import os
import sys
import pytest
from pyspark.sql import SparkSession

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.spark import get_spark_session

@pytest.fixture(scope="session")
def spark_session():
    """
    Session-scoped PySpark fixture with Delta Lake extensions configured.
    """
    spark = get_spark_session(app_name="F1Lakehouse_Pytest", master="local[2]")
    yield spark
    # Note: We do not stop SparkSession here to allow fast test execution across modules.
