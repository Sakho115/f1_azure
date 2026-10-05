"""
Silver Layer Transformation for Circuits Dimension.
Cleans column names, casts coordinates and altitude, and standardizes schema.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType, StringType

def transform_circuits(bronze_df: DataFrame) -> DataFrame:
    """
    Transforms Bronze Circuits DataFrame into Silver format.
    """
    return (
        bronze_df
        .select(
            col("circuitId").cast(IntegerType()).alias("circuit_id"),
            col("circuitRef").cast(StringType()).alias("circuit_ref"),
            col("name").cast(StringType()).alias("circuit_name"),
            col("location").cast(StringType()).alias("location"),
            col("country").cast(StringType()).alias("country"),
            col("lat").cast(DoubleType()).alias("latitude"),
            col("lng").cast(DoubleType()).alias("longitude"),
            col("alt").cast(IntegerType()).alias("altitude"),
            col("url").cast(StringType()).alias("url")
        )
        .withColumn("ingestion_date", current_timestamp())
    )
