"""
Silver Layer Transformation for Constructors Dimension.
Standardizes schema, drops redundant URL column if desired, renames columns to snake_case.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType

def transform_constructors(bronze_df: DataFrame) -> DataFrame:
    """
    Transforms Bronze Constructors DataFrame into Silver format.
    """
    return (
        bronze_df
        .select(
            col("constructorId").cast(IntegerType()).alias("constructor_id"),
            col("constructorRef").cast(StringType()).alias("constructor_ref"),
            col("name").cast(StringType()).alias("name"),
            col("nationality").cast(StringType()).alias("nationality"),
            col("url").cast(StringType()).alias("url")
        )
        .withColumn("ingestion_date", current_timestamp())
    )
