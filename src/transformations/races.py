"""
Silver Layer Transformation for Races Dimension.
Standardizes schema, unifies date and time strings into race_timestamp, and extracts race_year.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, to_timestamp, current_timestamp
from pyspark.sql.types import IntegerType, StringType

def transform_races(bronze_df: DataFrame) -> DataFrame:
    """
    Transforms Bronze Races DataFrame into Silver format.
    Combines date and time into a unified race_timestamp.
    """
    return (
        bronze_df
        .withColumn(
            "race_timestamp",
            to_timestamp(
                concat(col("date"), lit(" "), col("time")),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        .select(
            col("raceId").cast(IntegerType()).alias("race_id"),
            col("year").cast(IntegerType()).alias("race_year"),
            col("round").cast(IntegerType()).alias("round"),
            col("circuitId").cast(IntegerType()).alias("circuit_id"),
            col("name").cast(StringType()).alias("race_name"),
            col("date").cast(StringType()).alias("race_date"),
            col("time").cast(StringType()).alias("race_time"),
            col("race_timestamp"),
            col("url").cast(StringType()).alias("url")
        )
        .withColumn("ingestion_date", current_timestamp())
    )
