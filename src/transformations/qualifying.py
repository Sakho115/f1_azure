"""
Silver Layer Transformation for Qualifying Fact Table.
Processes split multi-line qualifying JSON files, standardizes driver/constructor keys,
and computes change detection hashes.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, coalesce, lit, current_timestamp
from pyspark.sql.types import IntegerType, StringType

def transform_qualifying(bronze_df: DataFrame) -> DataFrame:
    """
    Transforms Bronze Qualifying DataFrame into Silver format.
    """
    cleaned_df = (
        bronze_df
        .select(
            col("qualifyId").cast(IntegerType()).alias("qualify_id"),
            col("raceId").cast(IntegerType()).alias("race_id"),
            col("driverId").cast(IntegerType()).alias("driver_id"),
            col("constructorId").cast(IntegerType()).alias("constructor_id"),
            col("number").cast(IntegerType()).alias("number"),
            col("position").cast(IntegerType()).alias("position"),
            col("q1").cast(StringType()).alias("q1"),
            col("q2").cast(StringType()).alias("q2"),
            col("q3").cast(StringType()).alias("q3")
        )
    )

    hashed_df = cleaned_df.withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                coalesce(col("position").cast(StringType()), lit("")),
                coalesce(col("q1"), lit("")),
                coalesce(col("q2"), lit("")),
                coalesce(col("q3"), lit(""))
            ),
            256
        )
    )

    return (
        hashed_df
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("create_date", current_timestamp())
        .withColumn("update_date", current_timestamp())
    )
