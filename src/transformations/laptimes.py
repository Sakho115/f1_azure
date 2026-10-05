"""
Silver Layer Transformation for Lap Times Fact Table.
Standardizes lap times across split CSV files, formats positions, and computes record hash.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, coalesce, lit, current_timestamp
from pyspark.sql.types import IntegerType, StringType

def transform_laptimes(bronze_df: DataFrame) -> DataFrame:
    """
    Transforms Bronze Lap Times DataFrame into Silver format.
    """
    cleaned_df = (
        bronze_df
        .select(
            col("raceId").cast(IntegerType()).alias("race_id"),
            col("driverId").cast(IntegerType()).alias("driver_id"),
            col("lap").cast(IntegerType()).alias("lap"),
            col("position").cast(IntegerType()).alias("position"),
            col("time").cast(StringType()).alias("time"),
            col("milliseconds").cast(IntegerType()).alias("milliseconds")
        )
    )

    hashed_df = cleaned_df.withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                coalesce(col("position").cast(StringType()), lit("")),
                coalesce(col("time"), lit("")),
                coalesce(col("milliseconds").cast(StringType()), lit(""))
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
