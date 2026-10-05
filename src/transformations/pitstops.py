"""
Silver Layer Transformation for Pit Stops Fact Table.
Standardizes schema, casts timing metrics, computes record hash, and adds audit columns.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, coalesce, lit, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType, StringType

def transform_pitstops(bronze_df: DataFrame) -> DataFrame:
    """
    Transforms Bronze Pit Stops DataFrame into Silver format.
    Computes record_hash for incremental upserts.
    """
    cleaned_df = (
        bronze_df
        .select(
            col("raceId").cast(IntegerType()).alias("race_id"),
            col("driverId").cast(IntegerType()).alias("driver_id"),
            col("stop").cast(IntegerType()).alias("stop"),
            col("lap").cast(IntegerType()).alias("lap"),
            col("time").cast(StringType()).alias("time"),
            col("duration").cast(StringType()).alias("duration"),
            col("milliseconds").cast(IntegerType()).alias("milliseconds")
        )
    )

    hashed_df = cleaned_df.withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                coalesce(col("duration"), lit("")),
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
