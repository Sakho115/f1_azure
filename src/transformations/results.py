"""
Silver Layer Transformation for Race Results Fact Table.
Renames columns, casts data types, computes record checksum hash for Delta MERGE change detection,
and formats timing fields.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, coalesce, lit, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType, StringType

def transform_results(bronze_df: DataFrame) -> DataFrame:
    """
    Transforms Bronze Results DataFrame into Silver format.
    Computes deterministic record_hash for incremental upsert detection.
    """
    cleaned_df = (
        bronze_df
        .select(
            col("resultId").cast(IntegerType()).alias("result_id"),
            col("raceId").cast(IntegerType()).alias("race_id"),
            col("driverId").cast(IntegerType()).alias("driver_id"),
            col("constructorId").cast(IntegerType()).alias("constructor_id"),
            col("number").cast(IntegerType()).alias("number"),
            col("grid").cast(IntegerType()).alias("grid"),
            col("position").cast(IntegerType()).alias("position"),
            col("positionText").cast(StringType()).alias("position_text"),
            col("positionOrder").cast(IntegerType()).alias("position_order"),
            col("points").cast(DoubleType()).alias("points"),
            col("laps").cast(IntegerType()).alias("laps"),
            col("time").cast(StringType()).alias("time"),
            col("milliseconds").cast(IntegerType()).alias("milliseconds"),
            col("fastestLap").cast(IntegerType()).alias("fastest_lap"),
            col("rank").cast(IntegerType()).alias("rank"),
            col("fastestLapTime").cast(StringType()).alias("fastest_lap_time"),
            col("fastestLapSpeed").cast(DoubleType()).alias("fastest_lap_speed"),
            col("statusId").cast(IntegerType()).alias("status_id")
        )
    )

    # Compute deterministic MD5/SHA256 record hash for change detection
    hashed_df = cleaned_df.withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                coalesce(col("position").cast(StringType()), lit("")),
                coalesce(col("points").cast(StringType()), lit("")),
                coalesce(col("laps").cast(StringType()), lit("")),
                coalesce(col("milliseconds").cast(StringType()), lit("")),
                coalesce(col("fastest_lap").cast(StringType()), lit("")),
                coalesce(col("rank").cast(StringType()), lit(""))
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
