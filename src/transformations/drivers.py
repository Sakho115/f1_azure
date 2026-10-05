"""
Silver Layer Transformation for Drivers Dimension.
Flattens nested JSON structure (name.forename, name.surname), creates concatenated full_name,
and casts date of birth.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, to_date, current_timestamp
from pyspark.sql.types import IntegerType, StringType, DateType

def transform_drivers(bronze_df: DataFrame) -> DataFrame:
    """
    Transforms Bronze Drivers DataFrame into Silver format.
    Handles nested name struct and date formatting.
    """
    return (
        bronze_df
        .withColumn("forename", col("name.forename"))
        .withColumn("surname", col("name.surname"))
        .withColumn("full_name", concat(col("name.forename"), lit(" "), col("name.surname")))
        .withColumn("dob", to_date(col("dob"), "yyyy-MM-dd"))
        .select(
            col("driverId").cast(IntegerType()).alias("driver_id"),
            col("driverRef").cast(StringType()).alias("driver_ref"),
            col("number").cast(IntegerType()).alias("number"),
            col("code").cast(StringType()).alias("code"),
            col("forename").cast(StringType()).alias("forename"),
            col("surname").cast(StringType()).alias("surname"),
            col("full_name").cast(StringType()).alias("full_name"),
            col("dob").cast(DateType()).alias("dob"),
            col("nationality").cast(StringType()).alias("nationality"),
            col("url").cast(StringType()).alias("url")
        )
        .withColumn("ingestion_date", current_timestamp())
    )
