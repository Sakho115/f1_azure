"""
Unit tests for Silver Layer Transformations.
Tests column standardization, timestamp concatenation in races, nested JSON flattening in drivers,
and SHA-256 change detection hashing.
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.transformations.circuits import transform_circuits
from src.transformations.races import transform_races
from src.transformations.drivers import transform_drivers
from src.transformations.results import transform_results

def test_transform_circuits(spark_session):
    data = [(1, "silverstone", "Silverstone Circuit", "Silverstone", "UK", 52.0786, -1.01694, 153, "http://...")]
    cols = ["circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt", "url"]
    df = spark_session.createDataFrame(data, cols)

    transformed = transform_circuits(df)
    assert "circuit_id" in transformed.columns
    assert "circuit_ref" in transformed.columns
    assert "circuit_name" in transformed.columns
    assert "latitude" in transformed.columns
    assert "longitude" in transformed.columns
    assert "altitude" in transformed.columns
    row = transformed.collect()[0]
    assert row["circuit_name"] == "Silverstone Circuit"
    assert row["latitude"] == 52.0786

def test_transform_races_timestamp_merge(spark_session):
    data = [(1052, 2021, 1, 3, "Bahrain Grand Prix", "2021-03-28", "15:00:00", "http://...")]
    cols = ["raceId", "year", "round", "circuitId", "name", "date", "time", "url"]
    df = spark_session.createDataFrame(data, cols)

    transformed = transform_races(df)
    assert "race_timestamp" in transformed.columns
    assert "race_year" in transformed.columns
    row = transformed.collect()[0]
    assert row["race_timestamp"].year == 2021
    assert row["race_timestamp"].month == 3
    assert row["race_timestamp"].day == 28
    assert row["race_year"] == 2021

def test_transform_drivers_nested_json(spark_session):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("driverId", IntegerType()),
        StructField("driverRef", StringType()),
        StructField("number", IntegerType()),
        StructField("code", StringType()),
        StructField("name", StructType([
            StructField("forename", StringType()),
            StructField("surname", StringType())
        ])),
        StructField("dob", StringType()),
        StructField("nationality", StringType()),
        StructField("url", StringType())
    ])
    data = [(1, "hamilton", 44, "HAM", ("Lewis", "Hamilton"), "1985-01-07", "British", "http://...")]
    df = spark_session.createDataFrame(data, schema)

    transformed = transform_drivers(df)
    assert "full_name" in transformed.columns
    assert "forename" in transformed.columns
    assert "surname" in transformed.columns
    assert "dob" in transformed.columns
    row = transformed.collect()[0]
    assert row["full_name"] == "Lewis Hamilton"
    assert str(row["dob"]) == "1985-01-07"

def test_transform_results_record_hash(spark_session):
    data = [(25001, 1052, 1, 131, 44, 2, 1, "1", 1, 25.0, 56, "1:32:03", 5523897, 44, 2, "1:34.015", 207.235, 1)]
    cols = ["resultId", "raceId", "driverId", "constructorId", "number", "grid", "position", "positionText",
            "positionOrder", "points", "laps", "time", "milliseconds", "fastestLap", "rank",
            "fastestLapTime", "fastestLapSpeed", "statusId"]
    df = spark_session.createDataFrame(data, cols)

    transformed = transform_results(df)
    assert "record_hash" in transformed.columns
    assert "create_date" in transformed.columns
    assert "update_date" in transformed.columns
    row = transformed.collect()[0]
    assert len(row["record_hash"]) == 64  # SHA-256 string length
