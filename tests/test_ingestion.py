"""
Unit and integration tests for Bronze Lakehouse Ingestion Layer.
Validates multi-format ingestion (CSV, single-line JSON, nested JSON, multi-line JSON, split files)
and verifies audit metadata columns.
"""

import os
import pytest
from src.ingestion.file_ingestion import BronzeIngestionManager

def test_bronze_ingestion_formats(spark_session, tmp_path):
    # Initialize ingestion manager with project base data path
    manager = BronzeIngestionManager(spark_session)
    counts = manager.ingest_all()

    # Assert all 8 datasets were successfully ingested
    assert counts["circuits"] > 0, "Circuits CSV should contain records"
    assert counts["races"] > 0, "Races CSV should contain records"
    assert counts["constructors"] > 0, "Constructors single-line JSON should contain records"
    assert counts["drivers"] > 0, "Drivers nested JSON should contain records"
    assert counts["results"] > 0, "Results single-line JSON should contain records"
    assert counts["pitstops"] > 0, "Pitstops multi-line JSON should contain records"
    assert counts["laptimes"] > 0, "Laptimes split CSV should contain records"
    assert counts["qualifying"] > 0, "Qualifying split multi-line JSON should contain records"

    # Verify audit metadata injection in Bronze Delta tables
    bronze_circuits = spark_session.read.format("delta").load(os.path.join(manager.bronze_dir, "circuits"))
    assert "ingestion_date" in bronze_circuits.columns
    assert "input_file_name" in bronze_circuits.columns
    assert "source_system" in bronze_circuits.columns
    assert bronze_circuits.filter("ingestion_date IS NULL").count() == 0
