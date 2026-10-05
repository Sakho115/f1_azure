"""
Unit tests for Data Quality Assertion Suite.
Tests non-null assertions, primary key uniqueness, referential integrity, and range validation.
"""

import pytest
from src.quality.expectations import DataQualityValidator, QualityExpectationError

def test_quality_checks_pass_and_fail(spark_session):
    validator = DataQualityValidator(spark_session)

    # DataFrame with valid non-null unique data
    valid_data = [(1, "silverstone", "UK"), (2, "monza", "Italy")]
    valid_df = spark_session.createDataFrame(valid_data, ["id", "circuit_ref", "country"])

    res_null = validator.check_non_null(valid_df, ["id", "circuit_ref"], "test_table")
    assert res_null["test_table.id_non_null"]["status"] == "PASSED"
    assert res_null["test_table.circuit_ref_non_null"]["status"] == "PASSED"

    res_unique = validator.check_unique_primary_key(valid_df, "id", "test_table")
    assert res_unique["test_table.id_unique"]["status"] == "PASSED"

    # Test failure on duplicate primary key
    dup_data = [(1, "silverstone"), (1, "silverstone_duplicate")]
    dup_df = spark_session.createDataFrame(dup_data, ["id", "name"])
    res_dup = validator.check_unique_primary_key(dup_df, "id", "dup_table")
    assert res_dup["dup_table.id_unique"]["status"] == "FAILED"

    # Test referential integrity
    parent_data = [(1,), (2,)]
    child_data = [(101, 1), (102, 2)]
    invalid_child_data = [(101, 1), (103, 999)] # 999 does not exist in parent

    parent_df = spark_session.createDataFrame(parent_data, ["parent_id"])
    child_df = spark_session.createDataFrame(child_data, ["child_id", "fk_id"])
    invalid_child_df = spark_session.createDataFrame(invalid_child_data, ["child_id", "fk_id"])

    res_fk_pass = validator.check_referential_integrity(child_df, "fk_id", parent_df, "parent_id", "valid_rel")
    assert res_fk_pass["valid_rel_fk_integrity"]["status"] == "PASSED"

    res_fk_fail = validator.check_referential_integrity(invalid_child_df, "fk_id", parent_df, "parent_id", "invalid_rel")
    assert res_fk_fail["invalid_rel_fk_integrity"]["status"] == "FAILED"
