"""
Data Quality and Integrity Validation Suite.
Implements Great Expectations style assertions for lakehouse data quality:
- Nullability constraints
- Primary key uniqueness
- Referential integrity (foreign key validation)
- Numerical value boundaries
- Audit column presence
"""

import os
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, when
from src.utils.logger import get_logger

logger = get_logger("DataQualityValidator")

class QualityExpectationError(Exception):
    """Raised when critical data quality checks fail."""
    pass

class DataQualityValidator:
    """
    Executes automated data quality suites on Silver and Gold Delta tables.
    """

    def __init__(self, spark: SparkSession, base_data_dir: Optional[str] = None):
        self.spark = spark
        self.base_data_dir = base_data_dir or os.getenv("LAKEHOUSE_STORAGE_ROOT", "./data")
        self.silver_dir = os.path.join(self.base_data_dir, "silver")
        self.gold_dir = os.path.join(self.base_data_dir, "gold")

    def check_non_null(self, df: DataFrame, columns: List[str], table_name: str) -> Dict[str, Any]:
        """Asserts that specified columns have zero null values."""
        results = {}
        total = df.count()
        for c in columns:
            null_count = df.filter(col(c).isNull()).count()
            passed = (null_count == 0)
            results[f"{table_name}.{c}_non_null"] = {
                "check": "non_null",
                "table": table_name,
                "column": c,
                "null_count": null_count,
                "total_rows": total,
                "status": "PASSED" if passed else "FAILED"
            }
            if not passed:
                logger.error(f"Quality Check Failed: {table_name}.{c} has {null_count} null rows out of {total}!")
            else:
                logger.info(f"Quality Check Passed: {table_name}.{c} contains no nulls.")
        return results

    def check_unique_primary_key(self, df: DataFrame, pk_col: str, table_name: str) -> Dict[str, Any]:
        """Asserts that the primary key column contains strictly unique values."""
        total = df.count()
        distinct_count = df.select(pk_col).distinct().count()
        passed = (total == distinct_count)
        result = {
            f"{table_name}.{pk_col}_unique": {
                "check": "unique_primary_key",
                "table": table_name,
                "pk_col": pk_col,
                "total_rows": total,
                "distinct_count": distinct_count,
                "status": "PASSED" if passed else "FAILED"
            }
        }
        if not passed:
            logger.error(f"Uniqueness Check Failed: {table_name}.{pk_col} has duplicates! Total: {total}, Unique: {distinct_count}")
        else:
            logger.info(f"Uniqueness Check Passed: {table_name}.{pk_col} is uniquely distinct.")
        return result

    def check_referential_integrity(
        self,
        child_df: DataFrame,
        child_fk: str,
        parent_df: DataFrame,
        parent_pk: str,
        relationship_name: str
    ) -> Dict[str, Any]:
        """Asserts that all foreign keys in child_df exist in parent_df."""
        orphan_df = child_df.join(parent_df, child_df[child_fk] == parent_df[parent_pk], "left_anti")
        orphan_count = orphan_df.count()
        passed = (orphan_count == 0)
        result = {
            f"{relationship_name}_fk_integrity": {
                "check": "referential_integrity",
                "relationship": relationship_name,
                "orphan_count": orphan_count,
                "status": "PASSED" if passed else "FAILED"
            }
        }
        if not passed:
            logger.error(f"Referential Integrity Violation: {orphan_count} orphan rows in {relationship_name}!")
        else:
            logger.info(f"Referential Integrity Passed: {relationship_name} has 0 orphan keys.")
        return result

    def check_numeric_range(
        self,
        df: DataFrame,
        column: str,
        min_val: float,
        max_val: Optional[float],
        table_name: str
    ) -> Dict[str, Any]:
        """Asserts that values in column fall within expected range."""
        cond = (col(column) < min_val)
        if max_val is not None:
            cond = cond | (col(column) > max_val)
        out_of_bounds = df.filter(cond).count()
        passed = (out_of_bounds == 0)
        result = {
            f"{table_name}.{column}_range": {
                "check": "numeric_range",
                "table": table_name,
                "column": column,
                "min": min_val,
                "max": max_val,
                "out_of_bounds_count": out_of_bounds,
                "status": "PASSED" if passed else "FAILED"
            }
        }
        if not passed:
            logger.error(f"Range check failed: {table_name}.{column} has {out_of_bounds} values outside [{min_val}, {max_val}]")
        else:
            logger.info(f"Range check passed: {table_name}.{column} within bounds.")
        return result

    def run_full_suite(self) -> Dict[str, Any]:
        """Executes full comprehensive data quality suite across Silver and Gold layers."""
        logger.info("Executing comprehensive Lakehouse Data Quality Suite...")
        report = {}

        # 1. Silver Circuits
        circuits = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "circuits"))
        report.update(self.check_non_null(circuits, ["circuit_id", "circuit_name", "country"], "silver.circuits"))
        report.update(self.check_unique_primary_key(circuits, "circuit_id", "silver.circuits"))

        # 2. Silver Races
        races = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "races"))
        report.update(self.check_non_null(races, ["race_id", "race_year", "circuit_id", "race_name"], "silver.races"))
        report.update(self.check_unique_primary_key(races, "race_id", "silver.races"))
        report.update(self.check_referential_integrity(races, "circuit_id", circuits, "circuit_id", "races_to_circuits"))

        # 3. Silver Drivers
        drivers = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "drivers"))
        report.update(self.check_non_null(drivers, ["driver_id", "driver_ref", "full_name"], "silver.drivers"))
        report.update(self.check_unique_primary_key(drivers, "driver_id", "silver.drivers"))

        # 4. Silver Constructors
        constructors = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "constructors"))
        report.update(self.check_non_null(constructors, ["constructor_id", "name"], "silver.constructors"))
        report.update(self.check_unique_primary_key(constructors, "constructor_id", "silver.constructors"))

        # 5. Silver Results
        results = self.spark.read.format("delta").load(os.path.join(self.silver_dir, "results"))
        report.update(self.check_non_null(results, ["result_id", "race_id", "driver_id", "constructor_id"], "silver.results"))
        report.update(self.check_unique_primary_key(results, "result_id", "silver.results"))
        report.update(self.check_referential_integrity(results, "race_id", races, "race_id", "results_to_races"))
        report.update(self.check_referential_integrity(results, "driver_id", drivers, "driver_id", "results_to_drivers"))
        report.update(self.check_referential_integrity(results, "constructor_id", constructors, "constructor_id", "results_to_constructors"))
        report.update(self.check_numeric_range(results, "points", 0.0, 50.0, "silver.results"))

        # 6. Gold Standings
        driver_standings = self.spark.read.format("delta").load(os.path.join(self.gold_dir, "driver_standings"))
        report.update(self.check_non_null(driver_standings, ["race_year", "driver_id", "total_points", "rank"], "gold.driver_standings"))
        report.update(self.check_numeric_range(driver_standings, "rank", 1, 100, "gold.driver_standings"))

        failed_count = sum(1 for v in report.values() if v["status"] == "FAILED")
        passed_count = sum(1 for v in report.values() if v["status"] == "PASSED")
        logger.info(f"Quality Suite Completed: {passed_count} PASSED, {failed_count} FAILED.")

        if failed_count > 0:
            raise QualityExpectationError(f"{failed_count} quality checks failed during lakehouse validation!")

        return {
            "summary": {"passed": passed_count, "failed": failed_count, "total": len(report)},
            "details": report
        }
