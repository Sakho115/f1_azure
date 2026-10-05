"""
Formula 1 Lakehouse Pipeline - Apache Airflow DAG.
Orchestrates Bronze Ingestion, Silver Transformations, Incremental Delta Lake MERGE,
Gold Analytical Mart Generation, and Data Quality Validation.
Schedules weekly post-race execution (Sunday 22:00 UTC).
"""

from datetime import datetime, timedelta
import os
import sys

# Airflow imports with safe fallback for static analysis/linting
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator, BranchPythonOperator
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
except ImportError:
    # Allow import without airflow package installed
    DAG = object
    PythonOperator = object
    BranchPythonOperator = object
    BashOperator = object
    EmptyOperator = object

default_args = {
    "owner": "f1_data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

if DAG is not object:
    dag = DAG(
        dag_id="formula1_lakehouse_pipeline",
        default_args=default_args,
        description="End-to-end Medallion Lakehouse ETL Pipeline for Formula 1 Racing",
        schedule_interval="0 22 * * 0",  # Every Sunday evening post-race
        catchup=False,
        tags=["formula1", "lakehouse", "delta", "pyspark", "medallion"],
    )

    def check_race_calendar(**context):
        """
        Conditional check to verify if a Grand Prix occurred during the scheduled weekend window.
        """
        # In production, queries the race calendar table or Ergast API
        # Defaults to executing pipeline
        return "ingest_bronze_layer"

    start_task = EmptyOperator(task_id="start_pipeline", dag=dag)

    race_sensor = BranchPythonOperator(
        task_id="check_race_weekend_sensor",
        python_callable=check_race_calendar,
        provide_context=True,
        dag=dag,
    )

    ingest_bronze = BashOperator(
        task_id="ingest_bronze_layer",
        bash_command="python -m orchestration.pipeline --stage bronze",
        dag=dag,
    )

    transform_silver = BashOperator(
        task_id="transform_and_merge_silver_layer",
        bash_command="python -m orchestration.pipeline --stage silver",
        dag=dag,
    )

    build_gold = BashOperator(
        task_id="build_gold_analytical_marts",
        bash_command="python -m orchestration.pipeline --stage gold",
        dag=dag,
    )

    validate_quality = BashOperator(
        task_id="validate_data_quality",
        bash_command="python -m orchestration.pipeline --stage quality",
        dag=dag,
    )

    end_task = EmptyOperator(task_id="pipeline_complete", dag=dag)

    # Workflow DAG task dependencies
    start_task >> race_sensor >> ingest_bronze >> transform_silver >> build_gold >> validate_quality >> end_task
