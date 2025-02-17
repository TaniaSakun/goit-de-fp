from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from pathlib import Path

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 17),
}

# Define connection and base path for Spark jobs
connection_id = "spark-default"
spark_jobs_dir = Path("dags/tet_s_spark_jobs")

# DAG Definition
with DAG(
    dag_id="final_project_tet_s",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["tet_s"],
    doc_md="""
    ## Final Project DAG
    This DAG processes data in three stages:
    - **Landing to Bronze**
    - **Bronze to Silver**
    - **Silver to Gold**
    
    Each step runs a PySpark job using `SparkSubmitOperator`.
    """,
) as dag:

    def create_spark_task(task_id, script_name):
        """Helper function to create SparkSubmitOperator tasks."""
        return SparkSubmitOperator(
            task_id=task_id,
            application=str(spark_jobs_dir / script_name),
            conn_id=connection_id,
            verbose=True,
        )

    # Define tasks
    landing_to_bronze = create_spark_task("landing_to_bronze", "landing_to_bronze.py")
    bronze_to_silver = create_spark_task("bronze_to_silver", "bronze_to_silver.py")
    silver_to_gold = create_spark_task("silver_to_gold", "silver_to_gold.py")

    # Set task dependencies
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
