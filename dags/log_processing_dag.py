"""
log_processing_dag.py  —  Airflow 3 DAG: log_processing_pipeline

Orchestrates the full Spark-based web log processing pipeline.

Schedule  : @daily (midnight UTC)
Parameters: sample_mode (bool, default True)
             True  — verify raw data exists in HDFS via WebHDFS, then run Spark jobs.
             False — re-ingest from the local raw file before running Spark jobs.

Task chain (sequential):
  ingest_to_hdfs >> bronze_job >> silver_job >> gold_job >> export_to_local

Spark tasks use SparkSubmitOperator with conn_id="spark_default"
(spark://spark-master:7077, created by airflow-init).
spark-submit is resolved from $SPARK_HOME/bin/spark-submit, where SPARK_HOME
points to the pip-installed pyspark package inside the Airflow containers.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID  = "spark_default"
SPARK_APPS_DIR = "/opt/spark-apps"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="log_processing_pipeline",
    description="Nginx log pipeline: HDFS ingest → Bronze → Silver → Gold → Export",
    default_args=default_args,
    start_date=datetime(2019, 1, 22),
    schedule="@daily",
    catchup=False,
    tags=["spark", "hdfs", "parquet"],
    params={
        "sample_mode": Param(
            default=True,
            type="boolean",
            description=(
                "True (default): skip re-ingest, verify raw data already in HDFS. "
                "False: run 00_ingest_to_hdfs.py to (re-)upload the 3.3 GB log."
            ),
        )
    },
) as dag:

    def verify_or_ingest(**context):
        """
        Verify raw log exists in HDFS (sample_mode=True) or run the full
        ingest script (sample_mode=False).

        Uses the WebHDFS REST API instead of the hdfs CLI because the
        Airflow containers have only pip-installed PySpark — Hadoop CLI
        binaries are not available outside the HDFS containers.
        """
        import urllib.request
        import json
        import subprocess
        import sys

        sample_mode = context["params"]["sample_mode"]
        hdfs_url = (
            "http://hdfs-namenode:9870/webhdfs/v1"
            "/raw/access-logs/access.log"
            "?op=GETFILESTATUS&user.name=root"
        )

        if sample_mode:
            print("sample_mode=True: verifying raw log via WebHDFS...")
            req = urllib.request.Request(hdfs_url)
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode())
                    fs = data.get("FileStatus", {})
                    length = fs.get("length", 0)
                    ftype  = fs.get("type", "UNKNOWN")
                    print(f"OK — raw log found in HDFS: type={ftype}, size={length:,} bytes")
                    if ftype != "FILE":
                        raise ValueError(f"Expected FILE, got {ftype}")
                    if length < 1_000_000_000:
                        raise ValueError(f"File too small ({length:,} bytes) — may be incomplete")
            except urllib.error.HTTPError as e:
                raise RuntimeError(f"WebHDFS check failed HTTP {e.code}: {e.read().decode()}") from e
        else:
            print("sample_mode=False: running full ingest (may take several minutes)...")
            result = subprocess.run(
                [sys.executable, "/opt/spark-apps/00_ingest_to_hdfs.py"],
                capture_output=True, text=True
            )
            print(result.stdout)
            if result.returncode != 0:
                print(result.stderr, file=sys.stderr)
                raise RuntimeError("Ingest script failed")
            print("Ingest complete.")

    ingest_to_hdfs = PythonOperator(
        task_id="ingest_to_hdfs",
        python_callable=verify_or_ingest,
    )

    bronze_job = SparkSubmitOperator(
        task_id="bronze_job",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_APPS_DIR}/01_bronze.py",
        name="bronze_job",
        num_executors=1,
        executor_memory="2g",
        driver_memory="2g",
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://hdfs-namenode:9000",
            "spark.sql.shuffle.partitions": "16",
        },
    )

    silver_job = SparkSubmitOperator(
        task_id="silver_job",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_APPS_DIR}/02_silver.py",
        name="silver_job",
        num_executors=1,
        executor_memory="2g",
        driver_memory="2g",
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://hdfs-namenode:9000",
            "spark.sql.shuffle.partitions": "16",
        },
    )

    gold_job = SparkSubmitOperator(
        task_id="gold_job",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_APPS_DIR}/03_gold.py",
        name="gold_job",
        num_executors=1,
        executor_memory="2g",
        driver_memory="2g",
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://hdfs-namenode:9000",
            "spark.sql.shuffle.partitions": "16",
        },
    )

    export_to_local = SparkSubmitOperator(
        task_id="export_to_local",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_APPS_DIR}/04_export_to_local.py",
        name="export_to_local",
        num_executors=1,
        executor_memory="1g",
        driver_memory="1g",
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://hdfs-namenode:9000",
        },
    )

    ingest_to_hdfs >> bronze_job >> silver_job >> gold_job >> export_to_local
