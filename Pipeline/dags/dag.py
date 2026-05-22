from datetime import datetime, timedelta

from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # pyright: ignore[reportMissingImports]

import json
from  pathlib import Path

import logging

CONFIG_PATH = Path("/opt/airflow/dags/hdfs.json")

with open(CONFIG_PATH) as f:
    config = json.load(f)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="project1_pipeline",
    default_args=default_args,
    description="Pipeline Airflow con task Pyspark",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "pyspark", "project 1"],
) as dag:
    

    task_1 = SparkSubmitOperator(
        task_id="pre_processing",
        conn_id="spark_conn",
        application="/opt/airflow/jobs/pre-processing.py",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client"
        },
        application_args=[
            config["host"],
            config["port"],
            config["directory"],
            config["RDD_Dataframe"]
        ]
    )

    task_2 = SparkSubmitOperator(
        task_id="processing",
        conn_id="spark_conn",
        application="/opt/airflow/jobs/processing.py",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client"
        },
        application_args=[
            config["host"],
            config["port"],
            config["directory"],
            config["RDD_Dataframe"]
        ]
    )

    task_1 >> task_2