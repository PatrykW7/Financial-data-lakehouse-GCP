from datetime import datetime
from zoneinfo import ZoneInfo
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


@dag(
    dag_id = "my_first_dag",
    start_date = datetime(2026, 9, 14, tzinfo = ZoneInfo("Europe/Warsaw")),
    schedule = None,
    catchup = False
)


def my_first_dag():
    dataproc_run = DataprocCreateBatchOperator(
        task_id = "first_dataproc_use",
        project_id = "gcp-pde-498614",
        region = "asia-east1",
        gcp_conn_id = "google_cloud_default",
        batch = {
            "pyspark_batch": {
                "main_python_file_uri" : "gs://project-dev-storage/jobs/silver_layer_spark_test.py"
            }
        },
        batch_id = "silver-test"
    )



dag = my_first_dag()

