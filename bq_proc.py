import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    )

@dag(
    dag_id = "bg_test",
    schedule = None,
    start_date = pendulum.datetime(2026, 11, 5, tz = "Europe/Warsaw"),
    catchup = False
)


def airflow_learn():

    def wykonaj_proc():
        process = BigQueryInsertJobOperator(
            task_id = "bq_4",
            gcp_conn_id="google_cloud_default",
            project_id = "gcp-pde-498614",
            configuration={
            "query": {
                "query": """
                        CALL `gcp-pde-498614.raw.insert_dimCompany`(
                            'gs://project-dev-storage/silver/company_facts/2026-11-01');

                    """,
                "useLegacySql": False,
            }
        },
            location = "europe-central2"
        )

        return process
    wykonaj_proc()

airflow_learn()
