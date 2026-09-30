import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
        DataprocCreateBatchOperator,
)
from google.cloud import secretmanager


project_id = "gcp-pde-498614"
secret_region = "dataproc_region"
dataproc_account = "dataproc-service-acc"
dataproc_uri = 'dataproc-spark-uri'


client = secretmanager.SecretManagerServiceClient()


### DATAPROC REGION SECRET
secret_dataproc_region = (
    f"projects/{project_id}/"
    f"secrets/{secret_region}/"
    f"versions/latest"
)

response = client.access_secret_version(
    request = {"name": secret_dataproc_region}
)

dataproc_region = response.payload.data.decode("UTF-8")

###

### DATAPROC SERVICE ACCOUNT SECRET
secret_dataproc_service_acc = (
    f"projects/{project_id}/"
    f"secrets/{dataproc_account}/"
    f"versions/latest"
)

response = client.access_secret_version(
    request = {"name": secret_dataproc_service_acc}
)

dataproc_acc = response.payload.data.decode("UTF-8")



### DATAPROC SPARK URI
secret_dataproc_uri= (
    f"projects/{project_id}/"
    f"secrets/{dataproc_uri}/"
    f"versions/latest"
)

response = client.access_secret_version(
    request = {"name": secret_dataproc_uri}
)

dataproc_secret_uri = response.payload.data.decode("UTF-8")




@dag(
    dag_id = 'first_dag',
    start_date = pendulum.datetime(2026, 9, 28, tz = "Europe/Warsaw"),
    schedule = None,
    catchup = False
)


def airflow_learning():
    start = EmptyOperator(
        task_id = "start"
    )


    process = DataprocCreateBatchOperator(
        task_id = "process",
        project_id = project_id,
        region = dataproc_region,
        gcp_conn_id = "google_cloud_default",
        batch = {
            "pyspark_batch": {
                "main_python_file_uri" : dataproc_secret_uri,

                "args": [
                    "--processing-date", "{{ ds }}"
                ],
            },

            "environment_config": {
                "execution_config": {
                    "service_account": dataproc_acc,
                },
            },


            


            "runtime_config": {
                "version": "3.0",
                "properties": {
                    "spark.sql.extensions":
                       "io.delta.sql.DeltaSparkSessionExtension",          
                    "spark.sql.catalog.spark_catalog":
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    "spark.driver.cores": "4",
                    "spark.executor.cores": "4",
                    "spark.executor.instances": "1",
                    "spark.dynamicAllocation.enabled": "false"
                },


            },

        },

        batch_id = "test-dataproc-new7",
    )

    end = EmptyOperator(
        task_id = "end"
    )


    start >> process >> end

dag = airflow_learning()