import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
        DataprocCreateBatchOperator,
)
from google.cloud import secretmanager
from bronze_data_ingestion import (
    extract_company_tickers,
    save_company_tickers,
    extract_company_forms,
    save_company_forms,
    extract_company_facts,
    save_company_facts,
    extract_alpha_vantage,
    save_alpha_vantage,
    save_dlq,
)

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
    start_date = pendulum.datetime(2026, 10, 3, tz = "Europe/Warsaw"),
    schedule = None,
    catchup = False
)


def airflow_learning():
    company_list_cik = ["0001045810"]

    processing_date = "{{ ds }}"
    
    start = EmptyOperator(
        task_id = "start"
    )

    @task
    def ingest_company_tickers(
        company_list_cik: list[str],
        processing_date: str
    ) -> list[str]:
        df_companyTickers, raw_tickers, tickers_list = extract_company_tickers(company_list_cik)
        save_company_tickers(df_companyTickers, raw_tickers, processing_date)

        return tickers_list


    tickers_list = ingest_company_tickers(company_list_cik, processing_date)


    @task
    def ingest_company_forms(
        company_list_cik: list[str],
        processing_date: str
    ) -> None:
        
        valid_data, invalid_data = extract_company_forms(company_list_cik)

        if valid_data:
            save_company_forms(valid_data, processing_date)

        if invalid_data:
            save_dlq(
                "company_forms",
                invalid_data,
                processing_date
            )
    
    ingest_c_forms = ingest_company_forms(company_list_cik, processing_date)


    @task
    def ingest_company_facts(
        company_list_cik: list[str],
        processing_date: str
    ) -> None:
        
        valid_data, invalid_data = extract_company_facts(company_list_cik)
        if valid_data:
            save_company_facts(valid_data, processing_date)


        if invalid_data:
            save_dlq("company_facts", invalid_data, processing_date)
        
    ingest_c_facts = ingest_company_facts(company_list_cik, processing_date)


    @task
    def ingest_alpha_vantage(
        tickers_list: list[str],
        processing_date: str
    ) -> None:
        
        valid_data, invalid_data  = extract_alpha_vantage(tickers_list)
        
        if valid_data:
            save_alpha_vantage(valid_data, processing_date)

        if invalid_data:
            save_dlq("alpha_vantage", invalid_data, processing_date)
    
    
    ingest_a_vantage = ingest_alpha_vantage(tickers_list, processing_date)

    process = DataprocCreateBatchOperator(
        task_id = "process",
        project_id = project_id,
        region = dataproc_region,
        gcp_conn_id = "google_cloud_default",
        batch = {
            "pyspark_batch": {
                "main_python_file_uri" : "gs://project-dev-storage/jobs/dataproc_learning.py",

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

        batch_id = "test-dataproc-new204",
    )

    end = EmptyOperator(
        task_id = "end"
    )


    start >> tickers_list
    start >> ingest_c_forms
    start >> ingest_c_facts

    ingest_a_vantage >> process
    ingest_c_forms >> process
    ingest_c_facts >> process

    process >> end

dag = airflow_learning()