import pandas as pd
import requests
#import datetime
#from zoneinfo import ZoneInfo
from google.cloud import storage
import json
from google.cloud import secretmanager
from deltalake import write_deltalake
#from airflow.decorators import dag, task
#import pendulum
import pyarrow as pa
import time
from pandas import DataFrame


# SECRETS API + EMAIL 

PROJECT_ID = "gcp-pde-498614"
SECRET_ID_key = "alpha-vantage-api-key"
SECRET_ID_email = "sec-email"
#storage_client = storage.Client()
#bucket = storage_client.get_bucket("project-dev-storage")





# second secret

#cik_1  = "0001045810"
#cik_2 = "0000320193"
#TICKER_1 = "NVDA"
#TICKER_2 = "AAPL"


 # COMPANIES THAT WE ARE LOOKING FOR TO ANALYZE
#company_list_cik = ["0001045810", "0000320193", "0001652044", "0000789019", "0001018724"]
#company_list_cik = ["0001045810"]

#start_time = datetime.datetime.now(ZoneInfo('Europe/Warsaw'))
#start_time = str(start_time).split(' ')[0].replace('-','_')





def extract_company_tickers(company_list_cik: list) -> tuple[DataFrame, dict, list]:
    client = secretmanager.SecretManagerServiceClient()
    
    secret_email_sec = (
    f"projects/{PROJECT_ID}/"
    f"secrets/{SECRET_ID_email}/"
    f"versions/latest"
    )

    response = client.access_secret_version(
        request = {"name": secret_email_sec}
    )


    API_email = response.payload.data.decode("UTF-8")
    headers = {'User-Agent': API_email}
    
    
    companyTickers = requests.get("https://www.sec.gov/files/company_tickers_exchange.json", headers = headers)
    raw_tickers = companyTickers.json()
    
    df_companyTickers = pd.DataFrame(raw_tickers["data"], columns = raw_tickers["fields"])
    df_companyTickers["cik"] = df_companyTickers["cik"].astype(str).str.zfill(10) # ADDING 0 TO LEFT SIZE TO HAVE 10 DIGITS
    
    df_companyTickers = df_companyTickers[df_companyTickers["cik"].isin(company_list_cik)]
    
    tickers_list = df_companyTickers["ticker"].tolist()
    
    return df_companyTickers, raw_tickers, tickers_list


### COMPANY TICKERS 
def save_company_tickers(
    df: DataFrame,
    js: dict,
    processing_date: str) -> None:
    # companyTickers wil be saved to two catalogs, staging + bronze
            # staged - filred rows
            # bronze - original data
        
    storage_client = storage.Client()
    bucket = storage_client.get_bucket("project-dev-storage")

    ### SAVING DF TO BRONZE LAYER, RAW DATA
    blob = bucket.blob(f"bronze/company_tickers/{processing_date}/companyTickers")
    blob.upload_from_string(json.dumps(js), content_type = "application/json")
    
    ### SAVING DF TO DELTA LAKE WITH APACHE ARROW, STAGING LAYER
    arrow_table = pa.Table.from_pandas(df,preserve_index=False)
    
    write_deltalake(f"gs://project-dev-storage/staging/company_tickers/companyTickers_stg_{processing_date}/", arrow_table, mode = "overwrite")



### COMPANY FORMS
def extract_company_forms(company_list_cik: list) -> tuple[list[dict], list[dict]]:
    # 2.1 CompanyForms Extract               
    #  Defined list of company names, based on company_list_cik

    client = secretmanager.SecretManagerServiceClient()

    secret_email_sec = (
    f"projects/{PROJECT_ID}/"
    f"secrets/{SECRET_ID_email}/"
    f"versions/latest"
    )

    response = client.access_secret_version(
        request = {"name": secret_email_sec}
    )


    API_email = response.payload.data.decode("UTF-8")
    headers = {'User-Agent': API_email}

    valid_data = []  
    invalid_data = []
    wyn = []
    for i in company_list_cik:
        fillingMetadata_i = requests.get(f'https://data.sec.gov/submissions/CIK{i}.json', headers = headers)
        #wyn.append(fillingMetadata_i.json())

        data = fillingMetadata_i.json()
        condition = (
            isinstance(data, dict) and
            isinstance(data.get("filings"), dict)
        )

        if condition == True:
            valid_data.append(data)

        else:
            invalid_data.append({
                "source": "company_forms",
                "cik": i,
                "reason_error": "INVALID_JSON_STRUCTURE",
                "row_data": data
                })


        time.sleep(2)

    return valid_data, invalid_data


def save_company_forms(
    wyn: list[dict],
    processing_date: str
    ) -> None:
    
    # 2.2 CompanyForms - Save
    # Saving in Raw format 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket("project-dev-storage")

    blob = bucket.blob(f"bronze/company_forms/{processing_date}/companyForms")
    blob.upload_from_string(json.dumps(wyn), content_type = "application/json")



### COMPANY FACTS
def extract_company_facts(company_list_cik: list) -> tuple[list[dict], list[dict]]:
    
    client = secretmanager.SecretManagerServiceClient()
    
    secret_email_sec = (
    f"projects/{PROJECT_ID}/"
    f"secrets/{SECRET_ID_email}/"
    f"versions/latest"
    )

    response = client.access_secret_version(
        request = {"name": secret_email_sec}
    )


    API_email = response.payload.data.decode("UTF-8")
    headers = {'User-Agent': API_email}
    
    
    valid_data = []
    invalid_data = []

    for i in company_list_cik:
        companyFacts_i = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{i}.json", headers = headers)
        data = companyFacts_i.json()

        condition = (
            isinstance(data, dict) and 
            isinstance(data.get("facts"), dict)
        )

        if condition:
            valid_data.append(data)


        else:
            invalid_data.append({
                "source": "company_facts",
                "cik": i,
                "reason_error": "INVALID_JSON_STRUCTURE",
                "raw_data": data
                })

        
    
        time.sleep(2)

    return valid_data, invalid_data




def save_company_facts(wyn: list[dict], processing_date: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.get_bucket("project-dev-storage")
    
    blob = bucket.blob(f"bronze/company_facts/{processing_date}/companyFacts")
    blob.upload_from_string(json.dumps(wyn), content_type = "application/json")
    


def save_dlq(
    source: str,
    invalid_data: list[dict],
    processing_date: str
    ) -> None:

    storage_client = storage.Client()
    bucket = storage_client.get_bucket("project-dev-storage")

    if invalid_data:
        blob = bucket.blob(f"dlq/ingestion/{source}/{processing_date}/invalid_data")
        blob.upload_from_string(json.dumps(invalid_data), content_type = "application/json")




### ALPHA VANTAGE 
def extract_alpha_vantage(tickers_list: list[str]) -> tuple[list[dict], list[dict]]:
    
    client = secretmanager.SecretManagerServiceClient()

    # first secret
    secret_name_api_key = (
        f"projects/{PROJECT_ID}/"
        f"secrets/{SECRET_ID_key}/"
        f"versions/latest"
    )

    response = client.access_secret_version(
        request = {"name": secret_name_api_key}
    )

    API_KEY = response.payload.data.decode("UTF-8")
    
    valid_data = []
    invalid_data = []
    for i in tickers_list:
        alpha_vantage_i = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={i}&outputsize=compact&apikey={API_KEY}")
        data = alpha_vantage_i.json()

        condition = (
                        isinstance(data, dict) 
                        and isinstance(data.get("Meta Data"), dict) 
                        and isinstance(data.get("Time Series (Daily)"), dict)
                    )

        if condition == True:
            valid_data.append(data)
        else:
            invalid_data.append({
                "source": "alpha_vantage",
                "cik": i,
                "reason_error": "INVALID_JSON_STRUCTURE",
                "raw_data": data})


        time.sleep(2)


    return valid_data, invalid_data 
        


def save_alpha_vantage(wyn: list[dict], processing_date: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.get_bucket("project-dev-storage")

    blob = bucket.blob(f"bronze/alpha_vantage/{processing_date}/alpha_vantage")
    blob.upload_from_string(json.dumps(wyn), content_type = "application/json")





'''
@dag(
    dag_id = 'bronze_layer_ingestion_new2',
    start_date = pendulum.datetime(2026, 8, 1, tz = "Europe/Warsaw"),
    schedule = None,
    catchup = False
)



def ingest_bronze_dag() -> None:

 
    @task
    def ingest_company_tickers(
        company_list_cik: list[str]
    ) -> list[str]:
        df_companyTickers, raw_tickers, tickers_list = extract_company_tickers(company_list_cik)
        save_company_tickers(df_companyTickers, raw_tickers)

        return tickers_list

    tickers_list = ingest_company_tickers(company_list_cik)



    @task
    def ingest_company_forms(
        company_list_cik: list[str]
    ) -> None:
        
        valid_data, invalid_data = extract_company_forms(company_list_cik)

        if valid_data:
            save_company_forms(valid_data)

        if invalid_data:
            save_dlq(
                "company_forms",
                invalid_data
            )

    ingest_c_forms = ingest_company_forms(company_list_cik)
    


    @task
    def ingest_company_facts(
        company_list_cik: list[str]
    ) -> None:
        
        valid_data, invalid_data = extract_company_facts(company_list_cik)
        if valid_data:
            save_company_facts(valid_data)


        if invalid_data:
            save_dlq("company_facts", invalid_data)
    
    ingest_c_facts = ingest_company_facts(company_list_cik)


    @task
    def ingest_alpha_vantage(
        tickers_list: list[str]
    ) -> None:
        
        valid_data, invalid_data  = extract_alpha_vantage(tickers_list)
        
        if valid_data:
            save_alpha_vantage(valid_data)

        if invalid_data:
            save_dlq("alpha_vantage", invalid_data )


    ingest_a_vantage = ingest_alpha_vantage(tickers_list)



    #tickers_list >> ingest_a_vantage


dag = ingest_bronze_dag()

'''
    
      

