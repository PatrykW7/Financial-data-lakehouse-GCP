import pandas as pd
import requests
import datetime
from zoneinfo import ZoneInfo
from google.cloud import storage
import json
from airflow.decorators import dag, task
import pendulum
from google.cloud import secretmanager


# SECRETS API + EMAIL 

PROJECT_ID = "gcp-pde-498614"
SECRET_ID_key = "alpha-vantage-api-key"
SECRET_ID_email = "sec-email"

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


# second secret

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
cik_1  = "0001045810"
cik_2 = "0000320193"
TICKER_1 = "NVDA"
TICKER_2 = "AAPL"


# SETTING DAG

@dag(
    dag_id = 'bronze_layer_ingestion_new',
    start_date = pendulum.datetime(2026, 8, 1, tz = "Europe/Warsaw"),
    schedule = None,
    catchup = False
)



def ingest_bronze_dag():

    
    @task
    def extract_ang_ingest_data_bronze():
#                                I IMPORT Z API


### 1.1 Company Tickers

       companyTickers = requests.get("https://www.sec.gov/files/company_tickers_exchange.json", headers = headers)



       ### 1.2 Company Forms

       fillingMetadata_1 = requests.get(f'https://data.sec.gov/submissions/CIK{cik_1}.json', headers = headers)
       fillingMetadata_2 = requests.get(f'https://data.sec.gov/submissions/CIK{cik_2}.json', headers = headers)


       wyn = [fillingMetadata_1.json(), 
              fillingMetadata_2.json()
              ]



       ### 1.3 XBRL data

       companyFacts_1 = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_1}.json", headers = headers)
       companyFacts_2 = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_2}.json", headers = headers)


       wyn_xbrl = [
              companyFacts_1.json(),
              companyFacts_2.json()
              ]


       ### 1.4 Alpha Vantage - 1 request per second - trzeba bedzie zrobic funkcjonalnosc która to obsluzy 

       alpha_vantage_1 = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={TICKER_1}&outputsize=compact&apikey={API_KEY}")
       alpha_vantage_2 = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={TICKER_2}&outputsize=compact&apikey={API_KEY}")


       wyn_alpha_vantage = [
                     alpha_vantage_1.json(),
                     alpha_vantage_2.json()
                     ]



       #                                I SAVE TO BRONZE

       storage_client = storage.Client()
       bucket = storage_client.get_bucket("project-dev-storage")



       ### 2.1 CompanyTickers

       blob = bucket.blob(f"CIK/sec_metadata_{datetime.datetime.now(ZoneInfo('Europe/Warsaw'))}")
       blob.upload_from_string(json.dumps(companyTickers.json()), content_type = "application/json")



       ### 2.2 Company Forms

       blob = bucket.blob(f"company_forms/sec_metadata_{datetime.datetime.now(ZoneInfo('Europe/Warsaw'))}")
       blob.upload_from_string(json.dumps(wyn), content_type = "application/json")


       ### 2.3 XBRL data

       blob = bucket.blob(f"company_values/{datetime.datetime.now(ZoneInfo('Europe/Warsaw'))}")
       blob.upload_from_string(json.dumps(wyn_xbrl), content_type = "application/json")


       ### 2.4 Alpha Vantage

       blob = bucket.blob(f"alpha_vantage/{datetime.datetime.now(ZoneInfo('Europe/Warsaw'))}")
       blob.upload_from_string(json.dumps(wyn_alpha_vantage), content_type = "application/json")


    extract_ang_ingest_data_bronze()


dag = ingest_bronze_dag()