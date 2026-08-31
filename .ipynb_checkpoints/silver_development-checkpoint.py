import pandas as pd
import requests
import datetime
from zoneinfo import ZoneInfo
from google.cloud import storage
import json
from google.cloud import secretmanager
from deltalake import write_deltalake
from airflow.decorators import dag, task
import pendulum
import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from delta import configure_spark_with_delta_pip
import pendulum
from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            IntegerType,
            DecimalType,
            ArrayType,
            DataType,
            LongType,
            MapType
)


@dag(
    dag_id = 'silver_layer_ingestion',
    start_date = pendulum.datetime(2026, 8, 31, tz = "Europe/Warsaw"),
    schedule = None,
    catchup = False
)


def silver_layer():
    
    ###### 2.1. CompanyForms - Extract
    
    @task
    def silver_layer_processing():
        gcs_connector_jar = os.path.expanduser(
        "~/spark-jars/gcs-connector-hadoop3-latest.jar"
        )
    
        if not os.path.isfile(gcs_connector_jar):
            raise FileNotFoundError(
                f"Nie znaleziono konektora: {gcs_connector_jar}"
            )
        
        builder = (
            SparkSession.builder
            .master("local[*]")
            .appName("development_for_silver_layer")
        
            # GCS
            .config(
                "spark.jars",
                gcs_connector_jar
            )
            .config(
                "spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
            )
            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
            )
        
            # Delta Lake
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )
        
        spark = (
            configure_spark_with_delta_pip(builder)
            .getOrCreate()
        )
    
    
    
        #storage_client = storage.Client()
        #bucket = storage_client.get_bucket("project-dev-storage")

        
        df_companyForms = (
            spark.read
            .format("json")
            #.option("multiline", "true")
            .load(f"gs://project-dev-storage/bronze/company_forms/{datetime.date.today()}/")
        )
        
        
        
        df_companyForms =  df_companyForms.select(
                "filings.recent.accessionNumber",
                "filings.recent.filingDate",
                "filings.recent.reportDate",
                "filings.recent.form",
                "filings.recent.primaryDocument",
                "filings.recent.isXBRL",
                "filings.recent.isInlineXBRL"
        )
        
        
        
        df_companyForms = df_companyForms.select(F.explode(F.arrays_zip(df_companyForms.accessionNumber, df_companyForms.filingDate, df_companyForms.reportDate, df_companyForms.form, df_companyForms.primaryDocument, 
                                                                        df_companyForms.isXBRL, df_companyForms.isInlineXBRL)).alias("x")).select(F.col("x.accessionNumber"), F.col("x.filingDate"), F.col("x.reportDate"),
                                F.col("x.form"), F.col("x.primaryDocument"), F.col("x.isXBRL"), F.col("x.isInlineXBRL"))
        
        
        
        ###### 2.2. CompanyForms - Save to Delta Table
        
        df_companyForms.write.format("delta").mode("overwrite").save(f"gs://project-dev-storage/silver/company_forms/{datetime.date.today()}")
        
        
        
        ###### 3.1. CompanyFacts - Extract
        ### setting schema to laod data
        
        
        nested_units_usd = StructType([
            StructField("start", StringType()),
            StructField("end", StringType()),
            StructField("val", DoubleType()),
            StructField("accn", StringType()),
            StructField("fy", IntegerType()),
            StructField("fp", StringType()),
            StructField("form", StringType()),
            StructField("filed", StringType())
            ]
        )
        
        
        nested_units_usd_array = MapType(
            StringType(),
            ArrayType(nested_units_usd)
        )
        
        
        category_schema = StructType([
            StructField("label", StringType()),
            StructField("description", StringType()),
            StructField("units", nested_units_usd_array)
        ])
        
        
        ### combining schema to read 
        
        schema_xbrl = StructType([
            StructField("cik", LongType()),
            StructField("entityName", StringType()),
            StructField(
                "facts",
                StructType([
                    StructField(
                        "us-gaap",
                        MapType(
                            StringType(),
                            category_schema
                        )
                    )
                ])
            )
        ])
        
        
        # LOAD
        df_companyFacts = (
            spark.read
            .format("json")
            .option("multiline", "true")
            .schema(schema_xbrl)
            .load(f"gs://project-dev-storage/bronze/company_facts/{datetime.date.today()}/")
        )
        
        
        #df_companyFacts.select("cik", "entityName", F.explode("facts.`us-gaap`").alias("key", "value")).select("cik", "entityName", "key", F.explode("value")).show()
        df_company_facts = (df_companyFacts.select("cik", "entityName", F.explode("facts.`us-gaap`").alias("fact_name", "fact_details")).select("cik","entityName","fact_name",
                F.explode("fact_details.units").alias("unit_name","unit_values")).select("cik","entityName", "fact_name","unit_name",F.explode("unit_values").alias("fact_value"))
                .select("cik","entityName", "fact_name","unit_name", "fact_value.start", "fact_value.end", "fact_value.val", "fact_value.accn", "fact_value.fy", "fact_value.fp", "fact_value.form", "fact_value.filed"))
        
        
        
        ###### 3.2. CompanyFacts - Save to Delta Table
        df_company_facts.write.format("delta").mode("overwrite").save(f"gs://project-dev-storage/silver/company_facts/{datetime.date.today()}/")
        
        
        
        
        ###### 4.1. Alpha Vantage - Load from bronze
        df_alpha_vantage = (spark.read
                            .format("json")
                            .option("multiline",True)
                            .load(f"gs://project-dev-storage/bronze/alpha_vantage/{datetime.date.today()}/")
        )
        
        
        dates_list = df_alpha_vantage.schema["Time Series (Daily)"].dataType.fieldNames()
        
        df_alpha_vantage = df_alpha_vantage.select("Meta Data.`2. Symbol`", F.explode(F.array(*[
                                                        F.struct(
                                                           F.lit(f"{i}").alias("date"),
                                                           F.col(f"Time Series (Daily).{i}.`1. open`"),
                                                           F.col(f"Time Series (Daily).{i}.`2. high`"),
                                                           F.col(f"Time Series (Daily).{i}.`3. low`"),
                                                           F.col(f"Time Series (Daily).{i}.`4. close`"),
                                                           F.col(f"Time Series (Daily).{i}.`5. volume`")
                                                        )
                                                      for i in dates_list])).alias("x")
                 ).select(F.col("`2. Symbol`").alias("symbol"), F.col("x.date").alias("date"), F.col("x.`1. open`").alias("open"), F.col("x.`2. high`").alias("high"), F.col("x.`3. low`").alias("low"), 
                          F.col("x.`4. close`").alias("close"), F.col("x.`5. volume`").alias("volume")) 
        
        
        
        ###### 4.2 Alpha Vantage - Save to silver layer
        df_alpha_vantage.write.format("delta").mode("overwrite").save(f"gs://project-dev-storage/silver/alpha_vantage/{datetime.date.today()}/")

    silver_layer_processing()


dag = silver_layer()