import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import date
from pyspark.sql.types import (
                StructType,
                StructField,
                StringType,
                IntegerType,
                DoubleType,
                DecimalType,
                ArrayType,
                DataType,
                LongType,
                MapType
)



def read_alpha_vantage(
        spark: SparkSession,
        bronze_path: str
    ) -> DataFrame:

    return (
                spark.read\
                .format("json")\
                .option("multiline", True)
                .load(bronze_path)
            ) 


def transform_alpha_vantage(
        spark: SparkSession,
        df: DataFrame
    ) -> DataFrame:

    daily_type = df.schema["Time Series (Daily)"].dataType
    dates_list = daily_type.fieldNames()
    
    rows = [
            F.struct(
                F.lit(day).alias("date"),
                F.col(f"`Time Series (Daily)`.`{day}`.`1. open`").alias("open"), 
                F.col(f"`Time Series (Daily)`.`{day}`.`2. high`").alias("high"), 
                F.col(f"`Time Series (Daily)`.`{day}`.`3. low`").alias("low"),
                F.col(f"`Time Series (Daily)`.`{day}`.`4. close`").alias("close"),
                F.col(f"`Time Series (Daily)`.`{day}`.`5. volume`").alias("volume"),
            )
            for day in dates_list
    ]


    temp_alpha_vantage = (
        df.select(F.col("`Meta Data`.`2. Symbol`").alias("symbol"), F.explode(F.array(*rows)).alias("x"))
    )


    df_alpha_vantage = temp_alpha_vantage.select("symbol", F.col("x.date").try_cast("date").alias("date"),
                                                F.col("x.open").cast("double").alias("open"),
                                                F.col("x.high").cast("double").alias("high"),
                                                F.col("x.low").cast("double").alias("low"),
                                                F.col("x.close").cast("double").alias("close"),
                                                F.col("x.volume").cast("long").alias("volume"),
                                                )
    
    return df_alpha_vantage


def write_alpha_vantage(
        spark: SparkSession,
        df: DataFrame,
        silver_path: str
    ) -> None:

    df.write \
            .format("delta")\
            .mode("overwrite")\
            .save(silver_path)


def alpha_vantage_processing(
        spark: SparkSession,
        bronze_path: str,
        silver_path: str
    ) -> None:

    df = read_alpha_vantage(spark, bronze_path)
    res = transform_alpha_vantage(spark, df)
    write_alpha_vantage(spark, res, silver_path)


def company_forms_process(
        spark: SparkSession,
        bronze_path: str,
        silver_path: str
):
        
    df_companyForms = (
        spark.read
        .format("json")
        .option("multiline", "true")
        .load(bronze_path)
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
            
    df_companyForms.write\
        .format("delta")\
        .mode("overwrite")\
        .save(silver_path)
    

def company_facts_processing(
        spark: SparkSession,
        bronze_path: str,
        silver_path: str
):
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
        .load(bronze_path)
    )
            
            
    #df_companyFacts.select("cik", "entityName", F.explode("facts.`us-gaap`").alias("key", "value")).select("cik", "entityName", "key", F.explode("value")).show()
    df_company_facts = (df_companyFacts.select("cik", "entityName", F.explode("facts.`us-gaap`").alias("fact_name", "fact_details")).select("cik","entityName","fact_name",
            F.explode("fact_details.units").alias("unit_name","unit_values")).select("cik","entityName", "fact_name","unit_name",F.explode("unit_values").alias("fact_value"))
            .select("cik","entityName", "fact_name","unit_name", "fact_value.start", "fact_value.end", "fact_value.val", "fact_value.accn", "fact_value.fy", "fact_value.fp", "fact_value.form", "fact_value.filed"))
    
            
            
    ###### 3.2. CompanyFacts - Save to Delta Table
    df_company_facts.write\
        .format("delta")\
        .mode("overwrite")\
        .save(silver_path)


def check_processing_date(val: str) -> str:
    try:
        date.fromisoformat(val)
    except ValueError:
        raise ValueError(f"Niepoprawny processing_date: {val}. Oczekiwany format: YYYY-MM-DD.")
    return val



def main():

    PROJECT_ID = "gcp-pde-498614"
    BUCKET_NAME = "project-dev-storage"


    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processing-date",
        required=True,
    )


    args = parser.parse_args()
    processing_date_to_check = args.processing_date

    processing_date = check_processing_date(processing_date_to_check)
    
    #print("BUCKET_NAME:", BUCKET_NAME)

    '''
    bronze_path = (
        f"gs://{BUCKET_NAME}/bronze/alpha_vantage/{processing_date}/"
    )
    '''
    
    #print("bronze_path:", bronze_path)


    try:
        print("START DATAPROC LEARNING")

        spark = (
            SparkSession.builder \
            .appName("test dataproc") \
            .getOrCreate()

            )
        
        alpha_vantage_processing(
            spark, 
            f"gs://project-dev-storage/bronze/alpha_vantage/{processing_date}/",
            f"gs://project-dev-storage/silver/alpha_vantage/{processing_date}/"
            )
        

        '''
        company_forms_process(
            spark,
            f"gs://project-dev-storage/bronze/company_forms/{processing_date}/",
            f"gs://project-dev-storage/silver/company_forms/{processing_date}/"

        )
       

        company_facts_processing(
            spark,
            f"gs://project-dev-storage/bronze/company_facts/{processing_date}/",
            f"gs://project-dev-storage/silver/company_facts/{processing_date}/"
        )
        '''


     
        print("Spark version:", spark.version)
        print("=== END DATAPROC LEARNING ===")


    finally:
        spark.stop()


if __name__ == "__main__":
    main()