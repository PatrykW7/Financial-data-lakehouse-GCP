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


parser = argparse.ArgumentParser()
parser.add_argument(
    "--processing-date",
    required=True,
)


def check_processing_date(val: str) -> str:
    try:
        date.fromisoformat(val)
    except ValueError:
        raise ValueError(f"Niepoprawny processing_date: {val}. Oczekiwany format: YYYY-MM-DD.")
    return val


args = parser.parse_args()
processing_date_to_check = args.processing_date

processing_date = check_processing_date(processing_date_to_check)


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
                                                F.col("x.open").try_cast("double").alias("open"),
                                                F.col("x.high").try_cast("double").alias("high"),
                                                F.col("x.low").try_cast("double").alias("low"),
                                                F.col("x.close").try_cast("double").alias("close"),
                                                F.col("x.volume").try_cast("long").alias("volume"),
                                                )
    
    return df_alpha_vantage


def valid_data_alpha_vantage(
    spark:  SparkSession, 
    df: DataFrame) -> tuple[DataFrame, DataFrame]:

    condition = (F.col("symbol").isNull() |
                F.col("date").isNull()   |
                F.col("open").isNull()   |
                F.col("high").isNull()   |
                F.col("low").isNull()    |
                F.col("close").isNull()  |
                F.col("volume").isNull()
    )


    df_invalid = (
            df
            .filter(
                condition
            )
            .withColumn("error_reason", F.when(
                                                F.col("symbol").isNull(), "symbol col is NULL")
                                        .when(
                                                F.col("date").isNull(), "date is NULL")
                                        .when(
                                                F.col("open").isNull(), "open is NULL")
                                        .when(
                                                F.col("high").isNull(), "high is NULL")
                                        .when(
                                                F.col("low").isNull(), "low is NULL")
                                        .when(
                                                F.col("close").isNull(), "close is NULL")
                                        .when(
                                            F.col("volume").isNull(), "volume is NULL")                                      
            )
        )


    df_valid = (
        df.filter(
            ~condition            
        )
    )

    return df_valid, df_invalid



def write_alpha_vantage(
        spark: SparkSession,
        df: DataFrame,
        silver_path: str
    ) -> None:

    df.write \
            .format("delta")\
            .mode("overwrite")\
            .save(silver_path)



def write_invalid_data(
    spark: SparkSession,
    df: DataFrame,
    source: str
    )-> None:

    df.write \
    .format("delta") \
    .mode("append") \
    .save(f"gs://project-dev-storage/dlq/transformation/{source}/{processing_date}")




def alpha_vantage_processing(
        spark: SparkSession,
        bronze_path: str,
        silver_path: str,
    ) -> None:

    df = read_alpha_vantage(spark, bronze_path)
    res = transform_alpha_vantage(spark, df)
    df_valid, df_invalid = valid_data_alpha_vantage(spark, res)


    if not df_valid.isEmpty():
        df_valid.withColumn("processed_at", F.current_timestamp())
        df_valid.withColumn("processing_date", F.lit(processing_date))
        write_alpha_vantage(spark, df_valid, silver_path)


    if not df_invalid.isEmpty():
        write_invalid_data(
            spark, 
            df_invalid,
            "alpha_vantage"
        )



def read_company_forms_process(
        spark: SparkSession,
        bronze_path: str
    ) -> DataFrame:


    return (
        spark.read
        .format("json")
        .option("multiline", "true")
        .load(bronze_path)
        )


def transform_company_form_process(
        spark: SparkSession,
        df: DataFrame
    ) -> DataFrame:

    df_companyForms =  df.select(
                "cik",
                "filings.recent.accessionNumber",
                "filings.recent.filingDate",
                "filings.recent.reportDate",
                "filings.recent.form",
                "filings.recent.primaryDocument",
                "filings.recent.isXBRL",
                "filings.recent.isInlineXBRL"
        )
            
                    
    df_companyForms = df_companyForms.select(F.col("cik"), F.explode(F.arrays_zip(df_companyForms.accessionNumber, df_companyForms.filingDate, df_companyForms.reportDate, df_companyForms.form, df_companyForms.primaryDocument, 
                            df_companyForms.isXBRL, df_companyForms.isInlineXBRL)).alias("x")).select(F.col("cik"), F.col("x.accessionNumber"), F.col("x.filingDate"), F.col("x.reportDate"),
                            F.col("x.form"), F.col("x.primaryDocument"), F.col("x.isXBRL"), F.col("x.isInlineXBRL"))

    return df_companyForms



def valid_data_company_form_process(
        spark: SparkSession,
        df: DataFrame
    ) -> tuple[DataFrame, DataFrame]:

    condition = (
        
            F.col("accessionNumber").isNull() |
            F.col("filingDate").isNull() |
           # F.col("reportDate").isNull() |
            F.col("form").isNull() |
            F.col("primaryDocument").isNull() |
            F.col("isXBRL").isNull() |
            F.col("isInlineXBRL").isNull()
        
    )


    df_invalid = (
        df
        .filter(condition)
        .withColumn("error_reason",
                    F.when(
                        F.col("accessionNumber").isNull(), "accessionNumber IS NULL"
                    )
                    .when(
                        F.col("filingDate").isNull(), "filingDate IS NULL"
                    )
                    .when(F.col("reportDate").isNull(), "reportDate IS NULL"
                    )
                    .when(F.col("form").isNull(), "form IS NULL"
                    )
                    .when(F.col("primaryDocument").isNull(), "primaryDocument IS NULL"
                    )
                    .when(F.col("isXBRL").isNull(), "isXBRL IS NULL"
                    )
                    .when(F.col("isInlineXBRL").isNull(), "isInlineXBRL IS NULL")
                    
                    )
    )


    df_valid = df.filter(~condition)
    return df_valid, df_invalid



def write_company_form_process(
        spark: SparkSession,
        df: DataFrame,
        silver_path: str
    ) -> None:


    df.write\
        .format("delta")\
        .mode("overwrite")\
        .save(silver_path)


def company_forms_process(
        spark: SparkSession,
        bronze_path: str,
        silver_path: str
    ) -> None:

    df = read_company_forms_process(spark, bronze_path)
    res = transform_company_form_process(spark, df)
    df_valid, df_invalid = valid_data_company_form_process(spark, res)

    if not df_valid.isEmpty():
        df_valid.withColumn("processed_at", F.current_timestamp())
        df_valid.withColumn("processing_date", F.lit(processing_date))
        write_company_form_process(spark, df_valid, silver_path)

    if not df_invalid.isEmpty():
        write_invalid_data(spark, df_invalid, "company_forms")


def read_company_facts_processing(
        spark: SparkSession,
        bronze_path: str
    ) -> DataFrame:

    nested_units_usd = StructType([
            StructField("start", StringType()),
            StructField("end", StringType()),
            StructField("val", DecimalType(38, 9)),
            StructField("accn", StringType()),
            StructField("fy", IntegerType()),
            StructField("fp", StringType()),
            StructField("form", StringType()),
            StructField("filed", StringType()),
            StructField("frame", StringType())
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
    return (
        spark.read
        .format("json")
        .option("multiline", "true")
        .schema(schema_xbrl)
        .load(bronze_path)
    )


def transform_company_facts_processing(
        spark: SparkSession,
        df: DataFrame
    ) -> DataFrame:

    
    df_company_facts = (df.select("cik", "entityName", F.explode("facts.`us-gaap`").alias("fact_name", "fact_details")).select("cik","entityName","fact_name",
                F.explode("fact_details.units").alias("unit_name","unit_values")).select("cik","entityName", "fact_name","unit_name",F.explode("unit_values").alias("fact_value"))
                .select("cik","entityName", "fact_name","unit_name", "fact_value.start", "fact_value.end", "fact_value.val", "fact_value.accn", "fact_value.fy", "fact_value.fp", "fact_value.form", "fact_value.filed", "fact_value.frame"))

    return df_company_facts



def valid_data_company_facts(
    spark: SparkSession,
    df:DataFrame
    ) -> tuple[DataFrame, DataFrame]:


    condition = (
        F.col("cik").isNull() |
        F.col("entityName").isNull() |
        F.col("fact_name").isNull() |
        F.col("unit_name").isNull() |
        #F.col("start").isNull() |
        F.col("end").isNull() |
        F.col("val").isNull() |
        F.col("accn").isNull() |
        #F.col("fy").isNull() |
        #F.col("fp").isNull() |
        F.col("form").isNull() |
        F.col("filed").isNull()
    )


    df_valid = df.filter(
        ~condition
    )


    df_invalid = (df.filter(
        condition
        )
    ).withColumn("error_reason", F.when(
                                        F.col("cik").isNull(), "cik IS NULL" 
                                        )
                                 .when(
                                        F.col("entityName").isNull(), "entityName IS NULL"
                                        )
                                .when(
                                        F.col("fact_name").isNull(), "fact_name IS NULL"
                                        )
                                .when(
                                        F.col("unit_name").isNull(), "unit_name IS NULL"
                                        )
                                #.when(F.col("start").isNull(), "start IS NULL")
                                .when(
                                        F.col("end").isNull(), "end IS NULL"
                                        )
                                .when(
                                        F.col("val").isNull(), "val IS NULL"
                                        )
                                .when(
                                        F.col("accn").isNull(), "accn IS NULL"
                                        )
                                #.when(F.col("fy").isNull(), "fy IS NULL")
                                #.when(F.col("fp").isNull(), "fp IS NULL")
                                .when(
                                        F.col("form").isNull(), "form IS NULL"
                                        )
                                .when(
                                        F.col("filed").isNull(), "filed IS NULL"
                                )
    )                       

    
    return df_valid, df_invalid





def write_company_facts_processing(
        spark: SparkSession,
        df: DataFrame,
        silver_path: str
    ) -> None:


    df.write\
            .format("delta")\
            .mode("overwrite")\
            .save(silver_path)
    


def company_facts_processing(
        spark: SparkSession,
        bronze_path: str,
        silver_path: str
    ) -> None:

    #df = read_company_facts_processing(spark, bronze_path)
    #res = transform_company_facts_processing(spark, df)
    
    df = read_company_facts_processing(spark, bronze_path)
    res = transform_company_facts_processing(spark, df)

    df_valid, df_invalid = valid_data_company_facts(spark, res)


    if not df_valid.isEmpty():
        df_valid.withColumn("processed_at", F.current_timestamp())
        df_valid.withColumn("processing_date", F.lit(processing_date))
        write_company_facts_processing(spark, df_valid, silver_path)


    if not df_invalid.isEmpty():
        write_invalid_data(spark, df_invalid, "company_facts")





def main():

    PROJECT_ID = "gcp-pde-498614"
    BUCKET_NAME = "project-dev-storage"


    
    #processing_date = "2026-10-11"

    print("PROCESSING DATE:", processing_date)
    
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


        print("ALPHA SILVER:",f"gs://project-dev-storage/silver/alpha_vantage/{processing_date}/"
)

        
     
        alpha_vantage_processing(
            spark, 
            f"gs://project-dev-storage/bronze/alpha_vantage/{processing_date}/",
            f"gs://project-dev-storage/silver/alpha_vantage/{processing_date}/"
            )
        

        print(
            "FORMS SILVER:",
            f"gs://project-dev-storage/silver/company_forms/{processing_date}/"
        )

        
        company_forms_process(
            spark,
            f"gs://project-dev-storage/bronze/company_forms/{processing_date}/",
            f"gs://project-dev-storage/silver/company_forms/{processing_date}/"

        )


        print(
            "FACTS SILVER:",
            f"gs://project-dev-storage/silver/company_facts/{processing_date}/"
        )
        

        company_facts_processing(
            spark,
            f"gs://project-dev-storage/bronze/company_facts/{processing_date}/",
            f"gs://project-dev-storage/silver/company_facts/{processing_date}/"
        )


        
        '''
        data = [
            ('Apple', 1),
            ('NVIDIA', 2),
            ('Microsoft', 3)
        ]


        df = spark.createDataFrame(
            data, ["company", "value"]
        )


    

        '''
        print("Spark version:", spark.version)
        print("=== END DATAPROC LEARNING ===")


    finally:
        spark.stop()


if __name__ == "__main__":
    main()