import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ID = "gcp-pde-498614"
BUCKET_NAME = "project-dev-storage"


parser = argparse.ArgumentParser()

parser.add_argument(
    "--processing-date",
    required=True,
)


args = parser.parse_args()

processing_date = args.processing_date


print("BUCKET_NAME:", BUCKET_NAME)


bronze_path = (
    f"gs://{BUCKET_NAME}/bronze/alpha_vantage/{processing_date}/"
)

print("bronze_path:", bronze_path)

try:
    print("START DATAPROC LEARNING")


    spark = (
        SparkSession.builder \
        .appName("test dataproc") \
        .getOrCreate()

        )


    df = (
        spark.read\
        .format("json")\
        .option("multiline", True)
        .load(bronze_path)
    )

    df.show()
    df.printSchema()


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

    silver_path = f"gs://{BUCKET_NAME}/silver/alpha_vantage/{processing_date}/"


    df_alpha_vantage.write \
                    .format("delta")\
                    .mode("overwrite")\
                    .save(silver_path)

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
