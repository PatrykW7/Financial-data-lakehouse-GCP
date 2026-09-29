import argparse
from pyspark.sql import SparkSession

spark = (
        SparkSession.builder \
        .appName("test dataproc") \
        .getOrCreate()

)

PROJECT_ID = "gcp-pde-498614"
BUCKET_NAME = "project-dev-storage"


parser = argparse.ArgumentParser()

parser.add_argument(
    "--processing-date",
    required=True,
)


args = parser.parse_args()

processing_date = args.processing_date


try:
    print("START DATAPROC LEARNING")


    data = [
        ('Apple', 1),
        ('NVIDIA', 2),
        ('Microsoft', 3)
    ]


    df = spark.createDataFrame(
        data, ["company", "value"]
    )


    df.show()


    print("Spark version:", spark.version)
    print("=== END DATAPROC LEARNING ===")


finally:
    spark.stop()