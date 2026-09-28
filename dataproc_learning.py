from pyspark.sql import SparkSession

spark = (
        SparkSession.builder \
        .appName("test dataproc") \
        .getOrCreate()

)


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


spark.stop()