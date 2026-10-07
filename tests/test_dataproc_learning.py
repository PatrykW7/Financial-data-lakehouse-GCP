from dataproc_learning import check_processing_date
import pytest 
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row, DataFrame
from dataproc_learning import transform_alpha_vantage
import json


@pytest.mark.parametrize(
    "input_date",
    [
        "2026-10-05",
        "2026-05-26",
        "2026-10-31"
    ]

)

def test_processing_date_check(input_date: str):
    res = check_processing_date(input_date)
    assert res == input_date



@pytest.mark.parametrize(
    "input_date",
    [
    "31-12-2026",
    "05-10-2026",
    "11-28-2026"
    ]
)


def test_processing_date_invalid_check(input_date: str):
    with pytest.raises(ValueError):
        check_processing_date(input_date)
        




@pytest.mark.parametrize(
    "input_date",
    [
        "2026-12-45",
        "2026-02-31",
        "2026-100-05"
    ]
)


def test_processing_date_invalid(input_date: str):
    with pytest.raises(ValueError):
        check_processing_date(input_date)





### ALPHA VANTAGE TRANSFORM

# FIXTURE CONF - SPARK
@pytest.fixture(scope = "session")
def spark():
    spark = (SparkSession.builder 
            .appName("pytest") 
            .master("local[1]") 
            .getOrCreate()
    )

    yield spark
    spark.stop()


def create_df_alpha_vantage(spark):
    data = [
        Row(**{
            "Meta Data": Row(**{
                "2. Symbol": "NVDA"
            }),

            "Time Series (Daily)": Row(**{
                "2026-10-01": Row(**{
                    "1. open": "180.0",
                    "2. high": "185.0",
                    "3. low": "178.0",
                    "4. close": "183.0",
                    "5. volume": "1000000",
                }),

                "2026-10-02": Row(**{
                    "1. open": "183.0",
                    "2. high": "190.0",
                    "3. low": "181.0",
                    "4. close": "189.0",
                    "5. volume": "2000000",
                }),
            }),
        })
    ]

    return spark.createDataFrame(data)


def test_transform_alpha_vantage_row_count(spark):

    df = create_df_alpha_vantage(spark)
    result = transform_alpha_vantage(spark, df)
    assert result.count() == 2




