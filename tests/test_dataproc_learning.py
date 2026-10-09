from dataproc_learning import check_processing_date
import pytest 
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row, DataFrame
from dataproc_learning import transform_alpha_vantage, transform_company_form_process, transform_company_facts_processing
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




### ALPHA VANTAGE TRANSFORM TEST

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





##### TRANSFORM COMPANY FORM PROCESS TEST
def transform_company_forms_process_dataset(spark):
    data = [
        Row(
            filings=Row(
                recent=Row(
                    accessionNumber=["A1", "A2"],
                    filingDate=["2026-01-10", "2026-04-10"],
                    reportDate=["2025-12-31", "2026-03-31"],
                    form=["10-K", "10-Q"],
                    primaryDocument=["a1.htm", "a2.htm"],
                    isXBRL=[1, 1],
                    isInlineXBRL=[1, 1],
                )
            )
        )
    ]

    return spark.createDataFrame(data)


def test_transform_company_forms_process_row_number(spark):
    df = transform_company_forms_process_dataset(spark)
    res = transform_company_form_process(spark, df)
    assert res.count() == 2



def test_transform_company_forms_process_column_names(spark):
    

    df = transform_company_forms_process_dataset(spark)
    res = transform_company_form_process(spark, df)

    assert res.columns == [
        "accessionNumber",
        "filingDate",
        "reportDate",
        "form",
        "primaryDocument",
        "isXBRL",
        "isInlineXBRL"
    ]


def test_transform_company_forms_process_arrays_zip(spark):
    df = transform_company_forms_process_dataset(spark)
    res_df = transform_company_form_process(spark, df)

    res = res_df.collect()
    
    assert res[0]["accessionNumber"] == "A1"
    assert res[0]["filingDate"] == "2026-01-10"
    assert res[0]["reportDate"] == "2025-12-31"
    assert res[0]["form"] == "10-K"
    assert res[0]["primaryDocument"] == "a1.htm"
    assert res[0]["isXBRL"] == 1
    assert res[0]["isInlineXBRL"] == 1


    assert res[1]["accessionNumber"] == "A2"
    assert res[1]["filingDate"] == "2026-04-10"
    assert res[1]["reportDate"] == "2026-03-31"
    assert res[1]["form"] == "10-Q"
    assert res[1]["primaryDocument"] == "a2.htm"
    assert res[1]["isXBRL"] == 1
    assert res[1]["isInlineXBRL"] == 1





### COMPANY FACTS PROCESSING 



def company_facts_processing_dataset(spark):
    data = [
        Row(
            cik=1045810,
            entityName="NVIDIA CORP",
            facts=Row(**{
                "us-gaap": {
                    "Revenue": Row(
                        label="Revenue",
                        description="Revenue description",
                        units={
                            "USD": [
                                Row(
                                    start="2025-01-01",
                                    end="2025-12-31",
                                    val=100.0,
                                    accn="A1",
                                    fy=2025,
                                    fp="FY",
                                    form="10-K",
                                    filed="2026-01-20",
                                ),
                                Row(
                                    start="2026-01-01",
                                    end="2026-12-31",
                                    val=120.0,
                                    accn="A2",
                                    fy=2026,
                                    fp="FY",
                                    form="10-K",
                                    filed="2027-01-20",
                                ),
                            ]
                        },
                    )
                }
            }),
        )
    ]


    return spark.createDataFrame(data)


def test_company_facts_processing_row_number(spark):
    df = company_facts_processing_dataset(spark)
    res = transform_company_facts_processing(spark, df)

    assert res.count() == 2
        



def test_company_facts_processing_columns_names(spark):
    df = company_facts_processing_dataset(spark)
    res = transform_company_facts_processing(spark, df)

    assert res.columns == [
        "cik",
        "entityName",
        "fact_name",
        "unit_name",
        "start",
        "end",
        "val",
        "accn",
        "fy",
        "fp",
        "form",
        "filed"
    ]



def test_company_facts_processing_explode(spark):
    df = company_facts_processing_dataset(spark)
    res = transform_company_facts_processing(spark, df)

    res_rows = res.collect()


    assert res_rows[0]["cik"] == 1045810
    assert res_rows[0]["entityName"] == "NVIDIA CORP"
    assert res_rows[0]["fact_name"] == "Revenue"
    assert res_rows[0]["unit_name"] == "USD"
    assert res_rows[0]["start"] == "2025-01-01"
    assert res_rows[0]["end"] == "2025-12-31"
    assert res_rows[0]["val"] == 100.0
    assert res_rows[0]["accn"] == "A1"
    assert res_rows[0]["fy"] == 2025
    assert res_rows[0]["fp"] == "FY"
    assert res_rows[0]["form"] == "10-K"
    assert res_rows[0]["filed"] == "2026-01-20"


    assert res_rows[1]["cik"] == 1045810
    assert res_rows[1]["entityName"] == "NVIDIA CORP"
    assert res_rows[1]["fact_name"] == "Revenue"
    assert res_rows[1]["unit_name"] == "USD"
    assert res_rows[1]["start"] == "2026-01-01"
    assert res_rows[1]["end"] == "2026-12-31"
    assert res_rows[1]["val"] == 120.0
    assert res_rows[1]["accn"] == "A2"
    assert res_rows[1]["fy"] == 2026
    assert res_rows[1]["fp"] == "FY"
    assert res_rows[1]["form"] == "10-K"
    assert res_rows[1]["filed"] == "2027-01-20"