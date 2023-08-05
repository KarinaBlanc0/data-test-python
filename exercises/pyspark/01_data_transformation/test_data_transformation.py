import os.path
from data_transformation import data_transformation, calculate_avg_duration_of_click


def test_data_transformation(spark_session):
    assert spark_session is not None


def test_calculate_avg_duration_of_click(spark_session):
    input_df = spark_session.read.option("header", True).csv("exercises/pyspark/01_data_transformation/data.csv")

    result = calculate_avg_duration_of_click(input_df).collect()
    assert len(result) == 2
    assert result[0]["user_id"] == "1"
    assert result[0]["avg_duration"] == 6.5
    assert result[1]["user_id"] == "2"
    assert result[1]["avg_duration"] == 10.0


def test_data_transformation_process(spark_session):
    input_df = spark_session.read.option("header", True).csv("exercises/pyspark/01_data_transformation/data.csv")

    data_transformation(input_df)

    assert os.path.exists("exercises/pyspark/01_data_transformation/output/output.parquet") == True
