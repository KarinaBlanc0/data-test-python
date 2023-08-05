import os

from pyspark.sql.functions import asc

from data_join import data_join, calculate_total_spending_per_customer


def test_data_join(spark_session):
    assert spark_session is not None


def test_calculate_total_spending_per_customer(spark_session):
    purchases_df = spark_session.read.option("header", True).csv("exercises/pyspark/03_data_join/purchases.csv")
    users_df = spark_session.read.option("header", True).csv("exercises/pyspark/03_data_join/users.csv")

    result = calculate_total_spending_per_customer(purchases_df, users_df).orderBy(asc("user_id")).collect()

    assert len(result) == 3
    assert result[0]["user_id"] == '1'
    assert result[0]["total_spending"] == 75.0
    assert result[1]["user_id"] == '2'
    assert result[1]["total_spending"] == 20.0
    assert result[2]["user_id"] == '3'
    assert result[2]["total_spending"] == 30.0


def test_run_avg_process(spark_session):
    purchases_df = spark_session.read.option("header", True).csv("exercises/pyspark/03_data_join/purchases.csv")
    users_df = spark_session.read.option("header", True).csv("exercises/pyspark/03_data_join/users.csv")

    data_join(purchases_df, users_df)

    assert os.path.exists("exercises/pyspark/03_data_join/output/output.json") == True
