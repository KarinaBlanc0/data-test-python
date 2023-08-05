import os

from data_aggregation import data_aggregation, calculate_total_events_per_date


def test_data_aggregation(spark_session):
    assert spark_session is not None


def test_calculate_total_event_per_date(spark_session):
    input_df = spark_session.read.option("multiline", "true").json("exercises/pyspark/02_data_aggregation/data.json")
    result = calculate_total_events_per_date(input_df).collect()
    assert len(result) == 2
    assert result[0]["event_date"] == "2022-01-01"
    assert result[0]["total_events"] == 18
    assert result[1]["event_date"] == "2022-01-02"
    assert result[1]["total_events"] == 17


def test_run_avg_process(spark_session):
    input_df = spark_session.read.option("multiline", "true").json("exercises/pyspark/02_data_aggregation/data.json")

    data_aggregation(input_df)

    assert os.path.exists("exercises/pyspark/02_data_aggregation/output/output.csv") == True
