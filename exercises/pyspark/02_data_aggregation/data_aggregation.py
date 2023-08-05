"""
As a Data Engineer at a digital marketing agency, your team uses an in-house analytics tool that tracks user activity across various campaigns run by the company.
Each user interaction, whether a click or a view, is registered as an event. The collected data, stored in a JSON file named data.json, contains information about the date of the event (event_date) and the count of events that happened on that date (event_count).

The company wants to understand the total number of user interactions that occurred each day to identify trends in user engagement.
As such, your task is to analyze this data and prepare a summary report.
Your report should include the following information:
- The date of the events (event_date).
- The total number of events that occurred on each date (total_events).
The output should be sorted in descending order based on the total number of events, and the results should be saved in a CSV file named output.csv.
"""
import os

from pyspark.sql.functions import col, sum, desc
from pyspark.sql.types import IntegerType

output_dir = "exercises/pyspark/02_data_aggregation/output/"
output_filename = "output.csv"


def calculate_total_events_per_date(df):
    df = df.withColumn("event_count", col("event_count").cast(IntegerType()))
    df = df.groupBy("event_date").agg(sum("event_count").alias("total_events"))

    sorted_df = df.orderBy(desc("total_events"))
    return sorted_df


def data_aggregation(df):
    result = calculate_total_events_per_date(df)
    result.coalesce(1).write.mode("overwrite").csv(output_dir)
    rename_file()


def rename_file():
    for file in os.listdir(output_dir):
        if file.endswith(".csv"):
            os.rename(os.path.join(output_dir, file), os.path.join(output_dir, output_filename))
