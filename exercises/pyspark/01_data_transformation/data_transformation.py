"""
You are a Data Engineer at an online retail company that has a significant number of daily users on its website.
The company captures several types of user interaction data: including user clicks, views, and purchases.

You are given a CSV file named data.csv, which contains this information. The columns in the file are as follows: user_id, timestamp, event_type, and duration.
Your task is to perform an analysis to better understand user behavior on the website.
Specifically, your manager wants to understand the average duration of a ‘click’ event for each user.
This means you need to consider only those events where users have clicked on something on the website.

Finally, your analysis should be presented in the form of a Parquet file named output.parquet that contains two columns: user_id and avg_duration.
The challenge here is to devise the most efficient and accurate solution using PySpark to read, process, and write the data. Please also keep in mind the potential size and scale of the data while designing your solution.
"""
import os

from pyspark.sql.functions import col, avg
from pyspark.sql.types import DoubleType

output_dir = "exercises/pyspark/01_data_transformation/output/"
output_filename = "output.parquet"


def data_transformation(df):
    result = calculate_avg_duration_of_click(df)
    result.coalesce(1).write.mode("overwrite").parquet(output_dir)
    rename_file()


def calculate_avg_duration_of_click(df):
    df = df.withColumn("duration", col("duration").cast(DoubleType()))
    df = df.filter(df.event_type == "click") \
        .groupBy("user_id") \
        .agg(avg("duration").alias("avg_duration"))
    return df


def rename_file():
    for file in os.listdir(output_dir):
        if file.endswith(".parquet"):
            os.rename(os.path.join(output_dir, file), os.path.join(output_dir, output_filename))
