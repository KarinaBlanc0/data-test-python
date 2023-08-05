"""
As a Data Engineer at a rapidly growing e-commerce company, you are given two CSV files: users.csv and purchases.csv.
The users.csv file contains details about your users, and the purchases.csv file holds records of all purchases made.

Your team is interested in gaining a deeper understanding of customer behavior.
A question they’re particularly interested in is: "What is the total spending of each customer?"

Your task is to extract meaningful information from these data sets to answer this question.
The output of your work should be a JSON file, output.json.
"""
import os

from pyspark.sql.functions import col, sum

output_dir = "exercises/pyspark/03_data_join/output/"
output_filename = "output.json"


def calculate_total_spending_per_customer(purchase_df, users_df):
    df = purchase_df.groupBy(col("user_id")).agg(sum(col("price")).alias("total_spending"))
    df = df.join(users_df, df.user_id == users_df.user_id) \
        .select(users_df.user_id, users_df.name, df.total_spending) \

    return df


def data_join(purchase_df, users_df):
    result = calculate_total_spending_per_customer(purchase_df, users_df)
    result.coalesce(1).write.mode("overwrite").json(output_dir)
    rename_file()


def rename_file():
    for file in os.listdir(output_dir):
        if file.endswith(".json"):
            os.rename(os.path.join(output_dir, file), os.path.join(output_dir, output_filename))
