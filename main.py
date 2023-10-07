# Databricks notebook source
import pandas as pd
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials.json.json'
client = bigquery.Client()

# Define your BigQuery dataset and table names
dataset_id = 'dataset_futurum'
table_name = 'table2_futurum'

# Read the table 
df = (spark.sql(f'SELECT * FROM speedtest_t1 limit 100')).toPandas()

table_ref = client.dataset(dataset_id).table(table_name)
job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()

sql = 'SELECT * FROM dataset_futurum.table_futurum LIMIT 10'

# Define your SQL query
query = f"SELECT * FROM `{dataset_id}.{table_name}`"

# Use the BigQuery client to execute the query and get the results as a DataFrame
df1 = client.query(query).to_dataframe()
df1

