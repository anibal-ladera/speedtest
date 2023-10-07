# Databricks notebook source
# MAGIC %md
# MAGIC <a href="https://colab.research.google.com/github/anibal-ladera/speedtest/blob/main/Untitled4.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

# COMMAND ----------

import pandas as pd
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials.json'
client = bigquery.Client()

# Define your BigQuery dataset and table names
dataset_id = 'schema'
table_name = 'speedtest_t1'

# Read the table 
df = (spark.sql(f'SELECT * FROM speedtest_t1 limit 100')).toPandas()

table_ref = client.dataset(dataset_id).table(table_name)
job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()

sql = 'SELECT * FROM dataset_futurum.table_futurum LIMIT 10'

# Define your SQL query
query = f"SELECT * FROM `{dataset_id}.{table_name}`"

# Use the BigQuery client to execute the query and get the results as a DataFrame
quey_result = client.query(query).to_dataframe()
display(quey_result)

