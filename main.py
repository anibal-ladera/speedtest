# Databricks notebook source
import pandas as pd
from pandas_gbq import to_gbq
import requests
import json
import logging
import os

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración de BigQuery
project_id = 'futurum-396301'
dataset_id = 'schema'
table_id = 'team2_json'
table_id2 = 'testing'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/dbfs/FileStore/credentials/gcp_credenciales.json'

df = spark.read \
  .format("bigquery") \
  .option("table", f"{project_id}.{dataset_id}.{table_id}") \
  .option("credentialsFile", "/dbfs/FileStore/credentials/gcp_credenciales.json") \
  .option("parentProject", project_id) \
  .option("viewsEnabled", "true") \
  .load()
display(df)

# COMMAND ----------

#100% Python sin librerías de terceros
import pandas as pd
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials.json'
client = bigquery.Client()

# Define your BigQuery dataset and table names
dataset_id = 'schema'
table_name = 'speedtest_t1'

# Read the table 
df = spark.read.format("parquet") \
  .option("inferSchema", "true") \
  .option("multiline", "true") \
  .option("header", "true") \
  .option("sep", '|') \
  .load('/FileStore/speedtest/2023-04-01_performance_fixed_tiles.parquet')
display(df)

# COMMAND ----------

# MAGIC %sql select count(*) from speedtest_t1

# COMMAND ----------

# Ensure that you have loaded the correct spark bigquery connector library with Maven
# e.g.: `com.google.cloud.spark:spark-bigquery_2.12:0.26.0`

(df.write
 .format("bigquery")
 .option("credentialsFile", "/path/to/your/credentials.json")
 .option("table", "vm-etl-321800.schema.speedtest_t1") 
 .option("temporaryGcsBucket", "bucket_ladera1")
 .mode("error")
 .save())

print("Table written successfully to BigQuery.")

# COMMAND ----------

df = df.toPandas()

table_ref = client.dataset(dataset_id).table(table_name)
job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()

sql = 'SELECT * FROM dataset_futurum.table_futurum LIMIT 10'

# Define your SQL query
query = f"SELECT * FROM `{dataset_id}.{table_name}`"

# Use the BigQuery client to execute the query and get the results as a DataFrame
quey_result = client.query(query).to_dataframe()
display(quey_result)
