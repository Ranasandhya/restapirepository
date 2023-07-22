# Databricks notebook source
import requests
import pandas as pd
import json 


# COMMAND ----------

r = requests.get('https://jsonplaceholder.typicode.com/comments')
x = r.json()
df = pd.read_json(json.dumps(x))
print (df)

# COMMAND ----------

df_spark = spark.createDataFrame(df)

# COMMAND ----------

display(df_spark)

# COMMAND ----------

df_spark.write.mode('overwrite').format('delta').save('abfss://bronze@databrickckscourseextdl.dfs.core.windows.net/restapi_comments')

# COMMAND ----------


