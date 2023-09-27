# Databricks notebook source
#dbutils.fs.mount(
 # source = "wasbs://raw@storagepmsdemo.blob.core.windows.net",
  #mount_point = "/mnt/storagepmsdemo/raw",
  #extra_configs = {"fs.azure.account.key.storagepmsdemo.blob.core.windows.net":"kpvUobdy/eC+1vZhvfb6PEj0lZVA8doQ8x1qzybhx/u89MQQNhHSEe537JhcIV1YMR7OgH1jYc7x+ASt8L7iQQ=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/storagepmsdemo/raw/json

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/storagepmsdemo/raw/json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists json

# COMMAND ----------

df.write.mode("append").option("path","dbfs:/mnt/storagepmsdemo/raw/output/priya/json").saveAsTable("jsonbronze")

# COMMAND ----------

#dbutils.fs.mount(
 # source = "wasbs://raw@saunextadls.blob.core.windows.net",
  #mount_point = "/mnt/saunextadls/raw",
 # extra_configs = {"fs.azure.account.key.saunextadls.blob.core.windows.net":"DsZWJs7JVVHZz1I7GKyclV8ejCdj0V2UkqMlgAp6QyVOw5rvrHvmVTgwcThdHUymWg7MXon65/0z+AStj4Yiug=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/saunextadls/raw/json

# COMMAND ----------

df3=spark.read.json("dbfs:/mnt/saunextadls/raw/json")

# COMMAND ----------

from pyspark.sql.functions import *
df2=df3.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists json

# COMMAND ----------

df2.write.mode("overwrite").option("path","dbfs:/mnt/saunextadls/raw/output/priya/json").saveAsTable("jsonbronze1")

# COMMAND ----------


