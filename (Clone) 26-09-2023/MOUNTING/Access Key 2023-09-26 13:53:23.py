# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://raw@sanly.blob.core.windows.net",
  mount_point = "/mnt/sanly/raw",
  extra_configs = {"fs.azure.account.key.sanly.blob.core.windows.net":"+wZyMJdwqiETIzCNMc/uvE0AJQ/2+fIGVKKvfx4um7lsUO0EPZjLx3efLhF9OihDdkaV1TBwq77j+AStSZRQ1Q=="})

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/sanly/raw/Baby_Names.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

output="dbfs:/mnt/sanly/raw/output"

# COMMAND ----------

df.write.mode("overwrite").parquet(f"{output}/Priyadarsini/babyname")

# COMMAND ----------


