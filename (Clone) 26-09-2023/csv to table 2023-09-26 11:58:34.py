# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/
# MAGIC

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/FilteredData_2023_09_08.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("test.my_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_table

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls
# MAGIC dbfs:/user

# COMMAND ----------

# MAGIC %fs ls
# MAGIC dbfs:/user/hive/warehouse/test.db/my_table

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls
# MAGIC dbfs:/user/hive/warehouse/test.db/my_table

# COMMAND ----------


