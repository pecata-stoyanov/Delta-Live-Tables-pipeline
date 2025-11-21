# Databricks notebook source
# MAGIC %md
# MAGIC ### **Volumes**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME databrickspepi.bronze.bronze_volume
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from csv.`/Volumes/databrickspepi/bronze/bronze_volume/sales/`

# COMMAND ----------

# MAGIC %md
# MAGIC # DBUTILS

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.ls("/Volumes/databrickspepi/bronze/bronze_volume/sales/")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/databrickspepi/bronze//bronze_volume/customers")

# COMMAND ----------

dbutils.fs.cp("/Volumes/databrickspepi/bronze/bronze_volume/sales/","/Volumes/databrickspepi/bronze/bronze_volume/customers/",True)

# COMMAND ----------

dbutils.fs.rm("/Volumes/databrickspepi/bronze/bronze_volume/customers/fact_sales.csv")

# COMMAND ----------

dbutils.fs.put("/Volumes/databrickspepi/bronze//bronze_volume/customers/test.py", "print(Hello Pepi)", True)

# COMMAND ----------

all_items = dbutils.fs.ls("/Volumes/databrickspepi/bronze/bronze_volume/customers/")
all_items

# COMMAND ----------

file_names = [i.name for i in all_items]
file_names

# COMMAND ----------

dbutils.widgets.text("par1","")

# COMMAND ----------

for i in file_names:
  dbutils.fs.rm(f"/Volumes/databrickspepi/bronze/bronze_volume/customers/{i}")

# COMMAND ----------

dbutils.widgets.get("par1")

# COMMAND ----------

