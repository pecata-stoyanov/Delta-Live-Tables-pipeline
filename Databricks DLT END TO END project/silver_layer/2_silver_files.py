# Databricks notebook source
# MAGIC %md
# MAGIC ### **Text Files**

# COMMAND ----------

with open("/Workspace/Databricks_bootcamp/silver_layer/File.txt", "r")as f:
          content = f.read()
print(content)

# COMMAND ----------

with open("/Workspace/Databricks_bootcamp/silver_layer/newfile.py", "w") as f:
          f.write("My_secret = '1234'")

# COMMAND ----------

import newfile

# COMMAND ----------

newfile.my_secret

# COMMAND ----------

