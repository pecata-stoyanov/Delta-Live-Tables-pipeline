# Databricks notebook source
# MAGIC %md
# MAGIC #PySpark
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

df = spark.read.format("csv")\
      .option("inferSchema", True)\
        .option("header", True)\
        .load("/Volumes/databrickspepi/bronze/bronze_volume/customers/")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("name", upper(col("name")))
display(df)

# COMMAND ----------

df = df.withColumn(
    "domain",
    split(col("email"), "@")[1]
)
display(df)

# COMMAND ----------

display(df.groupBy("domain")
         .agg(count(col("customer_id")).alias("total_customers"))
         .sort(col("total_customers").desc()))

# COMMAND ----------

df = df.withColumn("processDate", current_timestamp())
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert

# COMMAND ----------

from  delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databrickspepi.silver.customers_enr"):

    dlt_obj = DeltaTable.forName(spark, "databrickspepi.silver.customers_enr")

    dlt_obj.alias("trg").merge(
        df.alias("src"),
        col("trg.customer_id") == col("src.customer_id")
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else: 
    df.write.format("delta") \
        .mode("append") \
        .saveAsTable("databrickspepi.silver.customers_enr")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PRODUCTS

# COMMAND ----------

df_prod = spark.read.format("csv")\
      .option("inferSchema", True)\
        .option("header", True)\
        .load("/Volumes/databrickspepi/bronze/bronze_volume/products/")
display(df_prod)

# COMMAND ----------

df_prod = df_prod.withColumn("processDate", current_timestamp())
display(df_prod)

# COMMAND ----------

display(df_prod.groupBy("category").agg(avg(col("price")).alias("avg_price")))

# COMMAND ----------

if spark.catalog.tableExists("databrickspepi.silver.products_enr"):

    dlt_obj = DeltaTable.forName(spark, "databrickspepi.silver.products_enr")

    dlt_obj.alias("trg").merge(
        df_prod.alias("src"),
        col("trg.product_id") == col("src.product_id")
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else: 
    df_prod.write.format("delta") \
        .mode("append") \
        .saveAsTable("databrickspepi.silver.products_enr")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from databrickspepi.silver.products_enr

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Stores**

# COMMAND ----------

df_str = spark.read.format("csv")\
      .option("inferSchema", True)\
        .option("header", True)\
        .load("/Volumes/databrickspepi/bronze/bronze_volume/stores/")
display(df_str)

# COMMAND ----------

df_str = df_str.withColumn("store_name", regexp_replace(col("store_name"), "_",""))
display(df_str)


# COMMAND ----------

df_str = df_str.withColumn("processDate", current_timestamp())
display(df_str)


# COMMAND ----------

if spark.catalog.tableExists("databrickspepi.silver.stores_enr"):

    dlt_obj = DeltaTable.forName(spark, "databrickspepi.silver.stores_enr")

    dlt_obj.alias("trg").merge(
        df_str.alias("src"),
        col("trg.store_id") == col("src.store_id")
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else: 
    df_str.write.format("delta") \
        .mode("append") \
        .saveAsTable("databrickspepi.silver.stores_enr")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Sales**

# COMMAND ----------

df_sales = spark.read.format("csv")\
      .option("inferSchema", True)\
        .option("header", True)\
        .load("/Volumes/databrickspepi/bronze/bronze_volume/sales/")
display(df_sales)

# COMMAND ----------

df_sales = df_sales.withColumn("PricePerSale",round(col("total_amount")/col("quantity"),2))
df_sales = df_sales.withColumn("processDate", current_timestamp())
display(df_sales)


# COMMAND ----------

if spark.catalog.tableExists("databrickspepi.silver.sales_enr"):

    dlt_obj = DeltaTable.forName(spark, "databrickspepi.silver.sales_enr")

    dlt_obj.alias("trg").merge(
        df_sales.alias("src"),
        col("trg.sales_id") == col("src.sales_id")
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else: 
    df_sales.write.format("delta") \
        .mode("append") \
        .saveAsTable("databrickspepi.silver.sales_enr")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databrickspepi.silver.sales_enr

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Spark SQL**

# COMMAND ----------

df_sql=spark.sql("Select * from databrickspepi.silver.products_enr")

# COMMAND ----------

display(df_sql)

# COMMAND ----------

df_sql.createOrReplaceTempView("temp_products")

# COMMAND ----------

df_sql=spark.sql("""
          SELECT *,
                CASE
                WHEN category = 'Toys' THEN 'Yes' Else 'No' END As flag
                from temp_products
          """
          )

# COMMAND ----------

display(df_sql)

# COMMAND ----------

