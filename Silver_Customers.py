# Databricks notebook source
# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Data Reading
df = spark.read.format("parquet").load("abfss://bronze@databricksetestgacc.dfs.core.windows.net/customers")
df =  df.drop("_rescued_data")  # dropped the column _rescued_data
display(df)

# COMMAND ----------

# Transformation on email column to get the domain part of the email address using split function.
# Split -> Array Indexing
df = df.withColumn("domains", split(df.email, "@")[1])
display(df)

# COMMAND ----------

# # Aggregation Functions - (Count, GroupBy, Sort, Descending)
# df_domain = df.groupBy("domains").agg(count("customer_id").alias("total_customers")).sort(desc("total_customers"))
# display(df_domain)

# COMMAND ----------

# # Fetching users with gmail, yahoo and hotmail domains using filter function.
# df_gmail = df.filter(col("domains") == "gmail.com")
# display(df_gmail)

# df_yahoo = df.filter(col("domains") == "yahoo.com")
# display(df_yahoo)

# df_hotmail = df.filter(col("domains") == "hotmail.com")
# display(df_hotmail)

# COMMAND ----------

# Creating new column "full_name" using concat function and dropping the columns "first_name" and "last_name" using drop function.
df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df = df.drop("first_name", "last_name")
display(df)


# COMMAND ----------

# Data Writing
df.write.format("delta").mode("overwrite").save("abfss://silver@databricksetestgacc.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating a managed table on top of existing Delta-formatted data
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksetestgacc.dfs.core.windows.net/customers'

# COMMAND ----------

