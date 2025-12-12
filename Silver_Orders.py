# Databricks notebook source
# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# Data Reading
df = spark.read.format("parquet").load("abfss://bronze@databricksetestgacc.dfs.core.windows.net/orders")
display(df)

# COMMAND ----------

df =  df.drop("_rescued_data")
display(df)


# COMMAND ----------

# Date Functions in PySpark
df = df.withColumn("order_date", to_timestamp(col("order_date")))
display(df)

# COMMAND ----------

# Creation of New Column (year)
df = df.withColumn("year", year(col("order_date")))
display(df)

# COMMAND ----------

# # Window Functions (Dense Rank, Rank, Row Number) (Manual way)
# df1 = df.withColumn("dense_rank",dense_rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
# df1 = df1.withColumn("rank",rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
# df1 = df1.withColumn("row_number",row_number().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
# display(df1)

# COMMAND ----------

# # Window Functions using Classes - OOPS
# class windows:

#     def dense_rank(self, df):
#         df_dense_rank = df.withColumn("dense_rank",dense_rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
#         return df_dense_rank

#     def rank(self, df):
#         df_rank = df.withColumn("rank",rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
#         return df_rank
    
#     def row_number(self, df):
#         df_row_number = df.withColumn("row_number",row_number().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
#         return df_row_number
    
# # Using class's object now you can call the functions for reusability purpose (If you have thousands of datasets)
# df_new = df
# obj = windows()
# df_new = obj.dense_rank(df_new)
# df_new = obj.rank(df_new)
# df_new = obj.row_number(df_new)
# display(df_new)


# COMMAND ----------

# Data Writing
df.write.format("delta").mode("overwrite").save("abfss://silver@databricksetestgacc.dfs.core.windows.net/orders")

# ------------------------------------------------------------------------------------------------
# # Instead of append we should use overwrite because we don't want to add new data, we everytime need to overwrite the data
# df.write.format("delta").mode("append").save("abfss://silver@databricksetestgacc.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating a managed table on top of existing Delta-formatted data
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksetestgacc.dfs.core.windows.net/orders'