# Databricks notebook source
'''
* DLT pipeline should use 8 cores.
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Pipeline

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# Expectations 
my_rules = {
    "rule1": "NOT NULL",
    "rule2": "NOT NULL"
}

# COMMAND ----------

# Streaming Table 

@dlt.table
@dlt.except_all_or_drop(my_rules)
def DimProducts_stage():
    df = spark.readStream.table("databricks_cata.silver.products_silver")
    return df


# COMMAND ----------

# Streaming View
@dlt.view
def DimProducts_view():
    df = spark.readStream.table("Live.DimProducts_stage")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimProducts

# COMMAND ----------

dlt.create_streaming_table("DimProducts") # It will create empty streaming table as we have not written any data yet.
  

# COMMAND ----------

dlt.apply_changes(
source= "Live.DimProducts_view",
target= "DimProducts",
keys = ["product_id"],
sequence_by = "product_id",
stored_as_scd_type = 2,
)

# COMMAND ----------

