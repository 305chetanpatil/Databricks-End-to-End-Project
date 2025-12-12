# Databricks notebook source
'''
# In this notebook, we will learn how to read data from a Delta table in an efficient and organized manner.
Our example will use the regions table, which is stored as a Delta table.

# 📌 Special Requirement: Regions Mapping File
Context
The regions file is not part of the star schema.
It is a static mapping file that provides reference information for regions.
Even though it is not a fact or dimension table, it needs to travel through the Medallion Architecture 
(Bronze → Silver → Gold).
The purpose is to serve this file to stakeholders in the Gold layer for reporting and analytics.
'''


# COMMAND ----------

df = spark.read.table("databricks_cata.bronze.regions")
df =  df.drop("_rescued_data")  # dropped the column _rescued_data
display(df)

# COMMAND ----------

# Data Writing
# It can be overwritten because it just a samll and static file it can be overwritten everytime.
df.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@databricksetestgacc.dfs.core.windows.net/regions")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating a managed table on top of existing Delta-formatted data
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksetestgacc.dfs.core.windows.net/regions'