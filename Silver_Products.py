# Databricks notebook source
# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Data Reading
df = spark.read.format("parquet").load("abfss://bronze@databricksetestgacc.dfs.core.windows.net/products")
df =  df.drop("_rescued_data")  # dropped the column _rescued_data
display(df)

# COMMAND ----------

# Created a temporary view that we can just use it, because let's say our function is created now if you want to use that function using SQL you should have some SQL objects
df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Functions: 
# MAGIC -- Using the Scalar Function (will be returning only one value that's why it is a scalar function)
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.discount_func(p_price double)
# MAGIC RETURNS double
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price * 0.90;
# MAGIC
# MAGIC -- We are getting price after applying 10% discount on the price column
# MAGIC select product_id, price, databricks_cata.bronze.discount_func(price) as discounted_price
# MAGIC from products;

# COMMAND ----------

# Functions using in dataframe directly
''' 
Points to remember:
1. expr() lets you use SQL-like expressions for column transformations within PySpark DataFrames.
'''
df = df.withColumn("discounted_price", expr("databricks_cata.bronze.discount_func(price)"))
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating a function using Python for brand column to make it uppercase.
# MAGIC -- First, create the function (run this cell separately)
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.upper_func(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$
# MAGIC   return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Then, in a new cell, run your SELECT statement
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   brand, 
# MAGIC   databricks_cata.bronze.upper_func(brand) AS brand_upper
# MAGIC FROM products

# COMMAND ----------

"""
@ Points to remember for the functions which are created in this notebook:
1. If you are storing your functions in Unity Catalog, here's what happens:
Location:
The functions are stored in the catalog and schema you specify when creating them. For example:
CREATE FUNCTION my_catalog.my_schema.my_function AS ...
This makes them persistent and governed.
Governance:
Unity Catalog provides centralized governance, meaning these functions are:
- Auditable
- Version-controlled
- Accessible across workspaces (with proper permissions)
Visibility:
You can view them in Databricks UI → Catalog → Schema → Functions or by running:
SHOW FUNCTIONS IN my_catalog.my_schema;

2. You should use this function only when you need to create a complex transformation or logic that cannot be easily achieved using standard PySpark functions. Additionally, if you want to save and govern it properly, you can manage it through Unity Catalog for better control and compliance.

"""

# COMMAND ----------

# Data Writing
df.write.format("delta").mode("overwrite").option("path", "abfss://silver@databricksetestgacc.dfs.core.windows.net/products").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating a managed table on top of existing Delta-formatted data
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksetestgacc.dfs.core.windows.net/products'