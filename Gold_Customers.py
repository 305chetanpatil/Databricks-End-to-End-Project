# Databricks notebook source
"""
-- Implementing a real-world solution for SCD Type-1 Dimension.
-- This notebook is designed to handle both initial (full) load and incremental load scenarios.
-- It ensures the dimension table is created during the first run and updated seamlessly on subsequent runs.

"""

# COMMAND ----------

# Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# initializing the initial load flag.
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# Data Reading from source(which is silver layer) 
df = spark.sql("select * from databricks_cata.silver.customers_silver")
df.display()

# COMMAND ----------

# Removing duplicates from the dataframe
df = df.dropDuplicates(subset=['customer_id'])  #because we need remove duplicates from column customer_id as it is a primary key.
display(df.limit(10))

# COMMAND ----------

'''
### Step: Filter Out Old vs New Records Before Assigning Surrogate Key:
Before generating surrogate keys for our dimension table, we need to identify which records are new and which are already present (old records). This is important for SCD Type-1 processing because we only update existing records and insert new ones.

### How to Identify New vs Old Records?
# If a gold dimension table already exists:
- Create a DataFrame from the existing dimension table.
- Perform a left join between the incoming DataFrame and the existing dimension table on the natural key (e.g., customer_id).
- Records with NULL values in the dimension columns after the join are new records.

# If this is an initial load (no existing table):
- All incoming records are considered new.
- However, we still want to handle this programmatically for future loads.

# Solution for Initial Load:
- Create a pseudo table that mimics the structure of the dimension table.
- This pseudo table should include:
    Joining key (e.g., customer_id)
    Audit columns (e.g., create_date, update_date)
    Surrogate key (if applicable) (e.g., DimCustomerKey)
- Use this pseudo table in the same join logic so that the code remains consistent for both initial and incremental loads.
'''

# Choose whether we have an existing dim table or it's the initial load
if init_load_flag == 0:
    # Incremental run: read existing dimension keys & natural keys from gold
    df_old = spark.sql("""
        SELECT
            DimCustomerKey,
            customer_id,
            -- If you want the stored column, use it (example: create_date)
            create_date,
            update_date
        FROM databricks_cata.gold.DimCustomers
    """)
else:
    # Initial run: create an empty pseudo table with the same schema
    # Ensures downstream joins work uniformly (schema-compatible, zero rows)
    df_old = spark.sql("""
        SELECT
            CAST(NULL AS BIGINT)   AS DimCustomerKey,
            CAST(NULL AS STRING)   AS customer_id,
            CAST(NULL AS DATE)     AS create_date,
            CAST(NULL AS TIMESTAMP) AS update_date
        FROM databricks_cata.silver.customers_silver
        WHERE 1 = 0
    """)

display(df_old)

# COMMAND ----------

# Renaming columns in df_old to avoid confusion during filtering operations
df_old = df_old.withColumnRenamed("customer_id", "customer_id_old")\
                .withColumnRenamed("create_date", "create_date_old")\
                .withColumnRenamed("update_date", "updated_date_old")\
                .withColumnRenamed("DimCustomerKey", "DimCustomerKey_old")
display(df_old)

# COMMAND ----------

# Applying join with the old records.
df_join = df.join(df_old, df['customer_id'] == df_old['customer_id_old'], "left")
display(df_join)


# COMMAND ----------

# Filtering out new records (those without a matching DimCustomerKey in df_old)
# We are selecting rows where DimCustomerKey_old is NULL, meaning these records exist only in the new dataset and have no match in the old one.
df_new = df_join.filter(df_join['DimCustomerKey_old'].isNull())
display(df_new)

# COMMAND ----------

# df_old means that needs to be added like this df_old is already added in our gold layer so that means we don't need to touch anything.
df_old = df_join.filter(df_join['DimCustomerKey_old'].isNotNull())
display(df_old)

# COMMAND ----------

# Preparing df_old for downstream processing

# 1) Drop unused columns to reduce clutter
df_old = df_old.drop('customer_id_old', 'updated_date_old')

# Renaming DimCustomerKey_old to 'DimCustomerKey'
df_old = df_old.withColumnRenamed("DimCustomerKey_old", "DimCustomerKey")

# 2) Normalize 'create_date':
#    - Rename from 'create_date_old' to 'create_date'
#    - Convert to proper timestamp for reliable time operations
df_old = df_old.withColumnRenamed("create_date_old", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))

# 3) Stamp the record with the current update time
df_old = df_old.withColumn("update_date", current_timestamp())

display(df_old)

# COMMAND ----------

# Preparing df_new

# dropping all the columns which are not required.
df_new = df_new.drop('DimCustomerKey_old', 'customer_id_old', 'create_date_old', 'updated_date_old')

# Recreating "update_date", "current_date" columns with current timestamp
df_new = df_new.withColumn("update_date", current_timestamp()).withColumn("create_date", current_timestamp())

display(df_new)


# COMMAND ----------

# Surrogate key - From 1.
df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(1))
display(df_new)

# COMMAND ----------

# Adding Max Surrogate Key.
if init_load_flag == 1:
    max_surrogate_key = 0

else:
    df_maxsurrogate_key = spark.sql("SELECT MAX(DimCustomerKey) AS max_surrogate_key from databricks_cata.gold.Dimcustomers")
    
    # Converting df_maxsurrogate_key to max_surrogate_key variable.
    max_surrogate_key = df_maxsurrogate_key.collect()[0]['max_surrogate_key']


# COMMAND ----------

# Add the max surrogate key to df_new as a new column to ensure continuity with the existing surrogate key sequence in the existing table.
df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key)+col("DimCustomerKey"))

# COMMAND ----------

# Union of df_new and df_old.
df_final = df_new.unionByName(df_old)  # unionByName is used to union two DataFrames based on their column names and data types.

display(df_final)


# COMMAND ----------

# SCD Type 1
from delta.tables import DeltaTable  # This is bacsically a detla table object that we create on top of our data just to create an instance of our table os that we can simply apply the merge condition on top of it.

if (spark.catalog.tableExists("databricks_cata.gold.Dimcustomers")):

    dlt_obj = DeltaTable.forPath(spark, "abfss://gold@databricksetestgacc.dfs.core.windows.net/Dimcustomers")
    print("Table already exists")

    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_final.write.mode("overwrite")\
        .format("delta")\
        .option("path","abfss://gold@databricksetestgacc.dfs.core.windows.net/Dimcustomers")\
        .saveAsTable("databricks_cata.gold.Dimcustomers")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.dimcustomers

# COMMAND ----------

# # This Cell is not part of the notebook's execution flow.(Only for understanding)

# # Surrogate key - All the values.
# '''
# ### **Assigning a Surrogate Key for SCD Type-1 Dimensions**
# In Slowly Changing Dimension (SCD) Type-1, it is a best practice to use a **surrogate key** instead of relying solely on the natural primary key (e.g., `customer_id`). A surrogate key is an artificial, system-generated identifier that uniquely represents each record in the dimension table. This helps maintain consistency and simplifies joins across fact and dimension tables.

# #### **Why Surrogate Key?**
# *   Natural keys (like `customer_id`) can change over time or may not be unique across systems.
# *   Surrogate keys ensure a stable and unique identifier for each dimension record.

# #### **How to Generate Surrogate Keys in Spark?**
# There are multiple ways to create surrogate keys in Spark:
# 1.  **Using `row_number()`**
# 2.  **Using `monotonic_increasing_id()`**

# #### **Best Practice**
# *   If you need a simple unique identifier without ordering, use `monotonic_increasing_id()`.
# *   If you require sequential numbering for reporting or debugging, use `row_number()`.
# '''

# df = df.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(1))
# # df.display()
