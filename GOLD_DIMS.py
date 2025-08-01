# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

# %sql
# SELECT * FROM workspace.silver.silver_flights;

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Parameters**
# MAGIC They are in string form. We can use eval to convert them into a list.

# COMMAND ----------

# # Catalog Name
# catalog = "workspace"

# # Key columns
# dbutils.widgets.text("keycols", "")

# # CDC column -> modifiedDate -> We will use this as it is
# dbutils.widgets.text("cdccol", "")

# # Back-dated refresh -> To backfill our dimensions until some particular date
# dbutils.widgets.text("backdated_refresh", "")

# # Source Object
# dbutils.widgets.text("source_object", "")

# # Sorce Schema
# dbutils.widgets.text("source_schema", "")


# COMMAND ----------

# val = dbutils.widgets.get("keycols")
# # Use eval to convert the string into a list
# key_col_list = eval(val)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Fetching parameters and creating variables based on the parameters**

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Solution with the loops

# COMMAND ----------

# # Key Cols list
# key_col_list = eval(dbutils.widgets.get("keycols"))

# # CDC Column
# cdc_col = dbutils.widgets.get("cdccol")

# # Backdated Refresh
# backdated_refresh = dbutils.widgets.get("backdated_refresh")

# # Source Object
# source_object = dbutils.widgets.get("source_object")

# # Soruce Schema
# source_schema = dbutils.widgets.get("source_schema")

# COMMAND ----------

# # Catalog Name
# catalog = "workspace"

# # Key Cols list
# key_cols_list = "['flight_id']"
# key_cols_list = eval(key_cols_list)

# # CDC Column
# cdc_col = "modifiedDate"

# # Backdated Refresh
# backdated_refresh = ""

# # Source Object
# source_object = "silver_flights"

# # Soruce Schema
# source_schema = "silver"

# # Target Schema
# target_schema = "gold"

# # Target Object
# target_object = "DimFlights"

# # Surrogate key
# surrogate_key = "DimFlightsKey"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Solution with the multi-threading -> No need to use the databricks loop

# COMMAND ----------

# # Catalog Name
# catalog = "workspace"

# # Key Cols list
# key_cols_list = "['airport_id']"
# key_cols_list = eval(key_cols_list)

# # CDC Column
# cdc_col = "modifiedDate"

# # Backdated Refresh
# backdated_refresh = ""

# # Source Object
# source_object = "silver_airports"

# # Soruce Schema
# source_schema = "silver"

# # Target Schema
# target_schema = "gold"

# # Target Object
# target_object = "DimAirports"

# # Surrogate key
# surrogate_key = "DimAirportsKey"

# COMMAND ----------

# Catalog Name
catalog = "workspace"

# Key Cols list
key_cols_list = "['passenger_id']"
key_cols_list = eval(key_cols_list)

# CDC Column
cdc_col = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

# Source Object
source_object = "silver_passengers"

# Soruce Schema
source_schema = "silver"

# Target Schema
target_schema = "gold"

# Target Object
target_object = "DimPassengers"

# Surrogate key
surrogate_key = "DimPassengersKey"

# COMMAND ----------

key_cols_list

# COMMAND ----------

# MAGIC %md
# MAGIC ## INCREMENTAL DATA INGESTION
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Last Load date

# COMMAND ----------

# If the backdated refresh is empty, then get the last load date
if len(backdated_refresh) == 0:
    
    # If the table exists in the destination, then get the last load date
    if spark.catalog.tableExists(f"catalog.{target_schema}.{target_object}"):
        last_load = spark.sql(f"SELECT max({cdc_col}) FROM workspace.{target_schema}.{target_object}").collect()[0][0] # collect converts the data into a list
    else:
    # If the table doesn't exist, then set the last load date to 1900-01-01 very far back in time
    # If the table doesn't exist, it means you need to bring in all the data from the source
        last_load = "1900-01-01 00:00:00"

# If the backdated refresh is not empty, then use the backdated refresh date
else:
    last_load = backdated_refresh

# Test the last load date
last_load

# COMMAND ----------

df_src = spark.sql(f" SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} > '{last_load}' ")

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OLD vs New records
# MAGIC
# MAGIC To identify which are the new and which are the old records to assign the surrogate key.
# MAGIC

# COMMAND ----------

# First check if the table exists
# We will apply the joint once we will have the Source (df_src) and the destination (dr_trg)
# We know that table doesn't exist in the destination we still want to apply the joint to do that we will create a pseudo table (empty table) and get the schema of the table and apply the joint.
# To do a joint, we need to have the same columns (necessary columns) in the source and the destination, we grab the dss_create_date dss_update_date and surrogate key.
# In general, we will pull all the keyCols, in our case it is only one, Cr/UpDate, SurrKeyCol

# We are going to check if the table exists in the destination
if spark.catalog.tableExists(f"catalog.{target_schema}.{target_object}"):

    # Key column string for incremental
    key_cols_string_incremental  = ','.join(key_cols_list)
    
    df_trg = spark.sql(f" SELECT {key_cols_string_incremental},{surrogate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}")

else:
    # As we know there is no table, so first we will create a pseudo table and get the schema of the table.
    
    # Key column string for Intial
    # We do not have the table so we will use the null values to create a table
    key_cols_string_init = [f"'' AS {i}" for i in key_cols_list ]
    key_cols_string_init = ", ".join(key_cols_string_init)

    # 1900-01-01 00:00:00 passing this value only to change the type of the column to timestamp
    df_trg = spark.sql( f"""SELECT {key_cols_string_init} , CAST('0' AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS timestamp) AS create_date, CAST('1900-01-01 00:00:00' AS timestamp) AS update_date WHERE 1=0 """)
                        

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Work to create empty tables
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We want to create create_date, update_date, etc..
# MAGIC -- We can't extract `flight_id` dirctly in the dynamic solution. What is the right way to perform this?
# MAGIC -- For that thing that's why we created the parameters earlier in the form of list comprehension, here we will only refer them, in the next cell we will use it.
# MAGIC -- This is the source we are just using it as a target for the experimentation.
# MAGIC
# MAGIC SELECT flight_id, '' as flight_name FROM workspace.silver.silver_flights;

# COMMAND ----------

# # To join the key columns we will use the join condition -> ' '.join(key_col_list) -> To grap the statement dynamically
# # if you have more than one column then just put simply one comma separated list. This is like a dynamic solution.
# # Then create date and update date; which is the same.
# # For the surrogate key we will create a parameter.

# key_cols_string_incremental  = ','.join(key_col_list) # here we are joining the key columns, using comma

# spark.sql(f"SELECT {key_cols_string_incremental}, {surrogate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}") # this is the thing we need to pull

# # But we don't have a table to run this code. We will have no key columns
# # So we need to create a table to run the code. We will do it in the following way:


# spark.sql(f"SELECT '' AS flight_id, '' AS DimFlightskey, '1900-01-01 00:00:00' AS create_date, '1900-01-01 00:00:00' AS update_date FROM workspace.silver.silver_flights WHERE 1=0").display() # 1=0 to only see the schema


# COMMAND ----------

df_trg.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Now we have our df_trg and df_src. We need simply to apply the join to find the old and new records.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Some work to apply join

# COMMAND ----------

# key_cols_list = "['flight_id']"
# key_cols_list = eval(key_cols_list)
# key_col_list

# COMMAND ----------

# At the moment this is like this

[f"src.{i} = trg.{i}" for i in key_cols_list]

# COMMAND ----------

# But we want like this
# We need one single string

' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])

# COMMAND ----------

# key_cols_list = key_col_string_incremental = ', '.join(key_col_list)
# spark.sql(f"SELECT {key_col_string_incremental} FROM {catalog}.{source_schema}.{source_object}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Join Condition

# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])
join_condition

# COMMAND ----------

# We will create two views where we will apply the join.
# We want all the columns from the source along with some columns from the target table which we created.
# From the target table we need surrogate key, create date and update date columns.

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
    SELECT src.*,
           trg.{surrogate_key},
           trg.create_date,
           trg.update_date
    FROM src
    LEFT JOIN trg
    ON {join_condition}
""")


# COMMAND ----------

df_join.display()

# COMMAND ----------

# Null in the DimKey column means these are the new records. They need to be updated.

# Old Records
df_old = df_join.filter(col(f"{surrogate_key}").isNotNull())

# New Records
df_new = df_join.filter(col(f"{surrogate_key}").isNull())

# COMMAND ----------

df_old.display()
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriching DataFrames

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing df_old 

# COMMAND ----------

# We need to keep everything as it is only need to update the update_date column.
# This is enriched version of our df_old

df_old_enr = df_old.withColumn("update_date", current_timestamp())
df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing df_new

# COMMAND ----------

df_new.display()

# COMMAND ----------

# How to find the max surrogate key?
# We are fetcing the maximum surrogate key from the table so that we can update the surrogate key for the new records
# The following code will only work if table exists, right now we don't have a any table and code will not work.

# spark.sql(f"""
#           SELECT max({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}
#           """).collect()[0][0]   # max surrogate key

# COMMAND ----------

# If the table exists
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"""SELECT max({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}""").collect()[0][0]

    df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
                    .withColumn("create_date", current_timestamp())\
                    .withColumn("update_date", current_timestamp())

else:
    # If the table doesn't exist
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
                    .withColumn("create_date", current_timestamp())\
                    .withColumn("update_date", current_timestamp())

# COMMAND ----------

df_new_enr.display()

# COMMAND ----------

df_old_enr.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Unioning Old and new records

# COMMAND ----------

df_old.printSchema()

# COMMAND ----------

# `unionByName` is used to combine the two DataFrames, `df_old_enr` and `df_new_enr`, into a single DataFrame called `df_union`. The `unionByName` method combines the two DataFrames based on the column names, rather than the column positions. This means that the resulting DataFrame will have the same schema as the original DataFrames, but with the rows from both DataFrames combined. `unionByName` is best to use when the data is not sorted. This is because the order of the rows in the DataFrames may not be the same, and `unionByName` will combine the rows based on their column names.

df_union = df_old_enr.unionByName(df_new_enr)


# COMMAND ----------

df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT

# COMMAND ----------

# Now we are ready to perform an UPSERT — i.e., update existing records or insert new ones.
# This step is crucial, especially when we perform **backdated data refreshes** on our table.

# The upsert operation uses a key column (e.g., a surrogate key) to identify matching records,
# and it also checks the **modified_date** to determine if the incoming record is newer.

# For example, suppose we have 110 records in our target table.
# After one month, a backdated refresh brings in a record with the same key (e.g., 90), 
# but an older value for one of its fields (e.g., the value was "XYZ" before, and is now "ABC").

# We do NOT want to overwrite "ABC" with the old value "XYZ" just because the key matched.
# That's why we use **modified_date** from the source as the source of truth.
# We compare this date with the existing record’s **modified_date** in the target,
# and only apply the upsert if the source record is more recent.

# Important: We should NOT rely on the update_date in the target table, 
# because it always reflects the latest load time, not the actual data change time.

# This logic mimics the **sequence-by column** behavior we see in Delta Live Tables (DLT).

# So, if the table already exists, we apply the upsert logic.
# If not, we create the table first and load the data.


# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# We can't create a external table on the managed volume
# If you want to create a external table you have to use the exact location of it

from delta.tables import DeltaTable

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    # Table exists — do MERGE
    # For external
    # dlt_obj = DeltaTable.forPath(spark, f"/Volumes/workspace/gold/gold_volume/{target_object}")
    # For managed table
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")

    dlt_obj.alias("tgt") \
        .merge(df_union.alias("src"), f"tgt.{surrogate_key} = src.{surrogate_key}") \
        .whenMatchedUpdateAll(condition=f"src.{cdc_col} = tgt.{cdc_col}") \
        .whenNotMatchedInsertAll() \
        .execute()

else:
    # Table does not exist — create managed table
    
    # For external table after .mode
    # .option("path", f"/Volumes/workspace/gold/gold_volume/{target_object}")\
        
    # We will create the managed table
    df_union.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.dimpassengers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Builder Complete!
# MAGIC
# MAGIC Our dimension builder is now ready — this is the **backbone** of our project.
# MAGIC
# MAGIC We’ve implemented **Slowly Changing Dimension (SCD) Type 1** logic, allowing you to:
# MAGIC - Convert any source table into a dimension table
# MAGIC - Control the behavior just by changing a few parameters
# MAGIC
# MAGIC You can now:
# MAGIC - **Run it multiple times** to see the **idempotent behavior**
# MAGIC - Play with different datasets or schemas
# MAGIC - Extend it for additional SCD types or audit logging if needed
# MAGIC
# MAGIC This builder is **flexible**, **parameter-driven**, and ready to support production-grade data pipelines.
# MAGIC
# MAGIC
# MAGIC