# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# # load the data from bronze layer and see

# df = spark.read.format("delta")\
#     .load("/Volumes/workspace/bronze/bronze_volume/bookings/data")

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloaders create `_rescued_data` column automatically so we need to drop it
# MAGIC ### `amount` column is in string format
# MAGIC ### We will add one more column, to give information about the time when the data was last processed

# COMMAND ----------

# Transformations

# df = df.withColumn("amount",col("amount").cast(DoubleType()))\
#     .drop("_rescued_data")\
#     .withColumn("booking_date", to_date(col("booking_date")))\
#     .withColumn("modifiedDate", current_timestamp())

# display(df)

# COMMAND ----------

# We can't import dlt within a notebook

import dlt

# COMMAND ----------

# No transformations are applied here, just read the data from bronze layer and write it to silver layer
# It will load the data incremently

@dlt.table(
    name="stage_bookings" # No need to give, it just increase the readability
)
def stage_bookings():
    # In order to create a streaming table, we need to use readStream
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/bookings/data")

    return df

# COMMAND ----------

# Now we will create a view, and apply transformations on the top of our data frame

@dlt.view(
    name="trans_bookings"
)
def trans_bookings():
  
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount",col("amount").cast(DoubleType()))\
        .drop("_rescued_data")\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .withColumn("modifiedDate", current_timestamp())

    return df

# COMMAND ----------

# Expectations or rules
rules = {
    "rule1": "booking_id IS NOT NULL"
    "rule2": "passenger_id IS NOT NULL"
}

# COMMAND ----------

# Streaming table
@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all_or_drop(rules)
# If the table is not following the rules, we get warning (by default), it fails, or drop records
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df

# COMMAND ----------

# We will use our notebook to explore the data and then export it to other files.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronze_volume/flights/data")

df = df.withColumn("flight_date", to_date(col("flight_date")))\
    .withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronze_volume/customers/data")

df = df.withColumn("flight_date", to_date(col("flight_date")))\
    .withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronze_volume/airports/data")

df = df.withColumn("flight_date", to_date(col("flight_date")))\
    .withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### After loading the data to the silver layer we can query it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_airports