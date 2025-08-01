import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# No transformations are applied here, just read the data from bronze layer and write it to silver layer
@dlt.table(
    name="stage_bookings"
)
def stage_bookings():
  
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/bookings/data")

    return df

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

# Expectations or rules
rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

# Streaming table
@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all_or_drop(rules)
# If the table is not following the rules, we get warning (by default), it fails, or drop records
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df