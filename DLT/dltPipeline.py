import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


#####################################################
##                  BOOKINGS                       ##
#####################################################

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


#####################################################
##                  FLIGHTS                        ##
#####################################################

@dlt.view(
    name="trans_flights"
)

def trans_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/flights/data")
    df = df.drop("_rescued_data")\
           .withColumn("modifiedDate", current_timestamp())
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1 # 1 for upsert, 2 for merge
)



#####################################################
##                  AIRPORTS                       ##
#####################################################

@dlt.view(
    name="trans_airports"
)

def trans_airports():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/airports/data")
    df = df.drop("_rescued_data")\
           .withColumn("modifiedDate", current_timestamp())
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1 # 1 for upsert, 2 for merge
)


#####################################################
##                  PASSENGERS                     ##
#####################################################

@dlt.view(
    name="trans_passengers"
)

def trans_passengers():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/customers/data")
    df = df.drop("_rescued_data")\
           .withColumn("modifiedDate", current_timestamp())
        
    return df

dlt.create_streaming_table(name="silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1 # 1 for upsert, 2 for merge
)


#############################################
##          SILVER BUSINESS VIEW           ##
#############################################

@dlt.table(
    name="silver_business_view"
)
def silver_business_view():
    df = dlt.readStream("silver_bookings")\
            .join(dlt.readStream("silver_flights"), ["flight_id"])\
            .join(dlt.readStream("silver_passengers"), ["passenger_id"])\
            .join(dlt.readStream("silver_airports"), ["airport_id"])\
            .drop("modifiedDate")
    return df


## EXTRA ##
@dlt.table(
    name="silver_business_mat"
)
def silver_business_mat():
    df = dlt.read("silver_business_view")
    return df