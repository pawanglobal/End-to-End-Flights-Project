from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="trans_passengers"
)

def trans_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/customers/data")
    return df

dlt.create_streaming_table(name="trans_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by=col("passenger_id"),
    stored_as_scd_type=1 # 1 for upsert, 2 for merge
)