from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="trans_flights"
)

def trans_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/flights/data")
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("flight_id"),
    stored_as_scd_type=1 # 1 for upsert, 2 for merge
)