from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="trans_airports"
)

def trans_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronze_volume/airports/data")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("airport_id"),
    stored_as_scd_type=1 # 1 for upsert, 2 for merge
)