# Databricks notebook source
# MAGIC %md
# MAGIC ## FACT TABLES

# COMMAND ----------

# Catalog Name
catalog = "workspace"

# Soruce Schema
source_schema = "silver"

# Source Object
source_object = "silver_bookings"

# CDC Column
cdc_columns = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

# Source Fact Table
fact_table = f"{catalog}.{source_schema}.{source_object}"

# Target Schema
target_schema = "gold"

# Target Object
target_object = "FactBookings"

# Surrogate key
surrogate_key = "DimBookingsKey"

# Fact Key Cols List
fact_key_cols = ["DimPassengersKey", "DimFlightsKey", "DimAirportsKey"]

# COMMAND ----------

# To create dimension keys dynamically for the fact tables
# We have parmater dimensions from which we will pass all the arrays 
# This particular dimension information would be coming from the user
# Dim passengers, Dim Flights, Dim Airports we will get from the user

dimensions = [
    {
        "table": f"{catalog}.{target_schema}.Dimpassengers",
        "alias": "DimPassengers",
        # this information comes from the user which tables we want to join (key_columns)
        "join_keys": [("passenger_id", "passenger_id")] # (fact_col, dim_col)
    },
    {
        "table": f"{catalog}.{target_schema}.DimFlights",
        "alias": "DimFlights",
        "join_keys": [("flight_id", "flight_id")] # (fact_col, dim_col)
    },
    {
        "table": f"{catalog}.{target_schema}.DimAirports",
        "alias": "DimAirports",
        "join_keys": [("airport_id", "airport_id")] # (fact_col, dim_col)
    }
]

# Columns you want to keep from Fact table (besides the surrogate keys) -> Numeric columns + surrogate keys
fact_columns = ["amount", "booking_date", "modifiedDate"]


# COMMAND ----------

# MAGIC %md
# MAGIC ### Last Load Date

# COMMAND ----------

# No Back Dated Refresh
if len(backdated_refresh)==0:

    # If Table Exists In the destination
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
        
        last_load = spark.sql(f"SELECT max({cdc_columns}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]

    else:
        last_load = "1900-01-01 00:00:00"

# Yes Back Dated Refresh
else:
    last_load = backdated_refresh

# Test The Last Load
last_load

# COMMAND ----------

# MAGIC %md
# MAGIC ### DYNAMIC FACT QUERY [BRING KEYS]

# COMMAND ----------

def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_columns, processing_data):
    """
    This function converst the functional query to the sequel query
    """
    fact_alias = "f"

    # Base columns to select
    select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    # Build joins dynamically
    join_clauses = []
    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]
        table_name = table_full.split(".")[-1]
        surrogate_key = f"{alias}.{table_name}Key"
        select_cols.append(surrogate_key)

        # Build ON clauses
        on_conditions = [
            f"{fact_alias}.{fk} = {alias}.{dk}"
            for fk, dk in dim["join_keys"]
        ]
        join_clause = f"LEFT JOIN {table_full} AS {alias} ON " + "  AND ".join(on_conditions)
        join_clauses.append(join_clause)
    
    # Final SELECT and JOIN clauses
    select_clause = ",\n  ".join(select_cols)
    joins = "\n".join(join_clauses)

    # WHERE clauses for incremental filtering
    where_clause = f"{fact_alias}.{cdc_columns} >= DATE('{last_load}')"

    # Final query
    query = f"""
SELECT
  {select_clause}
FROM {fact_table} AS {fact_alias}
{joins}
WHERE {where_clause}
""".strip()
    return query

# COMMAND ----------

query = generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_columns, last_load)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DF_FACT

# COMMAND ----------

print(query)

# COMMAND ----------

df_fact = spark.sql(query)


# COMMAND ----------

df_fact.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT
# MAGIC We usually do not perform upsert on fact tables but yes sometimes it is important

# COMMAND ----------

# Fact key Columns Merge Condition
fact_key_cols
fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
        .whenMatchedUpdateAll(condition=f"src.{cdc_columns} > trg.{cdc_columns}")\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.factbookings

# COMMAND ----------

# Check duplicates
df = spark.sql(f"SELECT * FROM workspace.gold.dimairports").groupby("DimAirportsKey").count().filter("count > 1")
df.display()


# COMMAND ----------

df = spark.sql(f"SELECT * FROM workspace.gold.dimflights").groupby("DimFlightsKey").count().filter("count > 1")
df.display()

# COMMAND ----------

df = spark.sql(f"SELECT * FROM workspace.gold.dimpassengers").groupby("DimPassengersKey").count().filter("count > 1")
df.display()

# COMMAND ----------

