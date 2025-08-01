# Databricks notebook source
# MAGIC %md
# MAGIC ### **INCREMENTAL DATA INGESTION**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To uplad the raw data
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.raw.bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.raw.silver;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.raw.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Architectural layers's vlumes
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.gold.bronze_volume;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.gold.silver_volume;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.gold.gold_volume;

# COMMAND ----------

# #  AUTOLOADER -- this is a static solution, but it would be dynamic, for each data catogory
# df = spark.readStream.format("cloudfiles")\
#     .option("cloudFiles.format","csv")\
#     .option("cloudFiles.schemaLocation","/Volumes/workspace/bronze/bronze_volume/airports/checkpoint")\
#     .option("cloudFiles.schemaEvolutionMode", "rescue")"\
#     .load("/Volumes/workspace/raw/rawvolume/rawdata/airports")

# COMMAND ----------


# df = spark.readStream.format("cloudfiles")\
#     .option("cloudFiles.format","csv")\
#     .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/bronze_volume/bookings/checkpoint")\
#     .option("cloudFiles.schemaEvolutionMode", "rescue")\
#     .load("/Volumes/workspace/raw/rawvolume/rawdata/bookings/")

# COMMAND ----------

# Write the data in bronze layer - in PySpare we need to write the data
# We will write in delta format

# df.writeStream.format("delta")\
#     .outputMode("append")\
#     .trigger(once=True)\
#     .option("checkpointLocation", "/Volumes/workspace/bronze/bronze_volume/bookings/checkpoint")\
#     .option("path", "/Volumes/workspace/bronze/bronze_volume/bookings/data")\
#     .start()

# COMMAND ----------

# MAGIC  %sql
# MAGIC -- We can query and check the data
# MAGIC
# MAGIC -- SELECT * FROM delta. `/Volumes/workspace/bronze/bronze_volume/bookings/data`

# COMMAND ----------

# MAGIC %md
# MAGIC ### To uplad data dynamically we will use the widgets

# COMMAND ----------

dbutils.widgets.text("src", "")
src_value = dbutils.widgets.get("src")
print(f"Received input parameter: src = '{src_value}'")

# COMMAND ----------

src_value = dbutils.widgets.get("src")
src_value

# COMMAND ----------

df = spark.readStream.format("cloudfiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronze_volume/{src_value}/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "rescue")\
    .load(f"/Volumes/workspace/raw/rawvolume/rawdata/{src_value}")

# COMMAND ----------

query = df.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", f"/Volumes/workspace/bronze/bronze_volume/{src_value}/checkpoint")\
    .option("path", f"/Volumes/workspace/bronze/bronze_volume/{src_value}/data")\
    .start()

query.awaitTermination()

# COMMAND ----------

# MAGIC  %sql
# MAGIC -- We can query and check the data
# MAGIC
# MAGIC -- SELECT * FROM delta. `/Volumes/workspace/bronze/bronze_volume/bookings/data`