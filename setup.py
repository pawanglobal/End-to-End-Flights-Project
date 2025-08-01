# Databricks notebook source
# MAGIC %sql
# MAGIC -- To create MANAGED VOLUME
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.raw.rawvolume

# COMMAND ----------

# Create a folder in MANGED VOLUME to uplad the data files
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume")

# COMMAND ----------

# Create subdirectories to uplad the data files of different catogories, here we have 4.
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/customers")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/flights")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/bookings")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCHEMAS FOR DIFFERENT LAYERS
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Loading the data
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronze_volume/airports/data/`