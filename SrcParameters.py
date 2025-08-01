# Databricks notebook source
# MAGIC %md
# MAGIC ### This is the input value to other notebooks to create a dynamic solution, especillay with the `BronzeLayer` notebook. A loop will ne run on this notebook to use the src value one by one. This is kind of  control flow.

# COMMAND ----------

# src_array = ["bookings", "airports", "customers", "flights"]

# COMMAND ----------

src_array =[
    {"src": "bookings"},
    {"src": "airports"},
    {"src": "customers"},
    {"src": "flights"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key= "output_key", value= src_array)