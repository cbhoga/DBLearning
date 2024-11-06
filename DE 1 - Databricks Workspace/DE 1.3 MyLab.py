# Databricks notebook source
# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

assert my_name is not None, "My name is still not None"
print(my_name)

# COMMAND ----------

display(example_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc_taxi

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")

display(files)

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

print(my_name)

# COMMAND ----------

assert my_name is not None, "My name is still not None"
print(my_name)
