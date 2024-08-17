# Databricks notebook source
# MAGIC %run "../includes/common_configuration"

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers").filter("number < 10"))

# COMMAND ----------

output_df = spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

filtered_df=output_df.filter(output_df["number"] < 10)

# COMMAND ----------

display(filtered_df)