# Databricks notebook source
# MAGIC %run "../includes/common_configuration"

# COMMAND ----------

result = dbutils.notebook.run("1.ingest_circuits.csv_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

result = dbutils.notebook.run("2.ingest_race.csv_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

result = dbutils.notebook.run("3.ingest_constructors.json_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

result = dbutils.notebook.run("4.ingest_drivers.Json_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

result = dbutils.notebook.run("6.ingest_pit_stops_multiline.json_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

result = dbutils.notebook.run("7.ingest_lap_times_multiple.csv_files", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

result = dbutils.notebook.run("8.ingest_qualifying_multiple.json_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

output_df=spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(output_df)