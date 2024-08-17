# Databricks notebook source
# MAGIC %run "../includes/common_configuration"

# COMMAND ----------

result = dbutils.notebook.run("1.ingest_circuits.csv_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

result = dbutils.notebook.run("2.ingest_race.csv_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

result = dbutils.notebook.run("3.ingest_constructors.json_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

result = dbutils.notebook.run("4.ingest_drivers.Json_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
# MAGIC