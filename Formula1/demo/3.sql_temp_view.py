# Databricks notebook source
# MAGIC %md
# MAGIC ### Local Temp View
# MAGIC Local temp view is available in only spark session and not being accessed in another notebook

# COMMAND ----------

# MAGIC %run "../includes/common_configuration"
# MAGIC

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_race_result
# MAGIC WHERE race_year = 2020
# MAGIC order by points desc 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Another way to execute SQL Code

# COMMAND ----------

final_df=spark.sql("SELECT * FROM v_race_result WHERE race_year=2020 ")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### The Best part we have in this if we want to pass the year in this by another variable we can do

# COMMAND ----------

year = 2019

# COMMAND ----------

final_df=spark.sql(f"SELECT * FROM v_race_result WHERE race_year= {year}")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temp View
# MAGIC Local temp view is available to whole application, to all the notebooks attached to the cluster.

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM global_temp.gv_race_result

# COMMAND ----------

display(spark.sql("SELECT * FROM global_temp.gv_race_result"))