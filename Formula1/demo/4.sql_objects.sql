-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE default

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed Table 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Creating Managed Table By Python

-- COMMAND ----------

-- MAGIC %run "../includes/common_configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC EXTENDED demo.race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Creating Managed Table By SQL

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS SELECT * FROM demo.race_results_python WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External Table 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### External Table By Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_result_ext_py").saveAsTable("demo.race_result_ext_py")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.parquet(f"{presentation_folder_path}/race_result_ext_py")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{presentation_folder_path}"))

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_result_ext_py;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### External Table By SQL 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.parquet(f"{presentation_folder_path}/race_result_ext_py")

-- COMMAND ----------

CREATE TABLE demo.race_result_ext_sql
(
  race_year int, 
  race_name string, 
  race_date timestamp, 
  circuit_location string, 
  driver_name string, 
  driver_number int, 
  driver_nationality string, 
  team string, 
  grid int, 
  fastest_lap int, 
  race_time string, 
  points float, 
  position int,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formula1dldbricksrg/presentation/race_result_ext_sql"


-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_result_ext_sql
SELECT * FROM demo.race_result_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(*) FROM demo.race_result_ext_sql