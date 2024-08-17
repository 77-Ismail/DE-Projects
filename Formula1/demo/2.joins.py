# Databricks notebook source
# MAGIC %run "../includes/common_configuration"

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}"))

# COMMAND ----------

circuit_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races").filter("race_year == 2019")\
    .withColumnRenamed("name","races_name") 

# COMMAND ----------

## Inner Join

# COMMAND ----------

join_df=circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "inner")\
    .select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.races_name,races_df.round) 

# COMMAND ----------

display(join_df)

# COMMAND ----------

circuit_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id < 70")\
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races").filter("race_year == 2019")\
    .withColumnRenamed("name","races_name") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner Join

# COMMAND ----------

inner_join_df=circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "inner")\
    .select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.races_name,races_df.round)

# COMMAND ----------

display(inner_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outer Joins

# COMMAND ----------

# MAGIC %md
# MAGIC #### Left Outer Join

# COMMAND ----------

left_outer_join_df=circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "left")\
    .select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.races_name,races_df.round)

# COMMAND ----------

display(left_outer_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Right Outer Join

# COMMAND ----------

right_outer_join_df=circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "right")\
    .select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.races_name,races_df.round)

# COMMAND ----------

display(right_outer_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Outer Join

# COMMAND ----------

full_outer_join_df=circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "full")\
    .select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.races_name,races_df.round)

# COMMAND ----------

display(full_outer_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Join

# COMMAND ----------

semi_join_df=circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(semi_join_df)

# COMMAND ----------

semi_join_df=races_df.join(circuit_df, races_df.circuit_id == circuit_df.circuit_id, "semi")

# COMMAND ----------

display(semi_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti Join
# MAGIC It Gives those rows not found in the right dataframe.

# COMMAND ----------

anti_join_df=circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(anti_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Join

# COMMAND ----------

cross_join_df=circuit_df.crossJoin(races_df)

# COMMAND ----------

display(cross_join_df)

# COMMAND ----------

int(circuit_df.count()) * int(races_df.count())

# COMMAND ----------

