# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/common_configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/formula1dldbricksrg/raw/races.csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                                  .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'),col('data_source'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1dldbricksrg/processed/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dldbricksrg/processed/races

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC ls /mnt/formula1dldbricksrg/processed
# MAGIC