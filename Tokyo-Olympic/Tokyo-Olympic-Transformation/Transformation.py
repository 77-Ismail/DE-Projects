# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType,DoubleType,FloatType


# COMMAND ----------

# MAGIC %run "../includes/common-configuration"

# COMMAND ----------

def mountstorage(storagename,containername,foldername):

    if any(mount.mountPoint == f"/mnt/{storagename}/{containername}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storagename}/{containername}")
    
    client_id = dbutils.secrets.get(scope='tokyo-olympic-scope',key='client-id')
    tenant_id = dbutils.secrets.get(scope='tokyo-olympic-scope',key='tenant-id')
    client_secret = dbutils.secrets.get(scope='tokyo-olympic-scope',key='client-secret')

    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
   

    dbutils.fs.mount(
        source = f"abfss://{containername}@{storagename}.dfs.core.windows.net/",
        mount_point = f"/mnt/tokyoolympic",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolympic"

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").load(f'{raw_folder_path}/athletes.csv')
coaches = spark.read.format("csv").option("header","true").load(f'{raw_folder_path}/coaches.csv')
entriesgender = spark.read.format("csv").option("header","true").load(f'{raw_folder_path}/entriesgender.csv')
medals = spark.read.format("csv").option("header","true").load(f'{raw_folder_path}/medals.csv')
teams = spark.read.format("csv").option("header","true").load(f'{raw_folder_path}/teams.csv')

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

athletes.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

coaches.show()


# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
                .withColumn("Male",col("Male").cast(IntegerType()))\
                .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

entriesgender.show()


# COMMAND ----------

medals.printSchema()

# COMMAND ----------

medals = medals.withColumn("Gold",col("Gold").cast(IntegerType()))\
                .withColumn("Silver",col("Silver").cast(IntegerType()))\
                .withColumn("Bronze",col("Bronze").cast(IntegerType()))\
                .withColumn("Total",col("Total").cast(IntegerType()))\
                .withColumn("Rank by Total",col("Rank by Total").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Another way of doing this

# COMMAND ----------

medals = spark.read.format("csv").option("inferSchema","true").option("header","true").load(f'{raw_folder_path}/medals.csv')

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

medals.show()


# COMMAND ----------

teams.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

athletes.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/athletes")
coaches.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/coaches")
entriesgender.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/entriesgender")
medals.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/medals")
teams.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/teams")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/tokyoolympic/transformed_data/"))