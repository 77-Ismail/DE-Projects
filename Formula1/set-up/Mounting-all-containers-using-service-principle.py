# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

def mountstorage(storagename,containername):

    if any(mount.mountPoint == f"/mnt/{storagename}/{containername}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storagename}/{containername}")
    
    client_id = dbutils.secrets.get(scope='formula1-scope1',key='client-id')
    tenant_id = dbutils.secrets.get(scope='formula1-scope1',key='tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope1',key='client-secret')

    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
   

    dbutils.fs.mount(
        source = f"abfss://{containername}@{storagename}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storagename}/{containername}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mountstorage('formula1dldbricksrg','presentation')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1dldbricksrg/demo'))

# COMMAND ----------

display(spark.read.csv('/mnt/formula1dldbricksrg/demo/circuits.csv'))