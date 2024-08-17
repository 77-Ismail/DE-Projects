# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope1')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope1',key='client-secret')

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope1',key='client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope1',key='tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope1',key='client-secret')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formula1dldbricksrg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dldbricksrg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dldbricksrg.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dldbricksrg.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dldbricksrg.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dldbricksrg.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dldbricksrg.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

