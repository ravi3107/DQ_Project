# Databricks notebook source
dbutils.fs.ls("abfss://oaon-operations-dev@dqpadls3107.dfs.core.windows.net/")

# COMMAND ----------

df=spark.read.format("csv").option("path","abfss://oaon-operations-dev@dqpadls3107.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/Parties/").load()
display(df)


# COMMAND ----------

#service_credential=dbutils.secrets.get(scope = "MyScope", key = "ClientSecret")
#appid=dbutils.secrets.get(scope = "MyScope", key = "appid")
#tenantid=dbutils.secrets.get(scope = "MyScope", key = "tenantid")

#df = (spark.read.format("com.microsoft.cdm").option("spark.sql.sources.useV2", "true")
 # .option("storage", "dqpadls3107.dfs.core.windows.net")
  #.option("appid", appid) 
  #.option("appKey", service_credential)
  #.option("tenantid", tenantid)
  #.option("manifestPath", "oaon-operations-dev/oaon-sandbox.operations.dynamics.com/Tables/Purchase/Purchase.manifest.cdm.json")
  #.option("entity", "Parties").load())

#display(df)
