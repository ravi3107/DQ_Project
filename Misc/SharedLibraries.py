# Databricks notebook source
ADLS_DEV_BASE_PATH = "abfss://oaon-operations-dev@dqpadls3107.dfs.core.windows.net/"

# COMMAND ----------

dbutils.fs.ls("abfss://oaon-operations-dev@dqpadls3107.dfs.core.windows.net/")

# COMMAND ----------

def readEntity(department,entity):
  df=spark.read.format("csv").option("path",f"abfss://oaon-operations-dev@dqpadls3107.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/{department}/{entity}/").load()
  return df

# COMMAND ----------

def writeEntity(entity_df,DeltaLakePath):
    entity_df.write.mode("overwrite").option("overwriteSchema","true").option("path",ADLS_DEV_BASE_PATH+DeltaLakePath).save()
