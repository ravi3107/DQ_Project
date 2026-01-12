# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreateSchema").getOrCreate()

# COMMAND ----------

ADLS_DEV_BASE_PATH = "abfss://oaon-operations-dev@dqpadls3107.dfs.core.windows.net/"

# COMMAND ----------

dbutils.fs.ls("abfss://oaon-operations-dev@dqpadls3107.dfs.core.windows.net/")

# COMMAND ----------

def readEntity(department,entity,schema):
  df=spark.read.format("csv").schema(schema).option("path",f"abfss://oaon-operations-dev@dqpadls3107.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/{department}/{entity}/").load()
  return df

# COMMAND ----------

def writeEntity(entity_df,DeltaLakePath):
    entity_df.write.mode("overwrite").option("overwriteSchema","true").option("path",ADLS_DEV_BASE_PATH+DeltaLakePath).save()

# COMMAND ----------

def readFromDeltaPath(DeltaLakePath):
    df=spark.read.format("delta").option("path",ADLS_DEV_BASE_PATH+DeltaLakePath).load()
    return df

