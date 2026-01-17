# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="WorkerTable"
department="HR"
DeltaLakePath=f"DeltaLake/Raw/{department}/{entityName}"

# COMMAND ----------

df=readFromDeltaPath(DeltaLakePath)
display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("bronze.workertable")
