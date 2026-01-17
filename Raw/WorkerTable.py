# Databricks notebook source
# MAGIC %md Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="WorkerTable"
department="HR"
DeltaLakePath="DeltaLake/Raw/HR/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("WorkerID", LongType(), True),  
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("SupervisorId", LongType(), True),
    StructField("WorkerName", StringType(), True),
    StructField("WorkerEmail", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("DOJ", TimestampType(), True),  
    StructField("DOL", TimestampType(), True),
    StructField("Vertical", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("PayPerAnnum", DoubleType(), True),
    StructField("Rate", DoubleType(), True),
    StructField("RecordId", LongType(), True)
])

# COMMAND ----------

WorkerTable_df=readEntity(department,entityName,schema)
display(WorkerTable_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(WorkerTable_df,DeltaLakePath)
