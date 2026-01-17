# Databricks notebook source
# MAGIC %md
# MAGIC Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="Currency"
department="Others"
DeltaLakePath="DeltaLake/Raw/Others/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("CurrencyId", LongType(), True),   
    StructField("Code", StringType(), True),   
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("Country", StringType(), True),
    StructField("CurrencyName", StringType(), True),
    StructField("RecordId", LongType(), True) 
])

# COMMAND ----------

parties_df=readEntity(department,entityName,schema)
display(parties_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(parties_df,DeltaLakePath)
