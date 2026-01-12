# Databricks notebook source
# MAGIC %md
# MAGIC Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="PurchContracts"
department="Purchase"
DeltaLakePath="DeltaLake/Raw/Purchase/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("ContractId", IntegerType(), True),   
    StructField("LastProcessedChange_DateTime", TimestampType(), True),   
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("FirstParty", StringType(), True),
    StructField("SecondParty", StringType(), True),
    StructField("ValidFrom", TimestampType(), True),
    StructField("ValidTo", TimestampType(), True),
    StructField("IsActive", IntegerType(), True),
    StructField("RecordId", IntegerType(), True)  
])

# COMMAND ----------

parties_df=readEntity(department,entityName,schema)
display(parties_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(parties_df,DeltaLakePath)
