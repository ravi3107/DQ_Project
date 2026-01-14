# Databricks notebook source
# MAGIC %md
# MAGIC Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="Parties"
department="Purchase"
DeltaLakePath="DeltaLake/Raw/Purchase/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("PartyId", IntegerType(), True),   
    StructField("PartyName", StringType(), True),   
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("PartyAddressCode", IntegerType(), True),
    StructField("EstablishedDate", TimestampType(), True),
    StructField("PartyEmailId", StringType(), True),
    StructField("PartyContactNumber", StringType(), True),
    StructField("RecordId", IntegerType(), True),
    StructField("TaxId", StringType(), True)   
])

# COMMAND ----------

parties_df=readEntity(department,entityName,schema)
display(parties_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(parties_df,DeltaLakePath)
