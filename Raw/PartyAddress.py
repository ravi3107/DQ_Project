# Databricks notebook source
# MAGIC %md
# MAGIC Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="PartyAddress"
department="Purchase"
DeltaLakePath="DeltaLake/Raw/Purchase/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("PartyNumber", IntegerType(), True),   
    StructField("LastProcessedChange_DateTime", TimestampType(), True),   
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("ZipCode", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("ValidFrom", TimestampType(), True),
    StructField("ValidTo", TimestampType(), True),
    StructField("RecordId", IntegerType(), True)   
])

# COMMAND ----------

final_df=readEntity(department,entityName,schema)
display(final_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(final_df,DeltaLakePath)
