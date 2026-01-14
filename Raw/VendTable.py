# Databricks notebook source
# MAGIC %md Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="VendTable"
department="Purchase"
DeltaLakePath="DeltaLake/Raw/Purchase/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("VendId", IntegerType(), True),   
    StructField("VendorName", StringType(), True),   
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
    StructField("Active", IntegerType(), True),
    StructField("RecordId", IntegerType(), True),
    StructField("TaxId", StringType(), True),
    StructField("CurrencyCode", StringType(), True)
])

# COMMAND ----------

parties_df=readEntity(department,entityName,schema)
display(parties_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(parties_df,DeltaLakePath)
