# Databricks notebook source
# MAGIC %md
# MAGIC Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="PurchaseOrder"
department="Purchase"
DeltaLakePath="DeltaLake/Raw/Purchase/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("PoNumber", StringType(), True),   
    StructField("LineItem", IntegerType(), True),   
    StructField("VendId", IntegerType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("Qty", IntegerType(), True),
    StructField("PurchasePrice", StringType(), True),
    StructField("TotalOrder", StringType(), True),
    StructField("ExchangeRate", StringType(), True),
    StructField("CostCenter", StringType(), True),  
    StructField("Itemkey", StringType(), True),
    StructField("currencycode", StringType(), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("ShipDate", TimestampType(), True),
    StructField("DeliveredDate", TimestampType(), True),
    StructField("TrackingNumber", StringType(), True),
    StructField("Batchid", StringType(), True),
    StructField("CreatedBy", StringType(), True),
    StructField("RecordId", IntegerType(), True),
    StructField("CategoryId", IntegerType(), True)
])

# COMMAND ----------

parties_df=readEntity(department,entityName,schema)
display(parties_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(parties_df,DeltaLakePath)
