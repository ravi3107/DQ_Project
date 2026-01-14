# Databricks notebook source
# MAGIC %md
# MAGIC Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="SalesOrderLine"
department="Sales"
DeltaLakePath="DeltaLake/Raw/Sales/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("SalesOrderNumber", LongType(), True),   
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("SalesOrderLine", LongType(), True),
    StructField("ItemId", LongType(), True),
    StructField("Qty", LongType(), True),
    StructField("Price", DoubleType(), True),
    StructField("VatPercentage", DoubleType(), True),
    StructField("CurrencyCode", StringType(), True),
    StructField("BookDate", TimestampType(), True),
    StructField("ShippedDate", TimestampType(), True),
    StructField("DeliveredDate", TimestampType(), True),
    StructField("TrackingNumber", LongType(), True),
    StructField("CustId", LongType(), True),
    StructField("PaymentTypeDesc", StringType(), True),
    StructField("RecordId", LongType(), True)  
])

# COMMAND ----------

SalesOrderLine_df=readEntity(department,entityName,schema)
display(SalesOrderLine_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(SalesOrderLine_df,DeltaLakePath)
