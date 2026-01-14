# Databricks notebook source
# MAGIC %md
# MAGIC Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="CustTable"
department="Sales"
DeltaLakePath="DeltaLake/Raw/Sales/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("CustomerId", LongType(), True),   
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("ZipCode", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("SignupDate", TimestampType(), True),
    StructField("RecordId", LongType(), True)    
])

# COMMAND ----------

CustTable_df=readEntity(department,entityName,schema)
display(CustTable_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(CustTable_df,DeltaLakePath)
