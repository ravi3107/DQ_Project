# Databricks notebook source
# MAGIC %md
# MAGIC Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="PromoTable"
department="Sales"
DeltaLakePath="DeltaLake/Raw/Sales/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("PromotionId", LongType(), True),   
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("PromotionName", StringType(), True),
    StructField("PromoCode", LongType(), True),
    StructField("PromoType", StringType(), True),
    StructField("PromoPercentage", DoubleType(), True),
    StructField("ValidFrom", TimestampType(), True),
    StructField("ValidTo", TimestampType(), True),
    StructField("IsActive", LongType(), True),
    StructField("RecordId", LongType(), True)    
])

# COMMAND ----------

PromoTable_df=readEntity(department,entityName,schema)
display(PromoTable_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(PromoTable_df,DeltaLakePath)
