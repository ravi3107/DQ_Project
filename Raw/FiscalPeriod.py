# Databricks notebook source
# MAGIC %md
# MAGIC Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

entityName="FiscalPeriod"
department="Others"
DeltaLakePath="DeltaLake/Raw/Others/"+entityName

# COMMAND ----------

schema = StructType([
    StructField("FiscalPeriodName", StringType(), True),      
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("FiscalStartDate", TimestampType(), True),
    StructField("FiscalEndDate", TimestampType(), True),
    StructField("FiscalMonth", LongType(), True),
    StructField("FiscalYearStart", TimestampType(), True),
    StructField("FiscalYearEnd", TimestampType(), True),
    StructField("FiscalQuarter", LongType(), True),
    StructField("FiscalQuarterStart", TimestampType(), True),
    StructField("FiscalQuarterEnd", TimestampType(), True),
    StructField("FiscalYear", LongType(), True),
    StructField("RecordId", LongType(), True)   
])

# COMMAND ----------

parties_df=readEntity(department,entityName,schema)
display(parties_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
writeEntity(parties_df,DeltaLakePath)
