# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

CostCenter_df=spark.table("bronze.costcenter")
display(CostCenter_df)

# COMMAND ----------

dimCostCenter_df=CostCenter_df.filter(CostCenter_df.RecordId.isNotNull()).select(
    CostCenter_df.CostCenterNumber,
    when(CostCenter_df.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(CostCenter_df.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
    from_utc_timestamp(CostCenter_df.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
    CostCenter_df.Vat,
    CostCenter_df.RecordId.alias("CostCenterRecordId")    
).withColumn("UpdatedDateTime",lit(UpdatedDateTime)
).withColumn("CostCenterHashKey",xxhash64(col("CostCenterRecordId")))

display(dimCostCenter_df)

# COMMAND ----------

df_final=dimCostCenter_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimcostcenter")
