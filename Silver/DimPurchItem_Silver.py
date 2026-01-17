# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

PurchItem_df=spark.table("bronze.purchitem")
display(PurchItem_df)

# COMMAND ----------

dimPurchItem_df=PurchItem_df.filter(PurchItem_df.RecordId.isNotNull()).select(
    PurchItem_df.ItemId,
    trim(PurchItem_df.Txt).alias("Txt"),
    when(PurchItem_df.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(PurchItem_df.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
    from_utc_timestamp(PurchItem_df.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
    from_utc_timestamp(PurchItem_df.ValidFrom,"CST").alias("ValidFrom"),
    from_utc_timestamp(PurchItem_df.ValidTo,"CST").alias("ValidTo"),
    PurchItem_df.Price.alias("ProductPerUnitCost"),
    PurchItem_df.RecordId.alias("PurchItemRecordId"),
    PurchItem_df.CategoryID   
).withColumn("UpdatedDateTime",lit(UpdatedDateTime)
).withColumn("PurchItemHashKey",xxhash64(col("PurchItemRecordId")))

display(dimPurchItem_df)

# COMMAND ----------

df_final=dimPurchItem_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimpurchitem")
