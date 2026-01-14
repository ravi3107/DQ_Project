# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

PurchaseCategory_df=spark.table("bronze.purchcategory")
display(PurchaseCategory_df)

# COMMAND ----------

dimPurchaseCategory_df=PurchaseCategory_df.filter(PurchaseCategory_df.RecordId.isNotNull()).select(
    PurchaseCategory_df.CategoryId,
    trim(PurchaseCategory_df.CategoryName).alias("CategoryName"),
    when(PurchaseCategory_df.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(PurchaseCategory_df.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
    from_utc_timestamp(PurchaseCategory_df.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
    PurchaseCategory_df.CategoryGroupId,
    PurchaseCategory_df.RecordId.alias("PurchCategoryRecordId")    
).withColumn("UpdatedDateTime",lit(UpdatedDateTime)
).withColumn("PurchCategoryHashKey",xxhash64(col("PurchCategoryRecordId")))

display(dimPurchaseCategory_df)

# COMMAND ----------

df_final=dimPurchaseCategory_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimpurchasecategory")
