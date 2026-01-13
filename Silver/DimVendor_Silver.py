# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

VendTable_df=spark.table("bronze.vendtable")

# COMMAND ----------

dimVendor_df=VendTable_df.filter(VendTable_df.RecordId.isNotNull()).select(
    VendTable_df.VendId.alias("VendorId"),
    trim(VendTable_df.VendorName).alias("VendorName"),
    when(VendTable_df.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(VendTable_df.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
    from_utc_timestamp(VendTable_df.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
    trim(VendTable_df.Address).alias("Address"),
    trim(VendTable_df.City).alias("City"),
    trim(VendTable_df.State).alias("State"),
    trim(VendTable_df.Country).alias("Country"),
    trim(VendTable_df.ZipCode).alias("ZipCode"),
    trim(VendTable_df.Region).alias("Region"),
    from_utc_timestamp(VendTable_df.ValidFrom,"CST").alias("ValidFrom"),
    from_utc_timestamp(VendTable_df.ValidTo,"CST").alias("ValidTo"),
    VendTable_df.Active,
    VendTable_df.RecordId.alias("VendorRecordId"),
    trim(VendTable_df.TaxId).alias("TaxId"),
    trim(VendTable_df.CurrencyCode).alias("CurrencyCode")
).withColumn("UpdatedDateTime",lit(UpdatedDateTime)
).withColumn("VendorHashKey",xxhash64(col("VendorRecordId")))

display(dimVendor_df)

# COMMAND ----------

df_final=dimVendor_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimvendor")
