# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

currency_df=spark.table("bronze.currency")
display(currency_df)

# COMMAND ----------

dimCurrency_df=currency_df.filter(currency_df.RecordId.isNotNull()).select(
    currency_df.CurrencyId,
    trim(currency_df.Code).alias("CurrencyCode"),
    when(currency_df.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(currency_df.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
    from_utc_timestamp(currency_df.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
    trim(currency_df.Country).alias("Country"),
    trim(currency_df.CurrencyName).alias("CurrencyName"),
    currency_df.RecordId.alias("CurrencyRecordId")    
).withColumn("UpdatedDateTime",lit(UpdatedDateTime)
).withColumn("CurrencyHashKey",xxhash64(col("CurrencyRecordId")))

display(dimCurrency_df)

# COMMAND ----------

df_final=dimCurrency_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimcurrency")
