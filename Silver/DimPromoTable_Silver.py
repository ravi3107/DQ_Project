# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

PromoTable_df=spark.table("bronze.promotable")

# COMMAND ----------

dimPromoTable_df = PromoTable_df.filter(PromoTable_df.RecordId.isNotNull()
    ).select(
        PromoTable_df.PromotionId,
        when(PromoTable_df.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(PromoTable_df.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
        from_utc_timestamp(PromoTable_df.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        trim(PromoTable_df.PromotionName).alias("PromotionName"),
        trim(PromoTable_df.PromoCode).alias("PromoCode"),
        trim(PromoTable_df.PromoType).alias("PromoType"),
        PromoTable_df.PromoPercentage,
        from_utc_timestamp(PromoTable_df.ValidFrom,'CST').alias("ValidFrom"),
        from_utc_timestamp(PromoTable_df.ValidTo,'CST').alias("ValidTo"),
        PromoTable_df.IsActive,
        PromoTable_df.RecordId.alias("PromoRecordId")
    ).withColumn("UpdatedDateTime", lit(UpdatedDateTime)
    ).withColumn("PromoRecordIdHashKey", xxhash64("PromoRecordId")
    )

display(dimPromoTable_df)

# COMMAND ----------

df_final=dimPromoTable_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimpromotable")
