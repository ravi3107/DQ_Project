# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

CustTable_df=spark.table("bronze.custtable")

# COMMAND ----------

dimCustTable_df=CustTable_df.filter(CustTable_df.RecordId.isNotNull()
).select(
        CustTable_df.CustomerId,
        when(CustTable_df.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(CustTable_df.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
        from_utc_timestamp(CustTable_df.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        trim(CustTable_df.CustomerName).alias("CustomerName"),
        trim(CustTable_df.Email).alias("Email"),
        trim(CustTable_df.Phone).alias("Phone"),
        trim(CustTable_df.Address).alias("Address"),
        trim(CustTable_df.City).alias("City"),
        trim(CustTable_df.State).alias("State"),
        trim(CustTable_df.Country).alias("Country"),
        trim(CustTable_df.Country).alias("ZipCode"),
        trim(CustTable_df.Region).alias("Region"),
        from_utc_timestamp(CustTable_df.SignupDate,'CST').alias("SignupDate"),
        CustTable_df.RecordId.alias("CustRecordId")
    ).withColumn("UpdatedDateTime", lit(UpdatedDateTime)
    ).withColumn("CustRecordIdHashKey", xxhash64("CustRecordId")
    )

display(dimCustTable_df)

# COMMAND ----------

df_final=dimCustTable_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimcusttable")
