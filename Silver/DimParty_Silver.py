# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

parties_df=spark.table("bronze.parties")
partyaddress_df=spark.table("bronze.partyaddress")

# COMMAND ----------

dimParty_df=parties_df.join(partyaddress_df, parties_df.PartyId == partyaddress_df.PartyNumber, "left").filter(parties_df.RecordId.isNotNull()).select(
    parties_df.PartyId,
    trim(parties_df.PartyName).alias("PartyName"),
    when(parties_df.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(parties_df.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
    from_utc_timestamp(parties_df.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
    trim(parties_df.PartyAddressCode).alias("PartyAddressCode"),
    from_utc_timestamp(parties_df.EstablishedDate,"CST").alias("EstablishedDate"),
    trim(parties_df.PartyEmailId).alias("PartyEmailId"),
    trim(parties_df.PartyContactNumber).alias("PartyContactNumber"),
    parties_df.RecordId.alias("PartyRecordId"),
    trim(parties_df.TaxId).alias("TaxId"),
    trim(partyaddress_df.Address).alias("Address"),
    trim(partyaddress_df.City).alias("City"),
    trim(partyaddress_df.State).alias("State"),
    trim(partyaddress_df.Country).alias("Country"),
    trim(partyaddress_df.Region).alias("Region"),
    from_utc_timestamp(partyaddress_df.ValidFrom,"CST").alias("ValidFrom"),
    when(partyaddress_df.ValidTo.isNull(),"1900-01-01").otherwise(partyaddress_df.ValidTo).alias("ValidTo"),
    partyaddress_df.RecordId.alias("PartyAddressRecordId")
).withColumn("UpdatedDateTime",lit(UpdatedDateTime)
).withColumn("PartyHashKey",xxhash64(col("PartyRecordId")))

display(dimParty_df)

# COMMAND ----------

df_final=dimParty_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimparty")
