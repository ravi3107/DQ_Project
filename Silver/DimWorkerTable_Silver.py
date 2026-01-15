# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

WorkerTable_df=spark.table("bronze.workertable")
Vertical_df=spark.table("silver.dimvertical")

# COMMAND ----------

dimWorkerTable_df = WorkerTable_df.filter(WorkerTable_df.RecordId.isNotNull()
    ).join(
        Vertical_df,WorkerTable_df.Vertical == Vertical_df.Vertical,"left"
    ).select(
       WorkerTable_df.WorkerID,
       when(WorkerTable_df.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(WorkerTable_df.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       from_utc_timestamp(WorkerTable_df.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       WorkerTable_df.SupervisorId,
       trim(WorkerTable_df.WorkerName).alias("WorkerName"),
       trim(WorkerTable_df.WorkerEmail).alias("WorkerEmail"),
       trim(WorkerTable_df.Phone).alias("Phone"),
       from_utc_timestamp(WorkerTable_df.DOJ,'CST').alias("DOJ"),
       from_utc_timestamp(WorkerTable_df.DOL,'CST').alias("DOL"), 
       Vertical_df.VerticalId ,
       WorkerTable_df.Type,
       WorkerTable_df.PayPerAnnum,
       WorkerTable_df.Rate,
       WorkerTable_df.RecordId.alias("WorkerRecordId")  
    ).withColumn("UpdatedDateTime", lit(UpdatedDateTime)
    ).withColumn("WorkerHashKey", xxhash64("WorkerRecordId")
    )
display(dimWorkerTable_df)

# COMMAND ----------

df_final=dimWorkerTable_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimworkertable")
