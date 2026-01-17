# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

WorkerTable_df=spark.table("bronze.workertable")
display(WorkerTable_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.dimvertical(
# MAGIC   VerticalId Int,
# MAGIC   Vertical String
# MAGIC )

# COMMAND ----------

df=WorkerTable_df.select(expr("trim(Vertical) AS Vertical")).distinct()
display(df)

# COMMAND ----------

Vertical_df=spark.table("silver.dimvertical")
display(Vertical_df)

# COMMAND ----------

NewRows_df=df.filter(df.Vertical.isNotNull()).exceptAll(Vertical_df.select("Vertical"))
display(NewRows_df)

# COMMAND ----------

Vertical_df.createOrReplaceTempView("vertical")

max_df = spark.sql("select ifnull(max(VerticalId),0) as max_id from vertical")
top_row=max_df.head(1)
max_id=top_row[0][0]
print(max_id)


# COMMAND ----------

ids_df = NewRows_df.withColumn("VerticalId", row_number().over(window=W.Window.orderBy(col("Vertical"))))
display(ids_df)

# COMMAND ----------

ids_Final = ids_df.withColumn("VerticalId",col("VerticalId")+max_id)
display(ids_Final)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dimvertical

# COMMAND ----------

df_final=ids_Final

# COMMAND ----------

df_final.write.mode("append").option("mergeSchema","true").saveAsTable("silver.dimvertical")
