# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

SalesOrderLine_df=spark.table("bronze.salesorderline")
display(SalesOrderLine_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.dimpaymentypes(
# MAGIC   PaymentTypeId Int,
# MAGIC   PaymentTypeDesc String
# MAGIC )

# COMMAND ----------

df=SalesOrderLine_df.select("PaymentTypeDesc").distinct()
display(df)

# COMMAND ----------

PaymentType_df=spark.table("silver.dimpaymentypes")
display(PaymentType_df)

# COMMAND ----------

NewRows_df=df.exceptAll(PaymentType_df.select("PaymentTypeDesc"))
display(NewRows_df)

# COMMAND ----------

PaymentType_df.createOrReplaceTempView("paymenttype")

max_df = spark.sql("select ifnull(max(PaymentTypeId),0) as max_id from paymenttype")
top_row=max_df.head(1)
max_id=top_row[0][0]
print(max_id)


# COMMAND ----------

ids_df = NewRows_df.withColumn("PaymentTypeId", row_number().over(window=W.Window.orderBy(col("PaymentTypeDesc"))))
display(ids_df)

# COMMAND ----------

ids_Final = ids_df.withColumn("PaymentTypeId",col("PaymentTypeId")+max_id)
display(ids_Final)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dimpaymentypes

# COMMAND ----------

df_final=ids_Final

# COMMAND ----------

df_final.write.mode("append").option("mergeSchema","true").saveAsTable("silver.dimpaymentypes")
