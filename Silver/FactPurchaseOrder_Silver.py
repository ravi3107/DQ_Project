# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

PurchaseOrder_df=spark.table("bronze.purchaseorder")
dimCostCenter_df=spark.table("silver.dimcostcenter")
dimCurrency_df=spark.table("silver.dimcurrency")

# COMMAND ----------

factPurchaseOrder_df=PurchaseOrder_df.filter(PurchaseOrder_df.RecordId.isNotNull()
    ).join(dimCostCenter_df,PurchaseOrder_df.CostCenter==dimCostCenter_df.CostCenterNumber,"left"
    ).join(dimCurrency_df,PurchaseOrder_df.currencycode==dimCurrency_df.CurrencyCode,"left"
    ).select(
    PurchaseOrder_df.PoNumber,
    PurchaseOrder_df.LineItem,
    PurchaseOrder_df.VendId.alias("VendorKey"),
    when(PurchaseOrder_df.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(PurchaseOrder_df.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
    from_utc_timestamp(PurchaseOrder_df.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
    PurchaseOrder_df.Qty,
    PurchaseOrder_df.PurchasePrice,
    PurchaseOrder_df.TotalOrder,
    PurchaseOrder_df.CostCenter.alias("CostCenterKey"),
    dimCostCenter_df.Vat.alias("VatAmount"),
    round((PurchaseOrder_df.TotalOrder+(PurchaseOrder_df.TotalOrder*dimCostCenter_df.Vat)),4).alias("TotalAmount"),
    PurchaseOrder_df.ExchangeRate,
    PurchaseOrder_df.Itemkey,
    dimCurrency_df.CurrencyId.alias("CurrencyKey"),
    from_utc_timestamp(PurchaseOrder_df.OrderDate,"CST").alias("OrderDate"),
    from_utc_timestamp(PurchaseOrder_df.ShipDate,"CST").alias("ShipDate"),
    from_utc_timestamp(PurchaseOrder_df.DeliveredDate,"CST").alias("DeliveredDate"),
    date_format(PurchaseOrder_df.OrderDate,"yyyyMMdd").cast("int").alias("OrderDateKey"),
    date_format(PurchaseOrder_df.ShipDate,"yyyyMMdd").cast("int").alias("ShipDateKey"),
    date_format(PurchaseOrder_df.DeliveredDate,"yyyyMMdd").cast("int").alias("DeliveredDateKey"),
    PurchaseOrder_df.TrackingNumber,
    PurchaseOrder_df.Batchid.alias("BatchId"),
    PurchaseOrder_df.CreatedBy,
    PurchaseOrder_df.RecordId.alias("PurchaseOrderRecordId"),
    PurchaseOrder_df.CategoryId.alias("CategoryKey")
).withColumn("UpdatedDateTime",lit(UpdatedDateTime)
).withColumn("PurchaseOrderHashKey",xxhash64(col("PurchaseOrderRecordId"))
)

display(factPurchaseOrder_df)

# COMMAND ----------

df_final=factPurchaseOrder_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.factpurchaseorder")
