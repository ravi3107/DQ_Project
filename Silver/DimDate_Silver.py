# Databricks notebook source
# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

FiscalPeriod_df=spark.table("bronze.fiscalperiod")
display(FiscalPeriod_df)

# COMMAND ----------

start_date=datetime.date(2018,1,1)
end_date=start_date+dateutil.relativedelta.relativedelta(years=8,month=12,day=31)

start_date=datetime.datetime.strptime(f"{start_date}","%Y-%m-%d")
end_date=datetime.datetime.strptime(f"{end_date}","%Y-%m-%d")

print(start_date)
print(end_date)

# COMMAND ----------

date_pd_df=pd.date_range(start=start_date,end=end_date,freq='D').to_frame(name='Date')
date_df=spark.createDataFrame(date_pd_df)
display(date_df)

# COMMAND ----------

join_df=date_df.join(
    FiscalPeriod_df.filter(FiscalPeriod_df.RecordId.isNotNull()),
    (date_df.Date>=FiscalPeriod_df.FiscalStartDate) & 
    (date_df.Date<=FiscalPeriod_df.FiscalEndDate),"left"
    )

display(join_df)

# COMMAND ----------

DimDate_df=join_df.select(
    "Date",
    date_format("Date","yyyyMMdd").cast("int").alias("DateId"),
    year("Date").alias("Year"),
    month("Date").alias("Month"),
    date_format("Date","MMM").cast("string").alias("MonthName"),
    dayofmonth("Date").alias("Day"),
    date_format("Date","E").cast("string").alias("DayName"),
    quarter("Date").alias("Quarter"),
    "FiscalPeriodName",
    "FiscalStartDate",
    "FiscalEndDate",
    "FiscalMonth",
    "FiscalYearStart",
    "FiscalYearEnd",
    "FiscalQuarter",
    "FiscalQuarterStart",
    "FiscalQuarterEnd",
    concat(lit("FY"),"FiscalYear").alias("FiscalYear"),
    ).withColumn("UpdatedDateTime",lit(UpdatedDateTime)
).withColumn("DateKey",xxhash64(col("DateId")))

display(DimDate_df)

# COMMAND ----------

df_final=DimDate_df

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.dimdate")
