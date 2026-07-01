# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

SourceTable = 'NDH.Actual_Exchange_Rate_NDT'
TargetTable = 'dbo.Actual_Exchange_Rate_NDT'

Actual_Exchange_Rate_NDT_df = spark.sql("SELECT * from NDH.Actual_Exchange_Rate_NDT").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(Actual_Exchange_Rate_NDT_df,TargetTable)

# COMMAND ----------

