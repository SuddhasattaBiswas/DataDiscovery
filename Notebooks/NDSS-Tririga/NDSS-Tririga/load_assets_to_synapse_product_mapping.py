# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

SourceTable = 'NDH.PRODUCT_SALES_CATEGORY_NDT'
TargetTable = 'DBO.PRODUCT_SALES_CATEGORY_NDT'

product_sales_category_ndt_df = spark.sql("SELECT * FROM "+SourceTable+" ")

setconnections();
overwriteToSynapse(product_sales_category_ndt_df,TargetTable)
