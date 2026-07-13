# Databricks notebook source
landing_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load("/mnt/ADLS1/LANDING/1stParty/TRIRIGA/TRIRIGA_SBR_QUANTITY_RULES/Global/TRIRIGA_SBR_QUANTITY_RULES_DELTA_TXN_APPEND-20220814043750.csv")

lease_data_raw_df = spark.read.parquet("/mnt/ADLS1/RAW/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA_DELTA_TXN_APPEND-20220731043136.parquet")

lease_data_df = spark.read.parquet("/mnt/ADLS1/PREP/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA.parquet")

landing_df.createOrReplaceTempView('LANDING_vw')
lease_data_raw_df.createOrReplaceTempView('LEASE_DATA_vw')
lease_data_df.createOrReplaceTempView('LEASE_DATA_vw')

# print("Landing Count : ",lease_data_landing_df.count())
# print("RAW Count : ",lease_data_raw_df.count())
# print("PREP Count : ",lease_data_df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM LANDING_vw
# MAGIC WHERE TRI_EFFECTIVE_FROM_DA IS null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT CST_PREMISE_LOCATION_ID,TRI_CONTROL_NUMBER_CN FROM LEASE_DATA_vw
# MAGIC WHERE CST_PREMISE_LOCATION_ID = 1005022

# COMMAND ----------

sbr_qr_source = spark.read.option("delimiter", "|").format("csv").option("header", "true").load("/mnt/ADLS1/LANDING/1stParty/TRIRIGA/TRIRIGA_SBR_QUANTITY_RULES/Global/TRIRIGA_SBR_QUANTITY_RULES_DELTA_TXN_APPEND-20220814043750.csv")
sbr_qr_source = sbr_qr_source.filter(sbr_qr_source.TRI_EFFECTIVE_FROM_DA.isNull()).select(sbr_qr_source.TRI_ID_TX,sbr_qr_source.TRI_EFFECTIVE_FROM_DA,sbr_qr_source.TRI_EFFECTIVE_TO_DA).display()

# COMMAND ----------

raw_df_31 = spark.read.parquet("/mnt/ADLS1/RAW/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA_DELTA_TXN_APPEND-20220731043136.parquet")
raw_df_7 = spark.read.parquet("/mnt/ADLS1/RAW/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA_DELTA_TXN_APPEND-20220807043136.parquet")
raw_df_14 = spark.read.parquet("/mnt/ADLS1/RAW/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA_DELTA_TXN_APPEND-20220814043136.parquet")

raw_df_31.createOrReplaceTempView('RAW_31_vw')
raw_df_7.createOrReplaceTempView('RAW_7_vw')
raw_df_14.createOrReplaceTempView('RAW_14_vw')

print("July 31 : ",raw_df_31.count())
print("Aug 7 : ",raw_df_7.count())
print("Aug 14 : ",raw_df_14.count())

# COMMAND ----------


