# Databricks notebook source
LEASE_DATA_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA.parquet"
SBR_QUANTITY_RULES_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_SBR_QUANTITY_RULES/Global/TRIRIGA_SBR_QUANTITY_RULES.parquet"
SBR_VALUE_RULES_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_SBR_VALUE_RULES/Global/TRIRIGA_SBR_VALUE_RULES.parquet"

LEASE_DATA_df = spark.read.parquet(LEASE_DATA_mdh)
SBR_QUANTITY_RULES_df = spark.read.parquet(SBR_QUANTITY_RULES_mdh)
SBR_VALUE_RULES_df = spark.read.parquet(SBR_VALUE_RULES_mdh)

LEASE_DATA_df.createOrReplaceTempView('LEASE_DATA_vw')
SBR_QUANTITY_RULES_df.createOrReplaceTempView('SBR_QUANTITY_RULES_vw')
SBR_VALUE_RULES_df.createOrReplaceTempView('SBR_VALUE_RULES_vw')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM SBR_QUANTITY_RULES_vw
# MAGIC --WHERE TRI_EFFECTIVE_FROM_DA IS null
# MAGIC ORDER BY Ingestion_Revision_Date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- August 7th count : 1797
# MAGIC -- Other than aug 7th count : 48422
# MAGIC
# MAGIC SELECT COUNT(*) FROM SBR_QUANTITY_RULES_vw
# MAGIC WHERE Ingestion_Revision_Date NOT LIKE '%2022-08-07%'
# MAGIC --ORDER BY Ingestion_Revision_Date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM LEASE_DATA_vw
# MAGIC --WHERE TRI_LEASE_ID_TX = 1000789
# MAGIC ORDER BY Ingestion_Revision_Date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM SBR_QUANTITY_RULES_vw
# MAGIC WHERE TRI_EFFECTIVE_FROM_DA IS null
# MAGIC --ORDER BY Ingestion_Revision_Date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT CST_QUANTITY_CALC_NU,TRI_LEASE_ID_TX,TRI_EFFECTIVE_FROM_DA,TRI_EFFECTIVE_TO_DA,TRI_MODIFIED_DATE_SY,Ingestion_Revision_Date FROM SBR_QUANTITY_RULES_vw 
# MAGIC --WHERE TRI_LEASE_ID_TX = 1000789
# MAGIC --AND TRI_SALES_CATEGORY_CL = 'Fuel - NGL -All Grds'
# MAGIC --AND TRI_EFFECTIVE_FROM_DA = '01/01/2019'
# MAGIC ORDER BY Ingestion_Revision_Date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT CST_QUANTITY_CALC_NU,TRI_LEASE_ID_TX,TRI_EFFECTIVE_FROM_DA,TRI_EFFECTIVE_TO_DA,TRI_MODIFIED_DATE_SY,Ingestion_Revision_Date FROM SBR_QUANTITY_RULES_vw 
# MAGIC WHERE TRI_LEASE_ID_TX = 1000789
# MAGIC --AND TRI_SALES_CATEGORY_CL = 'Fuel - NGL -All Grds'
# MAGIC --AND TRI_EFFECTIVE_FROM_DA = '01/01/2019'
# MAGIC ORDER BY Ingestion_Revision_Date DESC

# COMMAND ----------

sbr_qr_landing_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load("/mnt/ADLS1/LANDING/1stParty/TRIRIGA/TRIRIGA_SBR_QUANTITY_RULES/Global/TRIRIGA_SBR_QUANTITY_RULES_DELTA_TXN_APPEND-20220807043754.csv")

sbr_qr_raw_df = spark.read.parquet("/mnt/ADLS1/RAW/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA_DELTA_TXN_APPEND-20220731043136.parquet")

sbr_qr_df = spark.read.parquet("/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_SBR_QUANTITY_RULES/Global/TRIRIGA_SBR_QUANTITY_RULES.parquet)

sbr_qr_landing_df.createOrReplaceTempView('LANDING_SBR_QR_vw')
sbr_qr_raw_df.createOrReplaceTempView('RAW_SBR_QR_vw')
sbr_qr_df.createOrReplaceTempView('SBR_QR_vw')
