# Databricks notebook source
# DBTITLE 1,Common functions notebook
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

# DBTITLE 1,Package imports
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,NDH.LEASE_DATA_NDT --> DBO.LEASE_DATA_NDT
SourceTable = 'NDH.LEASE_DATA_NDT'
TargetTable = 'DBO.LEASE_DATA_NDT'

lease_data_ndt_df = spark.sql("SELECT * FROM "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapse(lease_data_ndt_df,TargetTable)

# COMMAND ----------

# DBTITLE 1,NDH.SBR_QUANTITY_RULES_NDT --> DBO.SBR_QUANTITY_RULES_NDT
SourceTable = 'NDH.SBR_QUANTITY_RULES_NDT'
TargetTable = 'DBO.SBR_QUANTITY_RULES_NDT'

sbr_quantity_rules_ndt_df = spark.sql("SELECT * FROM "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapse(sbr_quantity_rules_ndt_df,TargetTable)

# COMMAND ----------

# DBTITLE 1,NDH.SBR_VALUE_RULES_NDT --> DBO.SBR_VALUE_RULES_NDT
SourceTable = 'NDH.SBR_VALUE_RULES_NDT'
TargetTable = 'DBO.SBR_VALUE_RULES_NDT'

sbr_value_rules_ndt = spark.sql("SELECT * FROM "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapse(sbr_value_rules_ndt,TargetTable)
