# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE NDH.LEASE_DATA_NDT;
# MAGIC DROP TABLE NDH.SBR_QUANTITY_RULES_NDT;
# MAGIC DROP TABLE NDH.SBR_VALUE_RULES_NDT;

# COMMAND ----------

dbutils.fs.rm("/mnt/ADLS2/NDH/Sensitive/TRIRIGA/LEASE_DATA_NDT/",True)
dbutils.fs.rm("/mnt/ADLS2/NDH/NonSensitive/TRIRIGA/SBR_QUANTITY_RULES_NDT/",True)
dbutils.fs.rm("/mnt/ADLS2/NDH/NonSensitive/TRIRIGA/SBR_VALUE_RULES_NDT/",True)

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/ADLS2/NDH/Sensitive/TRIRIGA/LEASE_DATA_NDT/")
dbutils.fs.mkdirs("/mnt/ADLS2/NDH/NonSensitive/TRIRIGA/SBR_QUANTITY_RULES_NDT/")
dbutils.fs.mkdirs("/mnt/ADLS2/NDH/NonSensitive/TRIRIGA/SBR_VALUE_RULES_NDT/")
