# Databricks notebook source
# DBTITLE 1,Widgets Read
# dbutils.widgets.removeAll()
# dbutils.widgets.text("MountPath","")
# MountPath = dbutils.widgets.get("MountPath") 

# dbutils.widgets.text("MountPathADLS1","")
# MountPathADLS1 = dbutils.widgets.get("MountPathADLS1") 

# dbutils.widgets.text("Database","")
# Database = dbutils.widgets.get("Database")

# dbutils.widgets.text("ADLSFolderPath","")
# ADLSFolderPath = dbutils.widgets.get("ADLSFolderPath") 

Database = 'NDH'
DBLoc = '/mnt/ADLS2/NDH/Sensitive'

#MountPath + ADLSFolderPath

# COMMAND ----------

# DBTITLE 1,Drop table if exists
TableLoc = DBLoc + "/LE_NDA"
dbutils.fs.rm(TableLoc,True)
spark.sql(""" DROP TABLE IF EXISTS {0}.LE_NDA """.format(Database))

# COMMAND ----------

# DBTITLE 1,Table Creation Script
spark.sql("""
  CREATE TABLE {0}.LE_NDA
  (  
     KPI_TYPE_TEXT string,
     LE_TYPE_CODE string,
SITE_ID string,
SITE_NAME string,
LEASE_ID string,
TRANSACTION_ID string,
COMPANY_CODE string,
VALUE_TYPE_CODE string, 
CALENDAR_YEAR string,
CALENDAR_MONTH string,
LEASE_CLASSIFICATION_CODE string,
LE_LC_AMOUNT double,
LE_USD_AMOUNT double,
LEGALLY_COMMITTED_INDICATOR string,
LOCAL_CURRENCY_CODE string,
LEASE_START_DATE timestamp,  
LEASE_EXPIRY_DATE timestamp,   
LEASE_SITE_COUNT int,
AUTO_RENEWAL_INDICATOR string
     )
 USING DELTA 
 LOCATION '{1}'
"""
.format(Database,TableLoc))

# COMMAND ----------

# MAGIC %sql
# MAGIC --  DROP TABLE ndh.LE_NDT

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ndh.LE_ndt
