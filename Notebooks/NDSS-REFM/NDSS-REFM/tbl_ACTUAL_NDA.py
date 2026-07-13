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
TableLoc = DBLoc + "/ACTUAL_NDA"
spark.sql(""" DROP TABLE IF EXISTS {0}.ACTUAL_NDA """.format(Database))

# COMMAND ----------

# DBTITLE 1,Table Creation Script
#%sql
#CREATE TABLE ACTUAL_NDT
  #(  
#    KPI_TYPE_TEXT string,
#SITE_ID int,
#LEASE_ID string,
#TRANSACTION_ID string,
#COMPANY_CODE string,
#CALENDAR_YEAR timestamp,
#CALENDAR_MONTH string,
#LEASE_CLASSIFICATION_CODE string,
#ACTUAL_LC_AMOUNT double,
#ACTUAL_USD_AMOUNT double,
#LEGALLY_COMMITTED_INDICATOR string,
#LOCAL_CURRENCY_CODE string,
#CREATE_DATE timestamp,
#UPDATE_DATE timestamp,
#LEASE_SITE_COUNT int,
#COMMENTS_TEXT string,
#POSTING_PERIOD INT,
#PROFIT_CENTER INT,
#G_L_ACCOUNT INT,
#ASSIGNMENT_CODE STRING,
#FLOW_TYPE_CODE STRING,
#DOCUMENT_TYPE_CODE STRING   
#     )
# USING DELTA 
# LOCATION '/mnt/ADLS2/NDH/Sensitive/ACTUAL_NDT'


# COMMAND ----------

spark.sql("""
  CREATE TABLE {0}.ACTUAL_NDA
  (  
    KPI_TYPE_TEXT string,
SITE_ID string,
SITE_NAME  string,  
LEASE_ID string,
--TRANSACTION_ID string,
COMPANY_CODE string,
CALENDAR_YEAR int,
CALENDAR_MONTH string,
LEASE_CLASSIFICATION_CODE string,
ACTUAL_LC_AMOUNT double,
ACTUAL_USD_AMOUNT double,
LEGALLY_COMMITTED_INDICATOR string,
LOCAL_CURRENCY_CODE string,
--CREATE_DATE timestamp,
--UPDATE_DATE timestamp,
LEASE_SITE_COUNT int,
COMMENTS_TEXT string,
POSTING_PERIOD INT,
PROFIT_CENTER INT,
G_L_ACCOUNT INT,
--ASSIGNMENT_CODE STRING,
FLOW_TYPE_CODE STRING,
DOCUMENT_TYPE_CODE STRING,
GROWTH_SUSTAIN_SPLIT_TEXT STRING
   
     )
 USING DELTA 
 LOCATION '{1}'
"""
.format(Database,TableLoc))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table ndh.ACTUAL_NDT

# COMMAND ----------

# MAGIC %sql
# MAGIC desc ndh.Actual_NDT
